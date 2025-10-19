import verboselogs
from sqlalchemy import text

from triage.database_reflection import table_has_data, table_row_count, table_exists, table_has_duplicates


logger = verboselogs.VerboseLogger(__name__)
DEFAULT_ACTIVE_STATE = "active"


class EntityDateTableGenerator:
    """Create a table containing state membership on different dates

    The structure of the output table is:
        entity_id
        date
        active (boolean): Whether or not the entity is considered 'active'
            (i.e., in the cohort or subset) on that date

    Args:
        db_engine (sqlalchemy.engine)
        experiment_hash (string) unique identifier for the experiment
        query (string) SQL query string to select entities for a given as_of_date
            The as_of_date should be parameterized with brackets: {as_of_date}
        replace (boolean) Whether or not to overwrite old rows.
            If false, each as-of-date will query to see if there are existing rows
                and not run the query if so.
            If true, the existing table will be dropped and recreated.
    """
    def __init__(self, query, db_engine, entity_date_table_name, labels_table_name=None, replace=True, db_adapter=None):
        self.db_engine = db_engine
        self.db_adapter = db_adapter
        self.query = query
        self.entity_date_table_name = entity_date_table_name
        self.labels_table_name = labels_table_name
        self.replace = replace

    def generate_entity_date_table(self, as_of_dates):
        """Convert the object's input table
        into a states table for the given as_of_dates

        Args:
            as_of_dates (list of datetime.dates) Dates to include in the
                state table
        """
        logger.spam(f"Generating entity_date table {self.entity_date_table_name}")
        if self.query:
            logger.spam(f"Query is present, so running query on as_of_dates: {as_of_dates}")
            self._create_and_populate_entity_date_table_from_query(as_of_dates)
        elif self.labels_table_name:
            self._create_and_populate_entity_date_table_from_labels()
        else:
            raise ValueError("Neither query not labels table name is available, cannot compute cohort")
        logger.spam(f"Table {self.entity_date_table_name} created and populated")

        if not table_has_data(self.entity_date_table_name, self.db_engine):
            raise ValueError(self._empty_table_message(as_of_dates))

        if table_has_duplicates(
            self.entity_date_table_name,
            ['entity_id', 'as_of_date'],
            self.db_engine
            ):
            raise ValueError(f"Duplicates found in {self.entity_date_table_name}!")

        logger.debug(f"Entity-date table generated at {self.entity_date_table_name}")
        logger.spam(f"Generating stats on {self.entity_date_table_name}")
        logger.spam(f"Row count of {self.entity_date_table_name}: {table_row_count(self.entity_date_table_name, self.db_engine)}")


    def _maybe_create_entity_date_table(self):
        if self.replace or not table_exists(self.entity_date_table_name, self.db_engine):
            logger.spam(f"Creating entity_date table {self.entity_date_table_name}")

            with self.db_engine.begin() as conn:
                conn.execute(text(f"drop table if exists {self.entity_date_table_name}"))

                # Use database adapter for table creation
                if self.db_adapter:
                    ddl = self.db_adapter.get_entity_date_table_ddl(self.entity_date_table_name)
                else:
                    # Fallback to PostgreSQL DDL
                    ddl = f"""create table {self.entity_date_table_name} (
                            entity_id integer,
                            as_of_date timestamp,
                            {DEFAULT_ACTIVE_STATE} boolean
                        )"""
                conn.execute(text(ddl))

                logger.spam(f"Creating indices on entity_id and as_of_date for entity_date table {self.entity_date_table_name}")

                # Use database adapter for index creation
                if self.db_adapter:
                    index_sql = self.db_adapter.create_index_statement(
                        self.entity_date_table_name,
                        ['entity_id', 'as_of_date']
                    )
                else:
                    # Fallback to PostgreSQL syntax
                    index_sql = f"create index on {self.entity_date_table_name} (entity_id, as_of_date)"
                conn.execute(text(index_sql))
        else:
            logger.notice(
                f"Not dropping and recreating entity_date {self.entity_date_table_name} table because "
                f"replace flag was set to False and table was found to exist"
            )

    def _create_and_populate_entity_date_table_from_query(self, as_of_dates):
        """Create an entity_date table by sequentially running a
            given date-parameterized query for all known dates.

        Args:
        as_of_dates (list of datetime.date): Dates to calculate entity states as of
        """

        self._maybe_create_entity_date_table()
        logger.spam(f"Inserting rows into entity_date table {self.entity_date_table_name}")
        for as_of_date in as_of_dates:
            formatted_date = f"{as_of_date.isoformat()}"
            logger.spam(f"Looking for existing entity_date rows for as of date {as_of_date}")

            with self.db_engine.begin() as conn:
                # Use database adapter for checking existing entity_date records
                if self.db_adapter:
                    check_query = self.db_adapter.get_entity_date_check_query(
                        self.entity_date_table_name, formatted_date
                    )
                else:
                    # Fallback to PostgreSQL syntax
                    check_query = f"""select 1 from {self.entity_date_table_name}
                                     where as_of_date = '{formatted_date}'
                                     limit 1"""

                result = conn.execute(text(check_query))
                any_existing = list(result)

            if len(any_existing) == 1:
                logger.notice(f"Since >0 entity_date rows found for date {as_of_date}, skipping")
                continue

            dated_query = self.query.format(as_of_date=formatted_date)

            # Use database adapter for entity_date insert query
            if self.db_adapter:
                full_query = self.db_adapter.get_entity_date_insert_query(
                    self.entity_date_table_name, formatted_date, dated_query
                )
            else:
                # Fallback to PostgreSQL syntax
                full_query = f"""insert into {self.entity_date_table_name}
                    select q.entity_id, '{formatted_date}'::timestamp, true
                    from ({dated_query}) q
                    group by 1, 2, 3"""

            logger.spam(f"Running entity_date query for date: {as_of_date}, {full_query}")

            with self.db_engine.begin() as conn:
                conn.execute(text(full_query))

    def _create_and_populate_entity_date_table_from_labels(self):
        """Create an entity_date table by storing all distinct entity-id/as-of-date pairs
        from the labels table
        """
        self._maybe_create_entity_date_table()
        logger.spam(f"Populating entity_date table {self.entity_date_table_name} from labels table {self.labels_table_name}")
        if not table_exists(self.labels_table_name, self.db_engine):
            logger.warning("Labels table does not exist, cannot populate entity-dates")
            return

        # If any rows exist in the entity_date table, don't insert any for dates
        # already in the table. This replicates the logic used above by
        # _create_and_populate_entity_date_table_from_query
        logger.spam(f"Looking for existing entity_date rows for label as of dates")

        with self.db_engine.begin() as conn:
            result = conn.execute(text(
                f"""
                with label_dates as (
                    select distinct as_of_date::DATE AS as_of_date FROM {self.labels_table_name}
                )
                , cohort_dates as (
                    select distinct as_of_date::DATE AS as_of_date FROM {self.entity_date_table_name}
                )
                select distinct l.as_of_date
                from label_dates l
                join cohort_dates c using(as_of_date)
                """
            ))
            existing_dates = list(result)
        if len(existing_dates) > 0:
            existing_dates = ', '.join([rec[0].isoformat() for rec in existing_dates])
            logger.notice(f'Existing entity_dates records found for the following dates, '
                f'so new records will not be inserted for these dates {existing_dates}')

        # Use database adapter for labels-to-entity date query
        if self.db_adapter:
            insert_query = self.db_adapter.get_labels_to_entity_date_query(
                self.entity_date_table_name, self.labels_table_name
            )
        else:
            # Fallback to PostgreSQL syntax
            insert_query = f"""
                insert into {self.entity_date_table_name}
                select distinct entity_id, as_of_date, true
                from (
                    select distinct l.entity_id, l.as_of_date
                    from {self.labels_table_name} as l
                    left join (select distinct as_of_date from {self.entity_date_table_name}) as c
                        on l.as_of_date::DATE = c.as_of_date::DATE
                    where c.as_of_date IS NULL
                ) as sub
            """

        logger.spam(f"Running entity_date query from labels table: {insert_query}")

        with self.db_engine.begin() as conn:
            conn.execute(text(insert_query))

    def _empty_table_message(self, as_of_dates):
        return """Query does not return any rows for the given as_of_dates:
            {as_of_dates}
            '{query}'""".format(
            query=self.query or "labels table",
            as_of_dates=", ".join(
                str(as_of_date)
                for as_of_date in (
                    as_of_dates if len(as_of_dates) <= 5 else as_of_dates[:5] + ["…"]
                )
            ),
        )

    def clean_up(self):
        with self.db_engine.begin() as conn:
            conn.execute(text(f"drop table if exists {self.entity_date_table_name}"))


class CohortTableGeneratorNoOp(EntityDateTableGenerator):
    def __init__(self):
        pass

    def generate_entity_date_table(self, as_of_dates):
        logger.warning(
            "No cohort configuration is available, so no cohort will be created"
        )
        return

    def clean_up(self):
        logger.warning("No cohort configuration is available, so no cohort will be tear down")
        return

    @property
    def entity_date_table_name(self):
        return None


class SubsetEntityDateTableGenerator(EntityDateTableGenerator):
    def __init__(self, query, db_engine, entity_date_table_name, labels_table_name=None, replace=True, cohort_table=None, db_adapter=None):
        super().__init__(query, db_engine, entity_date_table_name, labels_table_name, replace, db_adapter)
        print('Initializing the new child class Subset entity date generator')
        self.cohort_table = cohort_table
        
    def create_and_populate_entity_date_table_from_query(self, as_of_dates):
        """Create an entity_date table by sequentially running a
            given date-parameterized query for all known dates.

        Args:
            as_of_dates (list of datetime.date): Dates to calculate entity states as of
        """

        self._maybe_create_entity_date_table()
        logger.spam(f"Inserting rows into entity_date table {self.entity_date_table_name}")

        for as_of_date in as_of_dates:
            formatted_date = f"{as_of_date.isoformat()}"
            logger.spam(f"Looking for existing entity_date rows for as of date {as_of_date}")

            with self.db_engine.begin() as conn:
                # Use database adapter for checking existing entity_date records
                if self.db_adapter:
                    check_query = self.db_adapter.get_entity_date_check_query(
                        self.entity_date_table_name, formatted_date
                    )
                else:
                    # Fallback to PostgreSQL syntax
                    check_query = f"""select 1 from {self.entity_date_table_name}
                                     where as_of_date = '{formatted_date}'
                                     limit 1"""

                result = conn.execute(text(check_query))
                any_existing = list(result)

            if len(any_existing) == 1:
                logger.notice(f"Since >0 entity_date rows found for date {as_of_date}, skipping")
                continue

            dated_query = self.query.format(as_of_date=formatted_date)

            # Use database adapter for subset entity_date insert query
            if self.db_adapter:
                full_query = self.db_adapter.get_subset_entity_date_insert_query(
                    self.entity_date_table_name, formatted_date, dated_query, self.cohort_table
                )
            else:
                # Fallback to PostgreSQL syntax
                full_query = f"""insert into {self.entity_date_table_name}
                    select q.entity_id, '{formatted_date}'::timestamp, true
                    from (
                        with subset as ({dated_query})
                        select
                            c. entity_id
                        from subset s inner join {self.cohort_table} c
                        on s.entity_id = c.entity_id
                        and c.as_of_date = '{formatted_date}'::date
                    ) q
                    group by 1, 2, 3"""

            logger.spam(f"Running entity_date query for date: {as_of_date}, {full_query}")

            with self.db_engine.begin() as conn:
                conn.execute(text(full_query))
            
            
    def generate_entity_date_table(self, as_of_dates):
        if self.query:
            logger.spam(f"Query is present, so running query on as_of_dates: {as_of_dates}")
            self.create_and_populate_entity_date_table_from_query(as_of_dates)
        else:
            logger.warning('Query not working, subset table not created!')
    
        if table_has_duplicates(
            self.entity_date_table_name,
            ['entity_id', 'as_of_date'],
            self.db_engine
            ):
            raise ValueError(f"Duplicates found in {self.entity_date_table_name}!")

        logger.debug(f"Entity-date table generated at {self.entity_date_table_name}")
        logger.spam(f"Generating stats on {self.entity_date_table_name}")
        logger.spam(f"Row count of {self.entity_date_table_name}: {table_row_count(self.entity_date_table_name, self.db_engine)}")