import verboselogs, logging
logger = verboselogs.VerboseLogger(__name__)

import textwrap

import pandas as pd
from sqlalchemy import text

from triage.database_reflection import table_exists
from triage.component.catwalk.storage import MatrixStore


class ProtectedGroupsGeneratorNoOp:
    def generate_all_dates(self, *args, **kwargs):
        logger.notice(
            "No bias audit configuration is available, so protected groups will not be created"
        )

    def generate(self, *args, **kwargs):
        logger.notice(
            "No bias audit configuration is available, so protected groups will not be created"
        )

    def as_dataframe(self, *args, **kwargs):
        logger.notice(
            "No bias audit configuration is available, so protected groups were not created: returning an empty data frame"
        )
        return pd.DataFrame()


class ProtectedGroupsGenerator:
    def __init__(self, db_engine, from_obj, attribute_columns, entity_id_column, knowledge_date_column, protected_groups_table_name, replace=True, db_adapter=None):
        self.db_engine = db_engine
        self.db_adapter = db_adapter
        self.replace = replace
        self.protected_groups_table_name = protected_groups_table_name
        self.from_obj = from_obj
        self.attribute_columns = attribute_columns
        self.entity_id_column = entity_id_column
        self.knowledge_date_column = knowledge_date_column

    def generate_all_dates(self, as_of_dates, cohort_table_name, cohort_hash):
        logger.spam("Creating protected groups table")
        table_is_new = False
        if not table_exists(self.protected_groups_table_name, self.db_engine):
            with self.db_engine.begin() as conn:
                # Use database adapter for table creation
                if self.db_adapter:
                    ddl = self.db_adapter.get_protected_groups_table_ddl(
                        self.protected_groups_table_name, self.attribute_columns
                    )
                else:
                    # Fallback to PostgreSQL DDL
                    ddl = f"""
                        create table if not exists {self.protected_groups_table_name} (
                        entity_id int,
                        as_of_date date,
                        {', '.join([str(col) + " varchar" for col in self.attribute_columns])},
                        cohort_hash text
                        )"""
                conn.execute(text(ddl))
            logger.debug(f"Protected groups table {self.protected_groups_table_name} created")
            table_is_new = True
        else:
            logger.debug(f"Protected groups table {self.protected_groups_table_name} exist")

        if self.replace:
            with self.db_engine.begin() as conn:
                conn.execute(text(
                    f"delete from {self.protected_groups_table_name} where cohort_hash = '{cohort_hash}'"
                ))
            logger.debug(f"Removed from {self.protected_groups_table_name} all rows from cohort {cohort_hash}")

        logger.spam(
            f"Creating protected_groups for {len(as_of_dates)} as of dates",
        )

        for as_of_date in as_of_dates:
            if not self.replace:
                logger.spam(
                    "Looking for existing protected_groups for as of date {as_of_date}"
                )
                with self.db_engine.begin() as conn:
                    # Use database adapter for checking existing records
                    if self.db_adapter:
                        check_query = self.db_adapter.get_protected_groups_check_query(
                            self.protected_groups_table_name, as_of_date, cohort_hash
                        )
                    else:
                        # Fallback to PostgreSQL syntax
                        check_query = f"""select 1 from {self.protected_groups_table_name}
                                         where as_of_date = '{as_of_date}'
                                         and cohort_hash = '{cohort_hash}'
                                         limit 1"""

                    result = conn.execute(text(check_query))
                    any_existing_rows = list(result)
                if len(any_existing_rows) == 1:
                    logger.debug("Since nonzero existing protected_groups found, skipping")
                    continue

            logger.debug(
                "Generating protected groups for as of date {as_of_date} "
            )
            self.generate(
                start_date=as_of_date,
                cohort_table_name=cohort_table_name,
                cohort_hash=cohort_hash
            )
        if table_is_new:
            with self.db_engine.begin() as conn:
                # Use database adapter for index creation
                if self.db_adapter:
                    index_sql = self.db_adapter.create_index_statement(
                        self.protected_groups_table_name, ['cohort_hash', 'as_of_date']
                    )
                else:
                    # Fallback to PostgreSQL syntax
                    index_sql = f"create index on {self.protected_groups_table_name} (cohort_hash, as_of_date)"
                conn.execute(text(index_sql))

        with self.db_engine.begin() as conn:
            result = conn.execute(text(
                "select count(*) from {}".format(self.protected_groups_table_name)
            ))
            nrows = result.scalar()
        if nrows == 0:
            logger.warning("Done creating protected_groups, but no rows in protected_groups table!")
        else:
            logger.success(f"Protected groups stored in the table "
                           f"{self.protected_groups_table_name} successfully")
            logger.spam(f"Protected groups table has {nrows} rows")

    def generate(self, start_date, cohort_table_name, cohort_hash):
        # Use database adapter for insert query
        if self.db_adapter:
            insert_query = self.db_adapter.get_protected_groups_insert_query(
                table_name=self.protected_groups_table_name,
                as_of_date=start_date,
                attribute_columns=self.attribute_columns,
                cohort_hash=cohort_hash,
                cohort_table_name=cohort_table_name,
                from_obj=self.from_obj,
                entity_id_column=self.entity_id_column,
                knowledge_date_column=self.knowledge_date_column
            )
        else:
            # Fallback to PostgreSQL syntax
            insert_query = textwrap.dedent(
                """
                insert into {protected_groups_table}
                select distinct on (cohort.entity_id, cohort.as_of_date)
                    cohort.entity_id,
                    '{as_of_date}'::date as as_of_date,
                    {attribute_columns},
                    '{cohort_hash}' as cohort_hash
                from {cohort_table_name} cohort
                left join (select * from {from_obj}) from_obj  on
                    cohort.entity_id = from_obj.{entity_id_column} and
                    cohort.as_of_date > from_obj.{knowledge_date_column}
                where cohort.as_of_date = '{as_of_date}'::date
                order by cohort.entity_id, cohort.as_of_date, {knowledge_date_column} desc
            """
            ).format(
                protected_groups_table=self.protected_groups_table_name,
                as_of_date=start_date,
                attribute_columns=", ".join([str(col) for col in self.attribute_columns]),
                cohort_hash=cohort_hash,
                cohort_table_name=cohort_table_name,
                from_obj=self.from_obj,
                knowledge_date_column=self.knowledge_date_column,
                entity_id_column=self.entity_id_column
            )

        logger.debug("Running protected_groups creation query")
        logger.spam(insert_query)
        with self.db_engine.begin() as conn:
            conn.execute(text(insert_query))

    def as_dataframe(self, as_of_dates, cohort_hash):
        """Queries the protected groups table to retrieve the protected attributes for each date
        Args:
            db_engine (sqlalchemy.engine) a database engine
            as_of_dates (list) the as_of_Dates to query

        Returns: (pd.DataFrame) a dataframe with protected attributes for the given dates
        """
        # Format dates as strings for the adapter
        as_of_dates_strings = [date.strftime("%Y-%m-%d") for date in as_of_dates]

        # Use database adapter for select query
        if self.db_adapter:
            query_string = self.db_adapter.get_protected_groups_select_query(
                self.protected_groups_table_name, as_of_dates_strings, cohort_hash
            )
        else:
            # Fallback to PostgreSQL syntax
            as_of_dates_sql = "[{}]".format(
                ", ".join("'{}'".format(date.strftime("%Y-%m-%d %H:%M:%S.%f")) for date in as_of_dates)
            )
            query_string = f"""
                with dates as (
                    select unnest(array{as_of_dates_sql}::timestamp[]) as as_of_date
                )
                select *
                from {self.protected_groups_table_name}
                join dates using(as_of_date)
                where cohort_hash = '{cohort_hash}'
            """

        # Use database adapter for efficient data loading
        if self.db_adapter:
            protected_df = self.db_adapter.query_to_dataframe(
                query_string,
                parse_dates=["as_of_date"],
                index_col=MatrixStore.indices
            )
        else:
            # Fallback to original PostgreSQL-specific method
            protected_df = pd.DataFrame.pg_copy_from(
                query_string,
                connectable=self.db_engine,
                parse_dates=["as_of_date"],
                index_col=MatrixStore.indices,
            )
        protected_df[self.attribute_columns] = protected_df[self.attribute_columns].astype(str)
        del protected_df['cohort_hash']
        return protected_df
