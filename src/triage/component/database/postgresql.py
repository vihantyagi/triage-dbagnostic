import json
from typing import Dict, List, Any
from sqlalchemy import String, text
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import Session
import pandas as pd

from .base import DatabaseAdapter
# Moved import inside methods to avoid circular import


class PostgreSQLAdapter(DatabaseAdapter):
    """PostgreSQL-specific database adapter for Triage.
    
    Implements the DatabaseAdapter interface using PostgreSQL-specific features
    like JSONB, arrays, and stored procedures.
    """
    
    def get_json_column_type(self):
        """Return PostgreSQL JSONB column type."""
        return JSONB
    
    def get_array_column_type(self, item_type):
        """Return PostgreSQL array column type."""
        return ARRAY(item_type)
    
    def store_json_data(self, data: Dict[str, Any]) -> str:
        """Convert Python dict to JSON string for PostgreSQL JSONB."""
        return json.dumps(data, default=str)
    
    def retrieve_json_data(self, json_data: Any) -> Dict[str, Any]:
        """Convert PostgreSQL JSONB to Python dict."""
        if isinstance(json_data, dict):
            return json_data
        if isinstance(json_data, str):
            return json.loads(json_data)
        return {}
    
    def get_model_group_id(self, class_path: str, parameters: Dict[str, Any],
                          feature_names: List[str], model_config: Dict[str, Any]) -> int:
        """Get or create model group ID using PostgreSQL stored procedure.

        Returns model group id using stored procedure 'get_model_group_id' which will
        return the same value for models with the same class_path, parameters,
        features, and model_config.

        Args:
            class_path: A full classpath to the model class
            parameters: hyperparameters to give to the model constructor
            feature_names: list of feature names used in the model
            model_config: stored metadata about the model configuration

        Returns:
            A database id for the model group
        """
        db_conn = self.db_engine.raw_connection()
        cur = db_conn.cursor()

        # Check if the stored procedure exists
        cur.execute(
            "SELECT EXISTS ( "
            "       SELECT * "
            "       FROM pg_catalog.pg_proc "
            "       WHERE proname = 'get_model_group_id' ) "
        )
        condition = cur.fetchone()

        if condition[0]:  # stored procedure exists
            query = (
                "SELECT get_model_group_id( "
                "            '{class_path}'::TEXT, "
                "            '{parameters}'::JSONB, "
                "             ARRAY{feature_names}::TEXT [] , "
                "            '{model_config}'::JSONB )".format(
                    class_path=class_path,
                    parameters=json.dumps(parameters),
                    feature_names=list(feature_names),
                    model_config=json.dumps(model_config, sort_keys=True),
                )
            )
            cur.execute(query)
            db_conn.commit()
            model_group_id = cur.fetchone()[0]
        else:
            # Fallback: stored procedure doesn't exist
            model_group_id = None

        db_conn.close()
        return model_group_id
    
    def create_schemas(self) -> List[str]:
        """Return PostgreSQL schema creation statements."""
        return [
            "CREATE SCHEMA IF NOT EXISTS triage_metadata",
            "CREATE SCHEMA IF NOT EXISTS test_results",
            "CREATE SCHEMA IF NOT EXISTS train_results", 
            "CREATE SCHEMA IF NOT EXISTS triage_production"
        ]
    
    def serialize_array(self, array_data: List[Any]) -> List[Any]:
        """PostgreSQL arrays can be stored directly as Python lists."""
        return sorted(array_data) if array_data else []
    
    def deserialize_array(self, array_data: Any) -> List[Any]:
        """PostgreSQL arrays come back as Python lists."""
        return list(array_data) if array_data else []

    def format_timestamp_array_query(self, timestamp_strings: List[str]) -> str:
        """Generate PostgreSQL-specific SQL for timestamp array IN clause.

        Uses PostgreSQL's native ARRAY syntax with UNNEST for optimal performance.

        Args:
            timestamp_strings: List of timestamp strings

        Returns:
            PostgreSQL SQL fragment using UNNEST(ARRAY[...])

        Example:
            Input: ['2020-01-01', '2020-02-01']
            Output: "(SELECT (UNNEST (ARRAY['2020-01-01', '2020-02-01']::timestamp[])))"
        """
        if not timestamp_strings:
            return "(SELECT NULL WHERE FALSE)"  # Empty result set

        # Format as PostgreSQL array literal
        array_literal = str(timestamp_strings)  # Python list to PostgreSQL array format
        return f"(SELECT (UNNEST (ARRAY{array_literal}::timestamp[])))"
    
    def execute_sql_file(self, file_path: str) -> None:
        """Execute a SQL file (for stored procedures, etc.)."""
        with open(file_path, 'r') as f:
            sql_content = f.read()
        
        with self.db_engine.connect() as conn:
            conn.execute(text(sql_content))
            conn.commit()
    
    def setup_stored_procedures(self) -> None:
        """Setup PostgreSQL-specific stored procedures."""
        import os.path
        from ..results_schema import schema

        # Get the stored procedure file path
        proc_file = os.path.join(
            os.path.dirname(schema.__file__),
            "sql",
            "model_group_stored_procedure.sql"
        )

        self.execute_sql_file(proc_file)

    def get_labels_table_ddl(self, table_name: str) -> str:
        """Generate PostgreSQL DDL for creating labels table."""
        return f"""
            create table {table_name} (
            entity_id int,
            as_of_date date,
            label_timespan interval,
            label_name varchar,
            label_type varchar,
            label smallint
            )"""

    def cast_to_interval(self, value: str) -> str:
        """Generate PostgreSQL interval casting expression."""
        return f"'{value}'::interval"

    def create_index_statement(self, table_name: str, columns: List[str], index_name: str = None) -> str:
        """Generate PostgreSQL index creation statement."""
        columns_str = ', '.join(columns)
        if index_name:
            return f"create index {index_name} on {table_name} ({columns_str})"
        else:
            # PostgreSQL allows unnamed indexes
            return f"create index on {table_name} ({columns_str})"

    def get_existing_labels_check_query(self, labels_table: str, as_of_date: str,
                                       label_timespan: str, label_name: str) -> str:
        """Generate PostgreSQL query to check for existing labels."""
        return f"""select 1 from {labels_table}
                   where as_of_date = '{as_of_date}'
                   and label_timespan = '{label_timespan}'::interval
                   and label_name = '{label_name}'
                   limit 1"""

    def get_label_insert_query(self, labels_table: str, start_date: str,
                              label_timespan: str, label_name: str,
                              query_with_db_variables: str) -> str:
        """Generate PostgreSQL query to insert labels."""
        return f"""
            insert into {labels_table}
            select
                entities_and_outcomes.entity_id,
                '{start_date}' as as_of_date,
                '{label_timespan}'::interval as label_timespan,
                '{label_name}' as label_name,
                'binary' as label_type,
                entities_and_outcomes.outcome as label
            from ({query_with_db_variables}) entities_and_outcomes
            """

    def get_entity_date_table_ddl(self, table_name: str) -> str:
        """Generate PostgreSQL DDL for creating entity_date table."""
        return f"""create table {table_name} (
                    entity_id integer,
                    as_of_date timestamp,
                    active boolean
                )"""

    def get_entity_date_check_query(self, table_name: str, formatted_date: str) -> str:
        """Generate PostgreSQL query to check for existing entity_date records."""
        return f"""select 1 from {table_name}
                   where as_of_date = '{formatted_date}'
                   limit 1"""

    def get_entity_date_insert_query(self, table_name: str, formatted_date: str, dated_query: str) -> str:
        """Generate PostgreSQL query to insert entity_date records."""
        return f"""insert into {table_name}
                   select q.entity_id, '{formatted_date}'::timestamp, true
                   from ({dated_query}) q
                   group by 1, 2, 3"""

    def get_subset_entity_date_insert_query(self, table_name: str, formatted_date: str,
                                           dated_query: str, cohort_table: str) -> str:
        """Generate PostgreSQL query to insert subset entity_date records."""
        return f"""insert into {table_name}
                   select q.entity_id, '{formatted_date}'::timestamp, true
                   from (
                       with subset as ({dated_query})
                       select
                           c.entity_id
                       from subset s inner join {cohort_table} c
                       on s.entity_id = c.entity_id
                       and c.as_of_date = '{formatted_date}'::date
                   ) q
                   group by 1, 2, 3"""

    def get_labels_to_entity_date_query(self, entity_table: str, labels_table: str) -> str:
        """Generate PostgreSQL query to populate entity_date from labels table."""
        return f"""
            insert into {entity_table}
            select distinct entity_id, as_of_date, true
            from (
                select distinct l.entity_id, l.as_of_date
                from {labels_table} as l
                left join (select distinct as_of_date from {entity_table}) as c
                    on l.as_of_date::DATE = c.as_of_date::DATE
                where c.as_of_date IS NULL
            ) as sub
        """

    def get_protected_groups_table_ddl(self, table_name: str, attribute_columns: List[str]) -> str:
        """Generate PostgreSQL DDL for creating protected groups table."""
        attribute_ddl = ', '.join([f"{col} varchar" for col in attribute_columns])
        return f"""
            create table if not exists {table_name} (
                entity_id int,
                as_of_date date,
                {attribute_ddl},
                cohort_hash text
            )"""

    def get_protected_groups_check_query(self, table_name: str, as_of_date: str, cohort_hash: str) -> str:
        """Generate PostgreSQL query to check for existing protected groups records."""
        return f"""select 1 from {table_name}
                   where as_of_date = '{as_of_date}'
                   and cohort_hash = '{cohort_hash}'
                   limit 1"""

    def get_protected_groups_insert_query(self, table_name: str, as_of_date: str,
                                         attribute_columns: List[str], cohort_hash: str,
                                         cohort_table_name: str, from_obj: str,
                                         entity_id_column: str, knowledge_date_column: str) -> str:
        """Generate PostgreSQL query to insert protected groups records."""
        attribute_select = ", ".join([str(col) for col in attribute_columns])

        return f"""
            insert into {table_name}
            select distinct on (cohort.entity_id, cohort.as_of_date)
                cohort.entity_id,
                '{as_of_date}'::date as as_of_date,
                {attribute_select},
                '{cohort_hash}' as cohort_hash
            from {cohort_table_name} cohort
            left join (select * from {from_obj}) from_obj  on
                cohort.entity_id = from_obj.{entity_id_column} and
                cohort.as_of_date > from_obj.{knowledge_date_column}
            where cohort.as_of_date = '{as_of_date}'::date
            order by cohort.entity_id, cohort.as_of_date, {knowledge_date_column} desc
        """

    def get_protected_groups_select_query(self, table_name: str, as_of_dates: List[str], cohort_hash: str) -> str:
        """Generate PostgreSQL query to retrieve protected groups data."""
        # Use the existing timestamp array formatting method
        dates_clause = self.format_timestamp_array_query(as_of_dates)

        return f"""
            with dates as (
                select * from {dates_clause} as as_of_date
            )
            select *
            from {table_name}
            join dates using(as_of_date)
            where cohort_hash = '{cohort_hash}'
        """

    def query_to_dataframe(self, query: str, parse_dates: List[str] = None, index_col=None):
        """Execute query using PostgreSQL's optimized pg_copy_from method."""
        return pd.DataFrame.pg_copy_from(
            query,
            connectable=self.db_engine,
            parse_dates=parse_dates or [],
            index_col=index_col
        )

    def get_table_columns_query(self, table_name: str, schema_name: str, exclude_columns: List[str]) -> str:
        """Generate PostgreSQL query to get table column names from information_schema."""
        from triage.component.architect.utils import str_in_sql

        return f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}' AND
                  table_schema = '{schema_name}' AND
                  column_name NOT IN ({str_in_sql(exclude_columns)})
        """

    def get_array_contains_expression(self, column: str, value: str, data_type: str = "varchar") -> str:
        """Generate PostgreSQL array contains expression using ANY operator."""
        return f"'{value}' = ANY({column})"

    def get_table_exists_check_query(self, schema: str, table: str) -> str:
        """Generate PostgreSQL query to check if a table exists by direct query."""
        return f"SELECT 1 FROM {schema}.{table} LIMIT 1"

    def build_array_categorical_choice(self, choice: str) -> str:
        """Generate PostgreSQL array literal for categorical choice comparison."""
        return f"array['{choice}'::varchar]"

    def get_explain_query_prefix(self) -> str:
        """Return PostgreSQL EXPLAIN prefix."""
        return "EXPLAIN"

    def get_limit_clause(self, limit: int) -> str:
        """Return PostgreSQL LIMIT clause."""
        return f"LIMIT {limit}"

    def export_query_to_csv(self, query: str, cursor, bio, include_header: bool = True):
        """Execute PostgreSQL COPY TO STDOUT operation."""
        header = "HEADER" if include_header else ""
        copy_sql = f"COPY ({query}) TO STDOUT WITH CSV {header}"
        cursor.copy_expert(copy_sql, bio)

    def get_drop_table_if_exists_query(self, table_name: str) -> str:
        """Generate PostgreSQL DROP TABLE IF EXISTS statement."""
        return f"DROP TABLE IF EXISTS {table_name}"

    def get_create_table_as_query(self, table_name: str, select_query: str) -> str:
        """Generate PostgreSQL CREATE TABLE AS statement."""
        return f"CREATE TABLE {table_name} AS ({select_query})"

    def get_existing_importances_count_query(self, model_id: int, as_of_date: str, method: str) -> str:
        """Generate PostgreSQL query to count existing individual importances."""
        return f"""SELECT COUNT(*) FROM test_results.individual_importances
                   WHERE model_id = {model_id}
                   AND as_of_date = '{as_of_date}'
                   AND method = '{method}'"""

    def delete_individual_importances_query(self, model_id: int, as_of_date: str, method: str) -> str:
        """Generate PostgreSQL query to delete individual importances."""
        return f"""DELETE FROM test_results.individual_importances
                   WHERE model_id = {model_id}
                   AND as_of_date = '{as_of_date}'
                   AND method = '{method}'"""

    def get_subset_table_query(self, as_of_dates: List[str], subset_table_name: str) -> str:
        """Generate PostgreSQL query to retrieve subset table data using UNNEST."""
        # Format dates for PostgreSQL array
        formatted_dates = [f"'{date}'" for date in as_of_dates]
        dates_array = f"ARRAY[{', '.join(formatted_dates)}]::timestamp[]"

        return f"""
            WITH dates AS (
                SELECT UNNEST({dates_array}) AS as_of_date
            )
            SELECT entity_id, as_of_date, active
            FROM {subset_table_name}
            JOIN dates USING(as_of_date)
        """