import json
import csv
import io
from typing import Dict, List, Any
from sqlalchemy import String, Text, text, Integer
from sqlalchemy.orm import Session
from sqlalchemy.dialects.oracle import JSON, VARRAY
import pandas as pd

from .base import DatabaseAdapter


class OracleAdapter(DatabaseAdapter):
    """Oracle 23ai database adapter for Triage.

    Implements the DatabaseAdapter interface using Oracle 23ai features
    like native JSON storage, JSON arrays, and JSON_TABLE operations.
    """
    
    def get_json_column_type(self):
        """Return Oracle 23ai native JSON column type."""
        return JSON  # Native JSON type in Oracle 23ai
    
    def get_array_column_type(self, item_type):
        """Return Oracle 23ai native array type based on item type."""
        # Use native VARRAY for simple types, JSON for complex types
        if item_type == String or item_type == Text:
            # Oracle VARRAY for string arrays (max 1000 elements)
            return VARRAY(String(255), 1000)
        elif item_type == Integer:
            # Oracle VARRAY for integer arrays
            return VARRAY(Integer, 1000)
        else:
            # Complex types still use JSON for flexibility
            return JSON
    
    def store_json_data(self, data: Dict[str, Any]) -> str:
        """Convert Python dict to JSON string for Oracle CLOB storage."""
        return json.dumps(data, default=str)
    
    def retrieve_json_data(self, json_data: Any) -> Dict[str, Any]:
        """Convert Oracle CLOB JSON to Python dict."""
        if isinstance(json_data, dict):
            return json_data
        if isinstance(json_data, str):
            try:
                return json.loads(json_data)
            except (json.JSONDecodeError, TypeError):
                return {}
        return {}
    
    def get_model_group_id(self, class_path: str, parameters: Dict[str, Any],
                          feature_names: List[str], model_config: Dict[str, Any]) -> int:
        """Get or create model group ID using Oracle-specific SQL.

        Oracle implementation that mimics the PostgreSQL stored procedure behavior
        but uses Oracle-specific syntax and features.

        Args:
            class_path: A full classpath to the model class
            parameters: hyperparameters to give to the model constructor
            feature_names: list of feature names used in the model
            model_config: stored metadata about the model configuration

        Returns:
            A database id for the model group
        """
        # Check if Oracle stored function exists (equivalent to PostgreSQL procedure)
        db_conn = self.db_engine.raw_connection()
        cur = db_conn.cursor()

        # Oracle equivalent of checking for stored procedure/function
        cur.execute("""
            SELECT COUNT(*)
            FROM user_objects
            WHERE object_name = 'GET_MODEL_GROUP_ID'
            AND object_type = 'FUNCTION'
        """)

        procedure_exists = cur.fetchone()[0] > 0

        if procedure_exists:
            # Call Oracle stored function using Oracle-specific syntax
            # Oracle uses CLOB for JSON and different parameter binding
            feature_names_str = ','.join(sorted(feature_names))

            # Oracle stored function call with proper parameter binding
            cur.execute("""
                SELECT admin.get_model_group_id(
                    :class_path,
                    :parameters_json,
                    :feature_names_list,
                    :model_config_json
                ) FROM DUAL
            """, {
                'class_path': class_path,
                'parameters_json': json.dumps(parameters),
                'feature_names_list': feature_names_str,
                'model_config_json': json.dumps(model_config, sort_keys=True)
            })

            result = cur.fetchone()
            model_group_id = result[0] if result else None
        else:
            # Fallback: Oracle stored function doesn't exist
            model_group_id = None

        db_conn.close()
        return model_group_id
    
    def create_schemas(self) -> List[str]:
        """Return Oracle schema creation statements."""
        return [
            "CREATE USER triage_metadata IDENTIFIED BY password123",
            "CREATE USER test_results IDENTIFIED BY password123",
            "CREATE USER train_results IDENTIFIED BY password123",
            "CREATE USER triage_production IDENTIFIED BY password123",
            "GRANT CREATE SESSION TO triage_metadata",
            "GRANT CREATE SESSION TO test_results", 
            "GRANT CREATE SESSION TO train_results",
            "GRANT CREATE SESSION TO triage_production"
        ]
    
    def serialize_array(self, array_data: List[Any]) -> List[Any]:
        """Convert Python list to Oracle native array format.

        For Oracle VARRAY columns, we can store Python lists directly.
        No JSON conversion needed.
        """
        if not array_data:
            return []
        return sorted(array_data)

    def deserialize_array(self, array_data: Any) -> List[Any]:
        """Convert Oracle native array to Python list.

        Oracle VARRAY columns return as Python lists directly.
        Handle JSON fallback for complex types.
        """
        if not array_data:
            return []
        if isinstance(array_data, list):
            # Native Oracle VARRAY comes back as Python list
            return array_data
        if isinstance(array_data, str):
            try:
                # Fallback for JSON-stored arrays
                return json.loads(array_data)
            except (json.JSONDecodeError, TypeError):
                return []
        return []
    
    def format_timestamp_array_query(self, timestamp_strings: List[str]) -> str:
        """Generate Oracle 23ai SQL using VALUES clause for timestamp array IN clause.

        Uses Oracle's native VALUES clause for optimal performance - no JSON overhead.

        Args:
            timestamp_strings: List of timestamp strings

        Returns:
            Oracle SQL fragment using VALUES clause

        Example:
            Input: ['2020-01-01', '2020-02-01']
            Output: "(VALUES (TO_DATE('2020-01-01', 'YYYY-MM-DD')), (TO_DATE('2020-02-01', 'YYYY-MM-DD')))"
        """
        if not timestamp_strings:
            return "(SELECT NULL FROM DUAL WHERE 1=0)"  # Oracle empty result set

        # Use VALUES clause - native SQL, no JSON conversion needed
        values_list = [f"(TO_DATE('{ts}', 'YYYY-MM-DD'))" for ts in timestamp_strings]
        return f"(VALUES {', '.join(values_list)})"

    def setup_stored_procedures(self) -> None:
        """Oracle doesn't need stored procedures - using Python logic instead."""
        # Oracle adapter uses pure Python business logic
        # No stored procedures needed
        pass

    def get_labels_table_ddl(self, table_name: str) -> str:
        """Generate Oracle DDL for creating labels table."""
        return f"""
            CREATE TABLE {table_name} (
            entity_id NUMBER(10),
            as_of_date DATE,
            label_timespan INTERVAL DAY TO SECOND,
            label_name VARCHAR2(255),
            label_type VARCHAR2(255),
            label NUMBER(5)
            )"""

    def cast_to_interval(self, value: str) -> str:
        """Generate Oracle interval casting expression."""
        # Oracle uses different interval syntax
        return f"INTERVAL '{value}' DAY TO SECOND"

    def create_index_statement(self, table_name: str, columns: List[str], index_name: str = None) -> str:
        """Generate Oracle index creation statement."""
        columns_str = ', '.join(columns)
        if not index_name:
            # Oracle requires explicit index names, so generate one
            index_name = f"idx_{table_name}_{'_'.join(columns)}".replace('.', '_')
        return f"CREATE INDEX {index_name} ON {table_name} ({columns_str})"

    def get_existing_labels_check_query(self, labels_table: str, as_of_date: str,
                                       label_timespan: str, label_name: str) -> str:
        """Generate Oracle query to check for existing labels."""
        return f"""select 1 from {labels_table}
                   where as_of_date = '{as_of_date}'
                   and label_timespan = INTERVAL '{label_timespan}' DAY TO SECOND
                   and label_name = '{label_name}'
                   and ROWNUM <= 1"""

    def get_label_insert_query(self, labels_table: str, start_date: str,
                              label_timespan: str, label_name: str,
                              query_with_db_variables: str) -> str:
        """Generate Oracle query to insert labels."""
        return f"""
            INSERT INTO {labels_table}
            SELECT
                entities_and_outcomes.entity_id,
                '{start_date}' as as_of_date,
                INTERVAL '{label_timespan}' DAY TO SECOND as label_timespan,
                '{label_name}' as label_name,
                'binary' as label_type,
                entities_and_outcomes.outcome as label
            FROM ({query_with_db_variables}) entities_and_outcomes
            """

    def get_entity_date_table_ddl(self, table_name: str) -> str:
        """Generate Oracle DDL for creating entity_date table."""
        return f"""CREATE TABLE {table_name} (
                    entity_id NUMBER(10),
                    as_of_date DATE,
                    active NUMBER(1)
                )"""

    def get_entity_date_check_query(self, table_name: str, formatted_date: str) -> str:
        """Generate Oracle query to check for existing entity_date records."""
        return f"""SELECT 1 FROM {table_name}
                   WHERE as_of_date = TO_DATE('{formatted_date}', 'YYYY-MM-DD')
                   AND ROWNUM <= 1"""

    def get_entity_date_insert_query(self, table_name: str, formatted_date: str, dated_query: str) -> str:
        """Generate Oracle query to insert entity_date records."""
        return f"""INSERT INTO {table_name}
                   SELECT q.entity_id, TO_DATE('{formatted_date}', 'YYYY-MM-DD'), 1
                   FROM ({dated_query}) q
                   GROUP BY q.entity_id, TO_DATE('{formatted_date}', 'YYYY-MM-DD'), 1"""

    def get_subset_entity_date_insert_query(self, table_name: str, formatted_date: str,
                                           dated_query: str, cohort_table: str) -> str:
        """Generate Oracle query to insert subset entity_date records."""
        return f"""INSERT INTO {table_name}
                   SELECT q.entity_id, TO_DATE('{formatted_date}', 'YYYY-MM-DD'), 1
                   FROM (
                       WITH subset AS ({dated_query})
                       SELECT
                           c.entity_id
                       FROM subset s INNER JOIN {cohort_table} c
                       ON s.entity_id = c.entity_id
                       AND c.as_of_date = TO_DATE('{formatted_date}', 'YYYY-MM-DD')
                   ) q
                   GROUP BY q.entity_id, TO_DATE('{formatted_date}', 'YYYY-MM-DD'), 1"""

    def get_labels_to_entity_date_query(self, entity_table: str, labels_table: str) -> str:
        """Generate Oracle query to populate entity_date from labels table."""
        return f"""
            INSERT INTO {entity_table}
            SELECT DISTINCT entity_id, as_of_date, 1
            FROM (
                SELECT DISTINCT l.entity_id, l.as_of_date
                FROM {labels_table} l
                LEFT JOIN (SELECT DISTINCT as_of_date FROM {entity_table}) c
                    ON TRUNC(l.as_of_date) = TRUNC(c.as_of_date)
                WHERE c.as_of_date IS NULL
            )
        """

    def get_protected_groups_table_ddl(self, table_name: str, attribute_columns: List[str]) -> str:
        """Generate Oracle DDL for creating protected groups table."""
        attribute_ddl = ', '.join([f"{col} VARCHAR2(255)" for col in attribute_columns])
        return f"""
            CREATE TABLE {table_name} (
                entity_id NUMBER(10),
                as_of_date DATE,
                {attribute_ddl},
                cohort_hash CLOB
            )"""

    def get_protected_groups_check_query(self, table_name: str, as_of_date: str, cohort_hash: str) -> str:
        """Generate Oracle query to check for existing protected groups records."""
        return f"""SELECT 1 FROM {table_name}
                   WHERE as_of_date = TO_DATE('{as_of_date}', 'YYYY-MM-DD')
                   AND cohort_hash = '{cohort_hash}'
                   AND ROWNUM <= 1"""

    def get_protected_groups_insert_query(self, table_name: str, as_of_date: str,
                                         attribute_columns: List[str], cohort_hash: str,
                                         cohort_table_name: str, from_obj: str,
                                         entity_id_column: str, knowledge_date_column: str) -> str:
        """Generate Oracle query to insert protected groups records."""
        attribute_select = ", ".join([str(col) for col in attribute_columns])

        # Oracle doesn't have DISTINCT ON, so we use ROW_NUMBER() window function
        return f"""
            INSERT INTO {table_name}
            SELECT entity_id, as_of_date, {attribute_select}, cohort_hash
            FROM (
                SELECT
                    cohort.entity_id,
                    TO_DATE('{as_of_date}', 'YYYY-MM-DD') as as_of_date,
                    {attribute_select},
                    '{cohort_hash}' as cohort_hash,
                    ROW_NUMBER() OVER (
                        PARTITION BY cohort.entity_id, cohort.as_of_date
                        ORDER BY {knowledge_date_column} DESC
                    ) as rn
                FROM {cohort_table_name} cohort
                LEFT JOIN (SELECT * FROM {from_obj}) from_obj ON
                    cohort.entity_id = from_obj.{entity_id_column} AND
                    cohort.as_of_date > from_obj.{knowledge_date_column}
                WHERE cohort.as_of_date = TO_DATE('{as_of_date}', 'YYYY-MM-DD')
            )
            WHERE rn = 1
        """

    def get_protected_groups_select_query(self, table_name: str, as_of_dates: List[str], cohort_hash: str) -> str:
        """Generate Oracle 23ai query to retrieve protected groups data using VALUES clause."""
        # Use VALUES clause - no JSON conversion needed
        values_list = [f"(TO_DATE('{date}', 'YYYY-MM-DD'))" for date in as_of_dates]

        return f"""
            WITH dates AS (
                SELECT COLUMN_VALUE as as_of_date
                FROM (VALUES {', '.join(values_list)})
            )
            SELECT p.*
            FROM {table_name} p
            JOIN dates d ON TRUNC(p.as_of_date) = TRUNC(d.as_of_date)
            WHERE p.cohort_hash = '{cohort_hash}'
        """

    def query_to_dataframe(self, query: str, parse_dates: List[str] = None, index_col=None):
        """Execute query using Oracle-specific connection method."""
        with self.db_engine.connect() as conn:
            return pd.read_sql_query(
                query,
                conn,
                parse_dates=parse_dates or [],
                index_col=index_col
            )

    def get_table_columns_query(self, table_name: str, schema_name: str, exclude_columns: List[str]) -> str:
        """Generate Oracle query to get table column names from ALL_TAB_COLUMNS."""
        from triage.component.architect.utils import str_in_sql

        # Oracle typically uses uppercase for object names
        return f"""
            SELECT COLUMN_NAME
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME = UPPER('{table_name}') AND
                  OWNER = UPPER('{schema_name}') AND
                  COLUMN_NAME NOT IN ({str_in_sql([col.upper() for col in exclude_columns])})
        """

    def get_array_contains_expression(self, column: str, value: str, data_type: str = "varchar") -> str:
        """Generate Oracle 23ai array contains expression using native MEMBER OF.

        Uses Oracle's native MEMBER OF operator for VARRAY containment checks.
        Falls back to JSON_EXISTS for JSON arrays.
        """
        # For native VARRAY columns - use MEMBER OF operator
        if data_type in ("varchar", "string", "text"):
            return f"'{value}' MEMBER OF {column}"
        elif data_type in ("integer", "int"):
            return f"{value} MEMBER OF {column}"
        else:
            # Fallback to JSON_EXISTS for JSON arrays
            return f"JSON_EXISTS({column}, '$[*]?(@ == \"{value}\")')"

    def get_table_exists_check_query(self, schema: str, table: str) -> str:
        """Generate Oracle query to check if a table exists by direct query."""
        return f"SELECT 1 FROM {schema}.{table} WHERE ROWNUM <= 1"

    def build_array_categorical_choice(self, choice: str) -> str:
        """Generate Oracle equivalent for categorical choice comparison.

        Since Oracle doesn't have native arrays like PostgreSQL, we return
        the choice value directly for use with comma-separated string operations.
        """
        return f"'{choice}'"

    def get_explain_query_prefix(self) -> str:
        """Return Oracle EXPLAIN prefix."""
        return "EXPLAIN PLAN FOR"

    def get_limit_clause(self, limit: int) -> str:
        """Return Oracle ROWNUM clause equivalent to LIMIT."""
        return f"WHERE ROWNUM <= {limit}"

    def export_query_to_csv(self, query: str, cursor, bio, include_header: bool = True):
        """Execute Oracle CSV export operation using cursor fetchall.

        Oracle doesn't have direct COPY TO STDOUT equivalent, so we fetch
        data and write CSV manually.
        """
        # Execute the query
        cursor.execute(query)

        # Get column names for header
        columns = [desc[0] for desc in cursor.description]

        # Create CSV writer using StringIO as intermediate
        string_io = io.StringIO()
        csv_writer = csv.writer(string_io)

        # Write header if requested
        if include_header:
            csv_writer.writerow(columns)

        # Fetch and write all rows
        rows = cursor.fetchall()
        for row in rows:
            csv_writer.writerow(row)

        # Convert to bytes and write to bio
        csv_data = string_io.getvalue().encode('utf-8')
        bio.write(csv_data)

    def get_drop_table_if_exists_query(self, table_name: str) -> str:
        """Generate Oracle 23ai DROP TABLE IF EXISTS statement."""
        return f"DROP TABLE IF EXISTS {table_name}"

    def get_create_table_as_query(self, table_name: str, select_query: str) -> str:
        """Generate Oracle CREATE TABLE AS statement."""
        return f"CREATE TABLE {table_name} AS {select_query}"

    def get_existing_importances_count_query(self, model_id: int, as_of_date: str, method: str) -> str:
        """Generate Oracle query to count existing individual importances."""
        return f"""SELECT COUNT(*) FROM test_results.individual_importances
                   WHERE model_id = {model_id}
                   AND as_of_date = TO_DATE('{as_of_date}', 'YYYY-MM-DD')
                   AND method = '{method}'"""

    def delete_individual_importances_query(self, model_id: int, as_of_date: str, method: str) -> str:
        """Generate Oracle query to delete individual importances."""
        return f"""DELETE FROM test_results.individual_importances
                   WHERE model_id = {model_id}
                   AND as_of_date = TO_DATE('{as_of_date}', 'YYYY-MM-DD')
                   AND method = '{method}'"""

    def get_subset_table_query(self, as_of_dates: List[str], subset_table_name: str) -> str:
        """Generate Oracle query to retrieve subset table data using VALUES clause."""
        # Format dates for Oracle VALUES clause
        values_list = [f"(TO_TIMESTAMP('{date}', 'YYYY-MM-DD HH24:MI:SS.FF6'))" for date in as_of_dates]

        return f"""
            WITH dates AS (
                SELECT COLUMN_VALUE as as_of_date
                FROM (VALUES {', '.join(values_list)})
            )
            SELECT entity_id, as_of_date, active
            FROM {subset_table_name}
            JOIN dates USING(as_of_date)
        """