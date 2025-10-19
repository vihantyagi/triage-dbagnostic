from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.engine import Engine


class DatabaseAdapter(ABC):
    """Abstract base class for database-specific operations in Triage.
    
    This adapter pattern allows Triage to work with different database systems
    by abstracting database-specific functionality like JSON storage, arrays,
    and stored procedures.
    """
    
    def __init__(self, db_engine: Engine):
        self.db_engine = db_engine
    
    @abstractmethod
    def get_json_column_type(self):
        """Return the appropriate SQLAlchemy column type for JSON data.
        
        Returns:
            SQLAlchemy column type for JSON storage (e.g., JSONB for PostgreSQL)
        """
        pass
    
    @abstractmethod
    def get_array_column_type(self, item_type):
        """Return the appropriate SQLAlchemy column type for arrays.
        
        Args:
            item_type: The type of items in the array (e.g., String, Integer)
            
        Returns:
            SQLAlchemy column type for array storage
        """
        pass
    
    @abstractmethod
    def store_json_data(self, data: Dict[str, Any]) -> str:
        """Convert Python dict to database-specific JSON format.
        
        Args:
            data: Python dictionary to store
            
        Returns:
            Database-specific JSON representation
        """
        pass
    
    @abstractmethod
    def retrieve_json_data(self, json_data: Any) -> Dict[str, Any]:
        """Convert database JSON to Python dict.
        
        Args:
            json_data: Database-specific JSON data
            
        Returns:
            Python dictionary
        """
        pass
    
    @abstractmethod
    def get_model_group_id(self, class_path: str, parameters: Dict[str, Any],
                          feature_names: List[str], model_config: Dict[str, Any]) -> int:
        """Get or create model group ID using database-specific implementation.

        This replaces the PostgreSQL stored procedure with database-agnostic logic.
        Each adapter implements this using appropriate database-specific methods.

        Args:
            class_path: A full classpath to the model class
            parameters: hyperparameters to give to the model constructor
            feature_names: list of feature names used in the model
            model_config: stored metadata about the model configuration

        Returns:
            Model group ID from the database
        """
        pass
    
    @abstractmethod
    def create_schemas(self) -> List[str]:
        """Return list of SQL statements to create required schemas.
        
        Returns:
            List of SQL DDL statements for schema creation
        """
        pass
    
    @abstractmethod
    def serialize_array(self, array_data: List[Any]) -> Any:
        """Convert Python list to database-specific array format.
        
        Args:
            array_data: Python list to serialize
            
        Returns:
            Database-specific array representation
        """
        pass
    
    @abstractmethod
    def deserialize_array(self, array_data: Any) -> List[Any]:
        """Convert database array to Python list.

        Args:
            array_data: Database-specific array data

        Returns:
            Python list
        """
        pass

    @abstractmethod
    def format_timestamp_array_query(self, timestamp_strings: List[str]) -> str:
        """Generate database-specific SQL for timestamp array IN clause.

        This handles database-specific syntax for querying against a list of timestamps.
        For example, PostgreSQL uses UNNEST(ARRAY[...]) while Oracle uses simple IN lists.

        Args:
            timestamp_strings: List of timestamp strings (e.g., ['2020-01-01', '2020-02-01'])

        Returns:
            Database-specific SQL fragment for use in IN clause

        Example:
            PostgreSQL: "(SELECT (UNNEST (ARRAY['2020-01-01', '2020-02-01']::timestamp[])))"
            Oracle: "('2020-01-01', '2020-02-01')"
        """
        pass

    @abstractmethod
    def get_labels_table_ddl(self, table_name: str) -> str:
        """Generate database-specific DDL for creating labels table.

        Args:
            table_name: Name of the labels table to create

        Returns:
            SQL DDL statement to create the labels table
        """
        pass

    @abstractmethod
    def cast_to_interval(self, value: str) -> str:
        """Generate database-specific interval casting expression.

        Args:
            value: String value to cast to interval type

        Returns:
            Database-specific interval casting expression

        Example:
            PostgreSQL: "'{value}'::interval"
            Oracle: "INTERVAL '{value}' DAY TO SECOND"
        """
        pass

    @abstractmethod
    def create_index_statement(self, table_name: str, columns: List[str], index_name: Optional[str] = None) -> str:
        """Generate database-specific index creation statement.

        Args:
            table_name: Name of the table to index
            columns: List of column names to include in index
            index_name: Optional index name (auto-generated if not provided)

        Returns:
            SQL statement to create the index
        """
        pass

    @abstractmethod
    def get_existing_labels_check_query(self, labels_table: str, as_of_date: str,
                                       label_timespan: str, label_name: str) -> str:
        """Generate database-specific query to check for existing labels.

        Args:
            labels_table: Name of the labels table
            as_of_date: The as_of_date to check for
            label_timespan: The label timespan to check for
            label_name: The label name to check for

        Returns:
            SQL query that returns 1 if matching labels exist, empty result otherwise
        """
        pass

    @abstractmethod
    def get_label_insert_query(self, labels_table: str, start_date: str,
                              label_timespan: str, label_name: str,
                              query_with_db_variables: str) -> str:
        """Generate database-specific query to insert labels.

        Args:
            labels_table: Name of the labels table to insert into
            start_date: The as_of_date for the labels
            label_timespan: The label timespan
            label_name: The label name
            query_with_db_variables: The user query with variables substituted

        Returns:
            Complete SQL INSERT statement for inserting labels
        """
        pass

    @abstractmethod
    def get_entity_date_table_ddl(self, table_name: str) -> str:
        """Generate database-specific DDL for creating entity_date table.

        Args:
            table_name: Name of the entity_date table to create

        Returns:
            SQL DDL statement to create the entity_date table
        """
        pass

    @abstractmethod
    def get_entity_date_check_query(self, table_name: str, formatted_date: str) -> str:
        """Generate database-specific query to check for existing entity_date records.

        Args:
            table_name: Name of the entity_date table
            formatted_date: The date to check for (ISO format)

        Returns:
            SQL query that returns 1 if matching records exist
        """
        pass

    @abstractmethod
    def get_entity_date_insert_query(self, table_name: str, formatted_date: str, dated_query: str) -> str:
        """Generate database-specific query to insert entity_date records.

        Args:
            table_name: Name of the entity_date table
            formatted_date: The as_of_date (ISO format)
            dated_query: The user query with date substituted

        Returns:
            Complete SQL INSERT statement for entity_date records
        """
        pass

    @abstractmethod
    def get_subset_entity_date_insert_query(self, table_name: str, formatted_date: str,
                                           dated_query: str, cohort_table: str) -> str:
        """Generate database-specific query to insert subset entity_date records.

        Args:
            table_name: Name of the entity_date table
            formatted_date: The as_of_date (ISO format)
            dated_query: The user query with date substituted
            cohort_table: Name of the cohort table to join with

        Returns:
            Complete SQL INSERT statement for subset entity_date records
        """
        pass

    @abstractmethod
    def get_labels_to_entity_date_query(self, entity_table: str, labels_table: str) -> str:
        """Generate database-specific query to populate entity_date from labels table.

        Args:
            entity_table: Name of the entity_date table
            labels_table: Name of the labels table

        Returns:
            Complete SQL INSERT statement to populate entity_date from labels
        """
        pass

    @abstractmethod
    def get_protected_groups_table_ddl(self, table_name: str, attribute_columns: List[str]) -> str:
        """Generate database-specific DDL for creating protected groups table.

        Args:
            table_name: Name of the protected groups table to create
            attribute_columns: List of attribute column names

        Returns:
            SQL DDL statement to create the protected groups table
        """
        pass

    @abstractmethod
    def get_protected_groups_check_query(self, table_name: str, as_of_date: str, cohort_hash: str) -> str:
        """Generate database-specific query to check for existing protected groups records.

        Args:
            table_name: Name of the protected groups table
            as_of_date: The date to check for
            cohort_hash: The cohort hash to check for

        Returns:
            SQL query that returns 1 if matching records exist
        """
        pass

    @abstractmethod
    def get_protected_groups_insert_query(self, table_name: str, as_of_date: str,
                                         attribute_columns: List[str], cohort_hash: str,
                                         cohort_table_name: str, from_obj: str,
                                         entity_id_column: str, knowledge_date_column: str) -> str:
        """Generate database-specific query to insert protected groups records.

        Args:
            table_name: Name of the protected groups table
            as_of_date: The as_of_date for the records
            attribute_columns: List of attribute column names
            cohort_hash: The cohort hash
            cohort_table_name: Name of the cohort table
            from_obj: The source table/query for attributes
            entity_id_column: Name of the entity ID column
            knowledge_date_column: Name of the knowledge date column

        Returns:
            Complete SQL INSERT statement for protected groups records
        """
        pass

    @abstractmethod
    def get_protected_groups_select_query(self, table_name: str, as_of_dates: List[str], cohort_hash: str) -> str:
        """Generate database-specific query to retrieve protected groups data.

        Args:
            table_name: Name of the protected groups table
            as_of_dates: List of as_of_dates to retrieve (formatted strings)
            cohort_hash: The cohort hash to filter by

        Returns:
            Complete SQL SELECT statement to retrieve protected groups data
        """
        pass

    @abstractmethod
    def query_to_dataframe(self, query: str, parse_dates: List[str] = None, index_col=None):
        """Execute query and return results as pandas DataFrame using database-specific optimizations.

        Args:
            query: SQL query string to execute
            parse_dates: List of column names to parse as dates
            index_col: Column(s) to use as the DataFrame index

        Returns:
            pandas.DataFrame with query results
        """
        pass

    @abstractmethod
    def get_table_columns_query(self, table_name: str, schema_name: str, exclude_columns: List[str]) -> str:
        """Generate database-specific query to get table column names.

        Args:
            table_name: Name of the table to get columns for
            schema_name: Name of the schema containing the table
            exclude_columns: List of column names to exclude from results

        Returns:
            SQL query string to retrieve column names
        """
        pass

    @abstractmethod
    def get_array_contains_expression(self, column: str, value: str, data_type: str = "varchar") -> str:
        """Generate database-specific SQL expression for array contains operation.

        Args:
            column: Column name that contains the array
            value: Value to check for in the array
            data_type: Data type of the array elements (default: varchar)

        Returns:
            SQL expression that evaluates to true if value is in the array

        Example:
            PostgreSQL: "'{value}' = ANY({column})"
            Oracle: "EXISTS (SELECT 1 FROM TABLE({column}) WHERE COLUMN_VALUE = '{value}')"
        """
        pass

    @abstractmethod
    def get_table_exists_check_query(self, schema: str, table: str) -> str:
        """Generate database-specific query to check if a table exists by direct query.

        Args:
            schema: Schema name containing the table
            table: Table name to check for existence

        Returns:
            SQL query that attempts to query the table directly

        Example:
            PostgreSQL: "SELECT 1 FROM {schema}.{table} LIMIT 1"
            Oracle: "SELECT 1 FROM {schema}.{table} WHERE ROWNUM <= 1"
        """
        pass

    @abstractmethod
    def build_array_categorical_choice(self, choice: str) -> str:
        """Generate database-specific array literal for categorical choice comparison.

        Args:
            choice: The categorical choice value

        Returns:
            Database-specific array literal expression

        Example:
            PostgreSQL: "array['{choice}'::varchar]"
            Oracle: "'{choice}'" (for comma-separated string approach)
        """
        pass

    @abstractmethod
    def get_explain_query_prefix(self) -> str:
        """Return database-specific EXPLAIN prefix for query analysis.

        Returns:
            Database-specific EXPLAIN command prefix

        Example:
            PostgreSQL: "EXPLAIN"
            Oracle: "EXPLAIN PLAN FOR"
        """
        pass

    @abstractmethod
    def get_limit_clause(self, limit: int) -> str:
        """Return database-specific limit clause.

        Args:
            limit: Number of rows to limit

        Returns:
            Database-specific limit clause

        Example:
            PostgreSQL: "LIMIT {limit}"
            Oracle: "WHERE ROWNUM <= {limit}"
        """
        pass

    @abstractmethod
    def export_query_to_csv(self, query: str, cursor, bio, include_header: bool = True):
        """Execute database-specific CSV export operation.

        Args:
            query: SQL query to execute and export
            cursor: Database cursor object
            bio: BytesIO object to write CSV data to
            include_header: Whether to include column headers in CSV

        Note:
            This method should execute the query and write CSV data to the bio object.
            PostgreSQL uses COPY TO STDOUT, Oracle may use different approaches.
        """
        pass

    @abstractmethod
    def get_drop_table_if_exists_query(self, table_name: str) -> str:
        """Generate database-specific DROP TABLE IF EXISTS statement.

        Args:
            table_name: Full table name including schema if needed

        Returns:
            Database-specific DROP TABLE statement

        Example:
            PostgreSQL: "DROP TABLE IF EXISTS {table_name}"
            Oracle: "DROP TABLE {table_name}" (with error handling)
        """
        pass

    @abstractmethod
    def get_create_table_as_query(self, table_name: str, select_query: str) -> str:
        """Generate database-specific CREATE TABLE AS statement.

        Args:
            table_name: Name of table to create
            select_query: SELECT query to use for table creation

        Returns:
            Database-specific CREATE TABLE AS statement

        Example:
            PostgreSQL: "CREATE TABLE {table_name} AS ({select_query})"
            Oracle: "CREATE TABLE {table_name} AS {select_query}"
        """
        pass

    @abstractmethod
    def get_existing_importances_count_query(self, model_id: int, as_of_date: str, method: str) -> str:
        """Generate database-specific query to count existing individual importances.

        Args:
            model_id: Model ID to check for
            as_of_date: Date to check for (formatted string)
            method: Importance calculation method name

        Returns:
            SQL query string to count matching importance records
        """
        pass

    @abstractmethod
    def delete_individual_importances_query(self, model_id: int, as_of_date: str, method: str) -> str:
        """Generate database-specific query to delete individual importances.

        Args:
            model_id: Model ID to delete records for
            as_of_date: Date to delete records for (formatted string)
            method: Importance calculation method name

        Returns:
            SQL DELETE statement to remove matching importance records
        """
        pass

    @abstractmethod
    def get_subset_table_query(self, as_of_dates: List[str], subset_table_name: str) -> str:
        """Generate database-specific query to retrieve subset table data.

        Args:
            as_of_dates: List of formatted date strings to query for
            subset_table_name: Name of the subset table to query

        Returns:
            SQL query string to retrieve entity_id, as_of_date, active from subset table
        """
        pass