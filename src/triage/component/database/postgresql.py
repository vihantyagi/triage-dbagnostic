import json
from typing import Dict, List, Any
from sqlalchemy import String, text
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import Session

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
    
    def get_model_group_id(self, model_type: str, hyperparameters: Dict[str, Any], 
                          feature_list: List[str], model_config: Dict[str, Any]) -> int:
        """Get or create model group ID using PostgreSQL stored procedure.
        
        This uses the existing PostgreSQL stored procedure for now.
        Later we'll replace this with pure Python logic.
        """
        # Sort feature list for consistent comparison
        sorted_features = sorted(feature_list)
        
        # Use the existing stored procedure
        query = text("""
            SELECT public.get_model_group_id(
                :model_type,
                :hyperparameters::jsonb,
                :feature_list,
                :model_config::jsonb
            )
        """)
        
        result = self.db_engine.execute(
            query,
            model_type=model_type,
            hyperparameters=json.dumps(hyperparameters),
            feature_list=sorted_features,
            model_config=json.dumps(model_config)
        )
        
        return result.scalar()
    
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