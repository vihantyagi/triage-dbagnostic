import json
from typing import Dict, List, Any
from sqlalchemy import String, Text, text
from sqlalchemy.orm import Session

from .base import DatabaseAdapter


class OracleAdapter(DatabaseAdapter):
    """Oracle-specific database adapter for Triage.
    
    Implements the DatabaseAdapter interface using Oracle-specific features
    like CLOB for JSON storage and collections for arrays.
    """
    
    def get_json_column_type(self):
        """Return Oracle JSON column type (CLOB for compatibility)."""
        # Oracle 12c+ has native JSON, but CLOB is more compatible
        return Text  # Maps to CLOB in Oracle
    
    def get_array_column_type(self, item_type):
        """Return Oracle array equivalent (comma-separated string for now)."""
        # Oracle has collections, but for simplicity using comma-separated strings
        # In production, this could use Oracle VARRAY or nested tables
        return Text
    
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
    
    def get_model_group_id(self, model_type: str, hyperparameters: Dict[str, Any], 
                          feature_list: List[str], model_config: Dict[str, Any]) -> int:
        """Get or create model group ID using pure Python logic.
        
        This replaces PostgreSQL stored procedures with database-agnostic Python code.
        """
        # Sort feature list for consistent comparison
        sorted_features = sorted(feature_list)
        feature_list_str = ','.join(sorted_features)
        
        # Serialize JSON data
        hyperparams_json = self.store_json_data(hyperparameters)
        config_json = self.store_json_data(model_config)
        
        with Session(bind=self.db_engine) as session:
            # Check if model group exists
            query = text("""
                SELECT model_group_id 
                FROM triage_metadata.model_groups 
                WHERE model_type = :model_type
                  AND hyperparameters = :hyperparams
                  AND feature_list = :features
                  AND model_config = :config
            """)
            
            result = session.execute(
                query,
                {
                    'model_type': model_type,
                    'hyperparams': hyperparams_json,
                    'features': feature_list_str,
                    'config': config_json
                }
            ).fetchone()
            
            if result:
                return result[0]
            
            # Insert new model group - Oracle-specific implementation
            # First insert the record
            insert_query = text("""
                INSERT INTO triage_metadata.model_groups
                (model_type, hyperparameters, feature_list, model_config)
                VALUES (:model_type, :hyperparams, :features, :config)
            """)

            session.execute(
                insert_query,
                {
                    'model_type': model_type,
                    'hyperparams': hyperparams_json,
                    'features': feature_list_str,
                    'config': config_json
                }
            )

            # Get the ID using Oracle's CURRVAL function on the sequence
            # Assumes there's a sequence named model_groups_seq for the primary key
            id_query = text("""
                SELECT model_groups_seq.CURRVAL FROM DUAL
            """)

            result = session.execute(id_query).fetchone()
            session.commit()

            return result[0] if result else None
    
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
    
    def serialize_array(self, array_data: List[Any]) -> str:
        """Convert Python list to comma-separated string for Oracle."""
        if not array_data:
            return ""
        return ','.join(str(item) for item in sorted(array_data))
    
    def deserialize_array(self, array_data: Any) -> List[Any]:
        """Convert Oracle comma-separated string to Python list."""
        if not array_data or array_data == "":
            return []
        return [item.strip() for item in str(array_data).split(',')]
    
    def format_timestamp_array_query(self, timestamp_strings: List[str]) -> str:
        """Generate Oracle-specific SQL for timestamp array IN clause.

        Oracle doesn't have native array syntax like PostgreSQL, so we use
        a simple comma-separated list in an IN clause, which is more portable.

        Args:
            timestamp_strings: List of timestamp strings

        Returns:
            Oracle SQL fragment using simple IN clause

        Example:
            Input: ['2020-01-01', '2020-02-01']
            Output: "('2020-01-01', '2020-02-01')"
        """
        if not timestamp_strings:
            return "(SELECT NULL FROM DUAL WHERE 1=0)"  # Oracle empty result set

        # Format as comma-separated list with proper quoting
        quoted_timestamps = [f"'{ts}'" for ts in timestamp_strings]
        return f"({', '.join(quoted_timestamps)})"

    def setup_stored_procedures(self) -> None:
        """Oracle doesn't need stored procedures - using Python logic instead."""
        # Oracle adapter uses pure Python business logic
        # No stored procedures needed
        pass