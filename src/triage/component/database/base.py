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
    def get_model_group_id(self, model_type: str, hyperparameters: Dict[str, Any], 
                          feature_list: List[str], model_config: Dict[str, Any]) -> int:
        """Get or create model group ID.
        
        This replaces the PostgreSQL stored procedure with Python logic.
        
        Args:
            model_type: The type of model
            hyperparameters: Model hyperparameters
            feature_list: List of features used
            model_config: Model configuration
            
        Returns:
            Model group ID
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