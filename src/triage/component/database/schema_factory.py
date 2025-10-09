"""Factory for creating database-agnostic schema components."""

from sqlalchemy import Column
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import DatabaseAdapter


class SchemaFactory:
    """Factory for creating database-agnostic SQLAlchemy column types.
    
    This factory uses a database adapter to provide the appropriate column types
    for different database systems (PostgreSQL, Oracle, etc.).
    """
    
    def __init__(self, adapter: 'DatabaseAdapter'):
        self.adapter = adapter
    
    def json_column(self, **kwargs):
        """Create a JSON column appropriate for the current database.
        
        Args:
            **kwargs: Additional arguments to pass to Column()
            
        Returns:
            SQLAlchemy Column with database-appropriate JSON type
        """
        return Column(self.adapter.get_json_column_type(), **kwargs)
    
    def array_column(self, item_type, **kwargs):
        """Create an array column appropriate for the current database.
        
        Args:
            item_type: The type of items in the array (e.g., String, Integer)
            **kwargs: Additional arguments to pass to Column()
            
        Returns:
            SQLAlchemy Column with database-appropriate array type
        """
        return Column(self.adapter.get_array_column_type(item_type), **kwargs)


# Global factory instance - will be set when adapter is configured
_schema_factory = None


def set_schema_factory(adapter: 'DatabaseAdapter' = None, db_url: str = None) -> None:
    """Set the global schema factory with the given adapter.

    Args:
        adapter: Database adapter to use for schema creation. If None, will auto-detect from db_url
        db_url: Database URL to auto-detect adapter from. Only used if adapter is None
    """
    global _schema_factory

    if adapter is None:
        adapter = _auto_detect_adapter(db_url)

    _schema_factory = SchemaFactory(adapter)


def _auto_detect_adapter(db_url: str = None) -> 'DatabaseAdapter':
    """Auto-detect the appropriate database adapter.

    Args:
        db_url: Database URL to detect from. If None, tries to detect from environment

    Returns:
        Appropriate DatabaseAdapter instance
    """
    # Import adapters here to avoid circular imports
    from . import get_postgresql_adapter, get_oracle_adapter

    # If URL provided, detect from URL
    if db_url:
        if 'postgresql' in db_url or 'postgres' in db_url:
            return get_postgresql_adapter()(db_engine=None)
        elif 'oracle' in db_url:
            return get_oracle_adapter()(db_engine=None)

    # Fallback: detect from available drivers
    try:
        import psycopg2
        return get_postgresql_adapter()(db_engine=None)
    except ImportError:
        pass

    try:
        import oracledb
        return get_oracle_adapter()(db_engine=None)
    except ImportError:
        pass

    # Final fallback: use PostgreSQL as default
    return get_postgresql_adapter()(db_engine=None)


def get_schema_factory() -> SchemaFactory:
    """Get the current schema factory.

    Returns:
        The current schema factory instance

    Note:
        If no schema factory has been configured, automatically sets up a default one
    """
    global _schema_factory

    if _schema_factory is None:
        # Auto-configure a default factory
        set_schema_factory()

    return _schema_factory


def json_column(**kwargs):
    """Convenience function to create a JSON column.
    
    Args:
        **kwargs: Additional arguments to pass to Column()
        
    Returns:
        SQLAlchemy Column with database-appropriate JSON type
    """
    return get_schema_factory().json_column(**kwargs)


def array_column(item_type, **kwargs):
    """Convenience function to create an array column.
    
    Args:
        item_type: The type of items in the array
        **kwargs: Additional arguments to pass to Column()
        
    Returns:
        SQLAlchemy Column with database-appropriate array type
    """
    return get_schema_factory().array_column(item_type, **kwargs)