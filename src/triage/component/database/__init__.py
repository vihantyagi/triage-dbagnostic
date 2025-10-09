from .base import DatabaseAdapter
from .schema_factory import set_schema_factory, get_schema_factory, json_column, array_column

# Import adapters only when accessed to avoid circular imports
def get_postgresql_adapter():
    from .postgresql import PostgreSQLAdapter
    return PostgreSQLAdapter

def get_oracle_adapter():
    from .oracle import OracleAdapter
    return OracleAdapter

def __getattr__(name):
    if name == 'PostgreSQLAdapter':
        return get_postgresql_adapter()
    elif name == 'OracleAdapter':
        return get_oracle_adapter()
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

__all__ = ['DatabaseAdapter', 'PostgreSQLAdapter', 'OracleAdapter',
           'set_schema_factory', 'get_schema_factory', 'json_column', 'array_column']