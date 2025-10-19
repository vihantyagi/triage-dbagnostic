import verboselogs, logging
logger = verboselogs.VerboseLogger(__name__)

from sqlalchemy import text
from triage.component.architect.utils import str_in_sql
from triage.util.structs import FeatureNameList


class FeatureDictionaryCreator:
    def __init__(self, features_schema_name, db_engine, db_adapter=None):
        self.features_schema_name = features_schema_name
        self.db_engine = db_engine
        self.db_adapter = db_adapter

    def _tables_to_include(self, feature_table_names):
        return [
            feature_table
            for feature_table in feature_table_names
            if "aggregation_imputed" in feature_table
        ]

    def feature_dictionary(self, feature_table_names, index_column_lookup):
        """ Create a dictionary of feature names, where keys are feature tables
        and values are lists of feature names.

        :return: feature_dictionary
        :rtype: dict
        """
        feature_dictionary = {}

        # iterate! store each table name + features names as key-value pair
        for feature_table_name in self._tables_to_include(feature_table_names):
            with self.db_engine.begin() as conn:
                result = conn.execute(text(
                    self._build_feature_names_query(
                        feature_table_name, index_column_lookup[feature_table_name]
                    )
                ))
                feature_names = [row[0] for row in result]
            feature_dictionary[feature_table_name] = FeatureNameList(feature_names)
        logger.spam(f"Feature dictionary built: {feature_dictionary}")
        return feature_dictionary

    def _build_feature_names_query(self, table_name, index_columns):
        """ For a given feature table, get the names of the feature columns.

        :param table_name: name of the feature table
        :type table_name: str

        :return: names of the feature columns in given table
        :rtype: list
        """
        # Use database adapter for database-specific column query
        if self.db_adapter:
            feature_names_query = self.db_adapter.get_table_columns_query(
                table_name, self.features_schema_name, index_columns
            )
        else:
            # Fallback to PostgreSQL-specific query for backward compatibility
            feature_names_query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{table_name}' AND
                      table_schema = '{self.features_schema_name}' AND
                      column_name NOT IN ({str_in_sql(index_columns)})
            """

        logger.spam(
            f"Extracting all possible feature names for table {table_name} with query {feature_names_query}"
        )

        return feature_names_query
