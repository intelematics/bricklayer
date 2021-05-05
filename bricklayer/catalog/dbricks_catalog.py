"""Module to access the databricks catalog"""

from typing import Iterator
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession

class DbricksTable:
    """A table found in the databricks catalog"""
    def __init__(self, database_name, table_name , table_version, info, spark):
        self.database_name = database_name
        self.table_name = table_name
        self.table_version = table_version
        self.spark = spark
        self.info = info

    @property
    def table_created_time(self):
        return self.info.get('Created Time')

    @property
    def table_type(self):
        return self.info.get('Type')

    @property
    def table_provider(self):
        return self.info.get('Provider')

    @property
    def table_location(self):
        return self.info.get('Location')

    @property
    def is_view(self):
        return self.table_type == 'VIEW'

    @property
    def sql_name(self):
        """Name of the table as used in SQL"""
        return f"{self.database_name}.{self.table_name}_version_{self.table_version}"

class DbricksDatabase:
    """Database found in the databricks catalog"""
    
    RELEVANT_TABLE_INFO = {'Created Time', 'Type', 'Provider', 'Location'}

    def __init__(self, name, spark):
        self.name = name
        self.spark = spark

    def get_tables(self) -> Iterator[DbricksTable]:
        """Generator to iterate over the databricks tables"""
        for table_row in self.spark.sql(f"SHOW TABLE EXTENDED IN {self.name} LIKE '*'").collect():
            info = self._parse_extended_info(table_row.information)
            yield DbricksTable(
                self.name,
                table_row.tableName.split('_version_')[0],
                table_row.tableName.split('_version_')[-1],
                info=info,
                spark=self.spark
            )

    def _parse_extended_info(self, info):
        result = {}
        for line in info.split('\n'):
            line_parts = line.split(':', maxsplit=1)
            if len(line_parts) > 1:
                if line_parts[0] in self.RELEVANT_TABLE_INFO:
                    result[line_parts[0]] = line_parts[1].strip()
        return result


    def __repr__(self):
        return f"{self.__class__.__name__}:{self.name}"

class DbricksCatalog:
    """Databricks catalog"""
    IGNORE_DATABASES = ('tmp', 'default')

    def __init__(self, spark=None):
        if spark is None:
            self.spark = SparkSession.builder.getOrCreate()
        else:
            self.spark = spark

    def get_databases(self) -> Iterator[DbricksDatabase]:
        """Iterator over all the databases in the databricks catalog"""
        for db_row in self.spark.sql('SHOW DATABASES').collect():
            if db_row.databaseName in self.IGNORE_DATABASES:
                continue
            yield DbricksDatabase(db_row.databaseName, spark=self.spark)
