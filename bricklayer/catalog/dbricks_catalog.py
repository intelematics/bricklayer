"""Module to access the databricks catalog"""

from typing import Iterator
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession

class DbricksTable:
    """A table found in the databricks catalog"""
    def __init__(self, database_name, table_name , table_version, spark):
        self.database_name = database_name
        self.table_name = table_name
        self.table_version = table_version
        self.spark = spark
        try:
            self.details = spark.sql(f"DESCRIBE DETAIL {self.sql_name}").first()
        except AnalysisException as _e:
            self.details = None

    @property
    def table_format(self):
        if self.details is None:
            return None
        return self.details.format

    @property
    def table_location(self):
        if self.details is None:
            return None
        return self.details.location

    @property
    def is_view(self):
        return self.details is None

    @property
    def sql_name(self):
        """Name of the table as used in SQL"""
        return f"{self.database_name}.{self.table_name}_version_{self.table_version}"

class DbricksDatabase:
    """Database found in the databricks catalog"""
    def __init__(self, name, spark):
        self.name = name
        self.spark = spark

    def get_tables(self) -> Iterator[DbricksTable]:
        """Generator to iterate over the databricks tables"""
        for table_row in self.spark.sql(f'SHOW TABLES IN {self.name}').collect():
            yield DbricksTable(
                self.name,
                table_row.tableName.split('_version_')[0],
                table_row.tableName.split('_version_')[-1],
                spark=self.spark
            )

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
