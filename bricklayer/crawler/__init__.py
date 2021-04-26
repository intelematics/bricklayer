"""
    delta_tables crawlers
    two functions supported
    - restore delta tables from delta_log location
    - update existing delta table from delta_log location
    Usage:
    ```
    from bricklayer.crawler import Crawler
    # restore tables
    c = Crawler()
    c.restore_delta_tables(
        working_dir='/mnt/dev_data_asset/delta',
        tables= ['flow.feature_flow_intelemap_link_version_1']
    )
    # relocate tables
    c.relocate_delta_tables(
        working_dir='/mnt/dev_data_asset/delta',
        tables= ['flow.feature_flow_intelemap_link_version_1']
    )
    ```
"""

from pathlib import Path
import typing
import logging
from pyspark.sql import SparkSession

class Crawler():

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()

    def restore_delta_tables(self, working_dir: str, tables: typing.Iterable[str] = None) -> None:
        """recreate delta tables for all delta_log/ path which was found in the target directory
        Args:
            working_dir (str): working directory in which save the delta table data
                e.g. '/mnt/dev_data_asset/delta'
            tables (typing.Iterable[str], optional): tables(table_sql_name) to be restored
                e.g. flow.feature_flow_intelemap_link_version_1
                Defaults to None.
        """
        logging.info(f'Input `working_dir`: {working_dir}')
        working_dir = working_dir.strip('/')
        working_dir_abs_path = Path(f'/dbfs/{working_dir}')
        logging.info(f'Absolute path of the working_dir: {str(working_dir_abs_path)}')

        if tables:
            for t in tables:
                table_name, version = t.split('_version_')
                table_location_path = f'/{working_dir}/{table_name}/version={version}'
                self._create_delta_table(t, table_location_path)
        else:
            path_list = working_dir_abs_path.glob('*.*/version=*/_delta_log/')
            if not path_list:
                logging.warn(f'Cannot find any qualified path in {working_dir_abs_path}')
                return
            for p in path_list:
                table_name = p.relative_to(working_dir_abs_path).parts[0]
                version = p.relative_to(working_dir_abs_path).parts[1].split('version=')[1]
                table_sql_name = f'{table_name}_version_{version}'
                table_location_path = f'/{working_dir}/{table_name}/version={version}'
                self._create_delta_table(table_sql_name, table_location_path)

    def _create_delta_table(self, table_sql_name: str, table_location_path: str) -> None:
        sql = f"""
        CREATE TABLE {table_sql_name}
        USING DELTA 
        LOCATION '{table_location_path}'
        """

        if Path(f'/dbfs{table_location_path}/_delta_log').exists():
            self.spark.sql(sql)
            logging.info(f'Restoring delta table for {table_sql_name} at {table_location_path} SUCCESS')
        else:
            logging.error(f'`/dbfs{table_location_path}/_delta_log` doesn\'t exist')
            logging.error(f'Restoring delta table for {table_sql_name} at {table_location_path} FAILED')

    def relocate_delta_tables(self, working_dir: str, tables: typing.Iterable[str] = None) -> None:
        """update the location url for all tables which could be retrieved by Databricks sql
        Args:
            working_dir (str): working directory in which save the delta table data
                e.g. '/mnt/dev_data_asset/delta'
            tables (typing.Iterable[str], optional): tables to be relocated
                e.g. flow.feature_flow_intelemap_link_version_1
                Defaults to None.
        """

        logging.info(f'Input `working_dir`: {working_dir}')
        working_dir = working_dir.strip('/')

        if not tables:
            tables = self._get_all_tables()

        for t in tables:
            table_name, version = t.split('_version_')
            table_new_location_path = f'/{working_dir}/{table_name}/version={version}'
            self._update_delta_table_location(t, table_new_location_path)

    def _update_delta_table_location(self, table_sql_name: str, table_new_location_path: str) -> None:
        sql = f"""
        ALTER TABLE {table_sql_name}
        SET LOCATION '{table_new_location_path}'
        """

        if Path(f'/dbfs{table_new_location_path}/_delta_log').exists():
            self.spark.sql(sql)
            logging.info(f'Relocating delta table for {table_sql_name} to {table_new_location_path} SUCCESS')
        else:
            logging.error(f'`/dbfs{table_new_location_path}/_delta_log` doesn\'t exist')
            logging.error(f'Relocating delta table for {table_sql_name} to {table_new_location_path} FAILED')

    def _get_all_tables(self):
        return [f'{db}.{table}'
            for db in list(self.spark.sql("SHOW DATABASES").toPandas().databaseName)
            for table in list(self.spark.sql(f"SHOW TABLES FROM {db} LIKE '*_version_*'").toPandas().tableName)
        ]
