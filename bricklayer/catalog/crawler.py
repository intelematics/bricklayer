"""
    delta_tables crawlers
    two functions supported
    - restore delta tables from delta_log location
    - update existing delta table from delta_log location
    Usage:
    ```
    from bricklayer.catalog import crawler
    # restore tables
    crawler.restore_delta_tables(
        dbfs_path='/mnt/data_asset/delta',
        table_names= ['flow.feature_flow_intelemap_link_version_1'],
        prefixes=['flow.feature_flow']
    )
    # relocate tables
    crawler.relocate_delta_tables(
        dbfs_path='/mnt/data_asset/delta',
        table_names= ['flow.feature_flow_intelemap_link_version_1'],
        prefixes=['flow.feature_flow']
    )
    ```
"""

import typing
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from . import dbricks_catalog

class Crawler():

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()

    def restore_delta_tables(
        self,
        dbfs_path: str,
        table_names: typing.Iterable[str] = None,
        prefixes: typing.Iterable[str] = None
    ) -> None:
        """recreate delta tables for all delta_log/ path which was found in the target directory
        Args:
            dbfs_path (str): relative path to dbfs/ in which save the delta table data
                e.g. '/mnt/data_asset/delta'
            tables (typing.Iterable[str], optional): tables(table_sql_name) to be restored
                e.g. flow.feature_flow_intelemap_link_version_1
                Defaults to None.
            prefixes (typing.Iterable[str], optional): prefix of tables to be relocated.
                If `talbe_names` and `prefixes` are using at the same time, only `table_names` start with `prefixes` will in
                e.g. 'flow.feature_flow'
                Defaults to None.
        """
        if isinstance(table_names, str):
            table_names = [table_names]

        if isinstance(prefixes, str):
            prefixes = [prefixes]

        logging.info(f'Input `dbfs_path`: {dbfs_path}')
        dbfs_path = dbfs_path.strip('/')
        abs_path = Path(f'/dbfs/{dbfs_path}')
        logging.info(f'Absolute full path of the directory: {str(abs_path)}')

        if not table_names:
            table_names = self._get_all_tables_from_dbfs_path(abs_path)

        if not prefixes:
            logging.debug(f'table_names before filtering: {table_names}')
            table_names = self._filter_tables_by_prefixes(table_names, prefixes)
            logging.debug(f'table_names after filtering: {table_names}')

        if not table_names:
            logging.warn('Cannot find any qualified table to restore')
            return

        success_paths = []
        failure_paths = []
        for t in table_names:
            table_name, version = t.split('_version_')
            table_location_path = f'/{dbfs_path}/{table_name}/version={version}'
            if self._create_delta_table(t, table_location_path):
                success_paths.append(table_location_path)
            else:
                failure_paths.append(table_location_path)
        logging.info(f"Restoring successful: {success_paths}")
        logging.info(f"Restoring failed: {failure_paths}")

    def _create_delta_table(self, table_sql_name: str, table_location_path: str) -> bool:
        sql = f"""
        CREATE TABLE {table_sql_name}
        USING DELTA 
        LOCATION '{table_location_path}'
        """

        if Path(f'/dbfs{table_location_path}/_delta_log').exists():
            self.spark.sql(sql)
            logging.info(f'Restoring delta table for {table_sql_name} at {table_location_path} SUCCESS')
            return True
        else:
            logging.debug(f'`/dbfs{table_location_path}/_delta_log` doesn\'t exist')
            logging.debug(f'Restoring delta table for {table_sql_name} at {table_location_path} FAILED')
            return False

    def relocate_delta_tables(
        self,
        dbfs_path: str,
        table_names: typing.Iterable[str] = None,
        prefixes: typing.Iterable[str] = None
    ) -> None:
        """update the location url for all tables which could be retrieved by Databricks sql
        Args:
            dbfs_path (str): working directory in which save the delta table data
                e.g. '/mnt/dev_data_asset/delta'
            table_names (typing.Iterable[str], optional): tables to be relocated
                e.g. 'flow.feature_flow_intelemap_link_version_1'
                Defaults to None.
            prefixes (typing.Iterable[str], optional): prefix of tables to be relocated.
                If `talbe_names` and `prefixes` are using at the same time, only `table_names` start with `prefixes` will in
                e.g. 'flow.feature_flow'
                Defaults to None.
        """
        if isinstance(table_names, str):
            table_names = [table_names]

        if isinstance(prefixes, str):
            prefixes = [prefixes]

        logging.info(f'Input `dbfs_path`: {dbfs_path}')
        dbfs_path = dbfs_path.strip('/')

        if not table_names:
            table_names = self._get_all_tables_from_dbs_catalog()

        if not prefixes:
            logging.debug(f'table_names before filtering: {table_names}')
            table_names = self._filter_tables_by_prefixes(table_names, prefixes)
            logging.debug(f'table_names after filtering: {table_names}')

        if not table_names:
            logging.warn('Cannot find any qualified table to relocate')
            return

        success_tables = []
        failure_tables = []
        for t in table_names:
            table_name, version = t.split('_version_')
            table_new_location_path = f'/{dbfs_path}/{table_name}/version={version}'
            if self._update_delta_table_location(t, table_new_location_path):
                success_tables.append(t)
            else:
                failure_tables.append(t)
        logging.info(f"Relocating successful: {success_tables}")
        logging.info(f"Relocating failed: {failure_tables}")

    def _update_delta_table_location(self, table_sql_name: str, table_new_location_path: str) -> bool:
        sql = f"""
        ALTER TABLE {table_sql_name}
        SET LOCATION '{table_new_location_path}'
        """

        if Path(f'/dbfs{table_new_location_path}/_delta_log').exists():
            self.spark.sql(sql)
            logging.info(f'Relocating delta table for {table_sql_name} to {table_new_location_path} SUCCESS')
            return True
        else:
            logging.debug(f'`/dbfs{table_new_location_path}/_delta_log` doesn\'t exist')
            logging.debug(f'Relocating delta table for {table_sql_name} to {table_new_location_path} FAILED')
            return False

    def _get_all_tables_from_dbs_catalog(self):
        return [
            table.sql_name
            for db in dbricks_catalog.DbricksCatalog().get_databases()
            for table in db.get_tables()
            if not table.is_view
        ]

    def _get_all_tables_from_dbfs_path(self, abs_path: str):
        return [
            f"{p.relative_to(abs_path).parts[0]}_version_{p.relative_to(abs_path).parts[1].split('version=')[1]}"
            for p in abs_path.glob('*.*/version=*/_delta_log/')
        ]

    def _filter_tables_by_prefixes(
        self,
        table_names: typing.Iterable[str],
        prefixes: typing.Iterable[str]
    ) -> typing.Iterable[str]:
        return [
            table_name
            for prefix in prefixes
            for table_name in table_names
            if table_name.startswith(prefix)
        ]

def restore_delta_tables(
        dbfs_path: str,
        table_names: typing.Iterable[str] = None,
        prefixes: typing.Iterable[str] = None
    ):
    Crawler().restore_delta_tables(dbfs_path, table_names, prefixes)

def relocate_delta_tables(
        dbfs_path: str,
        table_names: typing.Iterable[str] = None,
        prefixes: typing.Iterable[str] = None
    ):
    Crawler().relocate_delta_tables(dbfs_path, table_names, prefixes)
