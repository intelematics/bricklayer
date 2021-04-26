import json

def get_dbutils(spark):
    from pyspark.dbutils import DBUtils
    return DBUtils(spark)

def get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()

class NotebookContext():

    def __init__(self):
        dbutils = get_dbutils(get_spark())
        self._context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        self._context_tags = json.loads(self._context.toJson()).get('tags')

    def get_run_id(self):
        """Return the current run id"""
        return self._context.currentRunId().toString()

    def get_api_token(self):
        """Return the token id"""
        return self._context.apiToken().value()

    def get_browser_host_name(self):
        """Return the notebook host name"""
        return self._context_tags.get('browserHostName')

    def get_browser_host_name_url(self):
        """Return the notebook url host name"""
        return f'https://{self.get_browser_host_name()}'

    def get_notebook_path(self):
        return self._context.notebookPath().x()

notebook_context = NotebookContext()

# Set default logging handler to avoid "No handler found" warnings.
import logging
import sys
from logging import NullHandler

logging.getLogger(__name__).addHandler(NullHandler())
logging.basicConfig(
    level='INFO',
    stream=sys.stdout,
    format='[{levelname}] [{asctime}] [{name}] [{module}.{funcName}] {message}',
    style='{'
)
