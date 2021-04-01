"""
    Wrappers for databricks_cli api and bring some sanity back with namespaces.
    Usage:
    ```
    import DBApi
    # export notebook
    db = DBApi()
    db.export_notebook(
        source_path='/Repos/deploy/dac-dbs-volume-projection-validation/02_validation_notebooks/90_run_vp_6', 
       target_path= '/dbfs/mnt/external/tmp/90_run_vp_6'
    )
    ```
"""

import pathlib

from databricks_cli.workspace.api import WorkspaceApi 
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk import ApiClient


class DBSApi(object):

    def __init__(
        self,
        token,
        host='https://intelematics-dac-dev.cloud.databricks.com',
        apiVersion='2.0',
    ):
       
        self._client = client=ApiClient(
                            host=host,
                            apiVersion=apiVersion,
                            token=token
                            )

    def export_notebook(self, source_path, target_path, fmt='DBC', is_overwrite=False):
        (
            WorkspaceApi(self._client)
            .export_workspace(
                source_path, 
                target_path,
                fmt,
                is_overwrite
            )
        )

    def import_notebook(self, source_path, target_path, language='PYTHON', fmt='DBC', is_overwrite=False):
        (
            WorkspaceApi(self._client)
            .import_workspace(
                source_path,
                target_path,
                language,
                fmt,
                is_overwrite
            )
        )

    def backup_notebook(self, source_path, target_path, tmp_dir='/dbfs/mnt/external/tmp/'):
        notebook_name = pathlib.Path(source_path).parts[-1]
        intermediate_location = pathlib.Path(tmp_dir).joinpath(notebook_name).as_posix()
        self.export_notebook(source_path, intermediate_location)
        self.import_notebook(intermediate_location, target_path)

