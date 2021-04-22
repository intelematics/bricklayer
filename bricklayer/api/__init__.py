"""
    Wrappers for databricks_cli api and bring some sanity back with namespaces.
    Usage:
    ```
    import DBSApi
    # export notebook
    db = DBSApi()
    db.export_notebook(
        source_path='/Repos/deploy/dac-dbs-volume-projection-validation/02_validation_notebooks/90_run_vp_6',
        target_path= '/dbfs/mnt/external/tmp/90_run_vp_6'
    )
    # To save the current notebook to the runs folder
    db.export_current_notebook_run()
    ```
"""

import pathlib
import random
import datetime

import requests
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk import ApiClient

from .. import notebook_context


class DBSApi(object):

    def __init__(
        self,
        token=None,
        host=None,
        apiVersion='2.0',
    ):
        if token is None:
            token = notebook_context.get_api_token()
        
        if host is None:
            host = notebook_context.get_browser_host_name_url()

        self._client = ApiClient(
                            host=host,
                            apiVersion=apiVersion,
                            token=token
                            )

    def export_notebook(self, source_path, target_path, fmt='DBC', is_overwrite=False):
        "Export a notebook to a local file"
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
        "Import a notebook from a local file"
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

    def mkdir(self, dir_path):
        "Create a dir in the workspace"
        (
            WorkspaceApi(self._client)
            .mkdirs(
                dir_path
            )
        )

    def backup_notebook(self, source_path, target_path, tmp_dir='/dbfs/mnt/external/tmp/'):
        "Backup a notebook to another place in the workspace"
        tmp_name = f'backup_{random.randint(0,1000)}'
        intermediate_location = pathlib.Path(tmp_dir).joinpath(tmp_name)
        self.export_notebook(source_path, intermediate_location.as_posix())
        try:
            self.import_notebook(intermediate_location, target_path)
        finally:
            intermediate_location.unlink()

    def export_current_notebook_run(self, runs_dir='/Shared/runs/'):
        """Save the current notebook to a given location preserving
        the path and timestamp"""
        current_path = notebook_context.get_notebook_path()
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        target_path = (
                pathlib.Path(runs_dir)
                .joinpath(current_path[1:])
                .joinpath(timestamp)
        )
        try:
            self.backup_notebook(current_path, target_path.as_posix())
        except requests.exceptions.HTTPError as _e:
            error_code = _e.response.json()['error_code']
            if error_code == 'RESOURCE_DOES_NOT_EXIST':
                self.mkdir(target_path.parent.as_posix())
                self.backup_notebook(current_path, target_path.as_posix())
            else:
                raise
        


