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
import json

import requests
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk import ApiClient
from databricks_cli.sdk import JobsService
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.runs.api import RunsApi

from .. import get_notebook_context

class DBJobRun(object):
    '''Wrapper for a Job Run'''

    def __init__(self, job, run_id, client):
        self.job = job
        self.run_id = run_id
        self._client = client

    @property
    def data(self):
        '''Return the data from the raw API call'''
        return RunsApi(self._client).get_run(self.run_id)

    @property
    def result_state(self):
        return self.data['state'].get('result_state')

    @property
    def life_cycle_state(self):
        """Can be PENDING, RUNNING or TERMINATED"""
        return self.data['state'].get('life_cycle_state')

    @property
    def state_message(self):
        return self.data['state'].get('state_message')

    @property
    def run_page_url(self):
        '''Return the URL of the run in the datbricks API'''
        return self.data['run_page_url']

    @property
    def attempt_number(self):
        return self.data['attempt_number']

    def get_run_output(self):
        '''Return the output of the job as defined in the
        job notebook with a call to `dbutils.notebook.exit` function'''
        data = RunsApi(self._client).get_run_output(self.run_id)
        return data.get('notebook_output')

class DBJob(object):
    '''Wrapper for a Job Run'''
    def __init__(self, job_id, client):
        self.job_id = job_id
        self._client = client
        self.runs = []

    @property
    def data(self):
        '''Return the data from the raw JobApi call'''
        return JobsApi(self._client).get_job(self.job_id)

    @property
    def name(self):
        return self.data['settings']['name']

    @property
    def notebook_task(self):
        return self.data['settings']['notebook_task']

    def __repr__(self):
        return f'{self.__class__.__name__}:{self.job_id}'

    @property
    def existing_cluster_id(self):
        return self.data['settings']['existing_cluster_id']

    def run_now(self, jar_params=None, notebook_params=None, python_params=None,
                    spark_submit_params=None):
        """Run this job.
        :param jar_params: list of jars to be included
        :param notebook_params: map (dict) with the params to be passed to the job
        :param python_params: To pa passed to the notebook as if they were command-line parameters
        :param spark_submit_params: A list of parameters for jobs with spark submit task as command-line
                            parameters.
        """
        data = JobsApi(self._client).run_now(
            self.job_id,
            jar_params=jar_params,
            notebook_params=notebook_params,
            python_params=python_params,
            spark_submit_params=spark_submit_params
        )
        run = DBJobRun(self, data['run_id'], self._client)
        self.runs.append(run)
        return run

    def stop(self):
        "Stop this job."
        for run in self.runs:
            JobsService(self._client).client.perform_query(
                'POST', '/jobs/runs/cancel', data={
                    "run_id": run.run_id
                }
            )


class DBSApi(object):

    def __init__(
        self,
        token=None,
        host=None,
        api_version='2.0',
    ):
        if token is None:
            token = get_notebook_context().get_api_token()

        if host is None:
            host = get_notebook_context().get_browser_host_name_url()

        self._client = ApiClient(
                            host=host,
                            api_version=api_version,
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

    def backup_notebook(self, source_path, target_path, tmp_dir, fmt="DBC"):
        "Backup a notebook to another place in the workspace"
        tmp_name = f'backup_{random.randint(0,1000)}'
        intermediate_location = pathlib.Path(tmp_dir).joinpath(tmp_name)
        self.export_notebook(source_path, intermediate_location.as_posix(), fmt)
        try:
            self.import_notebook(intermediate_location, target_path, fmt)
        finally:
            intermediate_location.unlink()

    def export_current_notebook_run(self, target_path, tmp_dir, fmt="DBC"):
        """Save the current notebook to a given location in the required format (default DBC)
        and preserving the path and timestamp.
        Formats allowed:
            SOURCE : The notebook will be imported/exported as source code.
            HTML   : The notebook will be imported/exported as an HTML file.
            JUPYTER: The notebook will be imported/exported as a Jupyter/IPython Notebook file.
            DBC	   : The notebook will be imported/exported as Databricks archive format.
        """
        current_path = get_notebook_context().get_notebook_path()
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        target_path = (
                pathlib.Path(target_path)
                .joinpath(current_path[1:])
                .joinpath(timestamp)
        )
        try:
            self.backup_notebook(current_path, target_path.as_posix(), tmp_dir, fmt)
        except requests.exceptions.HTTPError as _e:
            error_code = _e.response.json()['error_code']
            if error_code == 'RESOURCE_DOES_NOT_EXIST':
                self.mkdir(target_path.parent.as_posix())
                self.backup_notebook(current_path, target_path.as_posix(), fmt)
            else:
                raise

    def create_job(self, notebook_path, job_name=None, cluster_name=None,
            cluster_id=None, notifications_email=None):
        """Create a databricks job.
        :param notebook_path: The path of the notebook to be run in the job, can be relative
        :param job_name: Name of the job to be run, if missing it will use the notebook_path
        :param cluster_name: If provided the job will run in the cluster with this name
        :param cluster_id: If provided the job will run in the cluster with this id (should not
                            be provided at the same time with cluster_name)
        :param notifications_email: If provided notifications on success or failure on the job run
                            will be sent to this email address.

        Examples
        --------
        ```
        job = DBSApi().create_job('./dummy_job')
        job.run_now()
        #
        job = DBSApi().create_job('./dummy_job',cluster_name='Shared Writer')
        run = job.run_now(notebook_params={'PARAM':'PARAM_VALUE'})
        #
        # Example on how to run jobs with a max number of concurrent runs
        # this can help when we have capacity limits in cpu in the infrastructure side
        import time
        NUM_JOBS_TO_RUN = 6
        MAX_CONCURRENT_JOBS = 3
        jobs_to_run = [
            DBSApi().create_job('./dummy_job') for x in range(NUM_JOBS_TO_RUN)
        ]
        runs = []
        while True:
            running_runs = list(filter(lambda r:r.life_cycle_state !='TERMINATED', runs))
            print(f'running runs:{len(running_runs)}')
            if len(running_runs) < MAX_CONCURRENT_JOBS:
                if not jobs_to_run:
                    break
                job_to_run = jobs_to_run.pop()
                new_run = job_to_run.run_now()
                runs.append(new_run)
            else:
                time.sleep(2)
        ```
        """
        if cluster_name:
            assert cluster_id is None
            _cluster_id = ClusterApi(self._client).get_cluster_id_for_name(cluster_name)
        elif cluster_id:
            _cluster_id = cluster_id
        else:
            _cluster_id = get_notebook_context().get_notebook_cluster_id()

        if job_name:
            _job_name = job_name
        else:
            _job_name = notebook_path


        if not pathlib.Path(notebook_path).is_absolute():
            notebook_path = (
                pathlib
                .Path(get_notebook_context().get_notebook_path())
                .parent
                .joinpath(notebook_path)
                .as_posix()
            )

        _json = (
                    {
                    "name": _job_name,
                    "existing_cluster_id": _cluster_id,
                    "notebook_task": {
                        "notebook_path": notebook_path
                    },
                    "email_notifications": {
                        "on_success": [
                        notifications_email
                        ],
                        "on_failure": [
                        notifications_email
                        ]
                    }
                }
            )
        jobdata = JobsApi(self._client).create_job(_json)
        return DBJob(
            jobdata['job_id'],
            self._client
        )

    def list_jobs(self, job_name='', job_id=''):
        """List all jobs with job name or job id
        """
        jobs = []
        _jobs = JobsApi(self._client).list_jobs()['jobs']

        if job_name:
            result = list(
                filter(
                    lambda job:
                        job_name in job['settings']['name'],
                        _jobs
                ))
        elif job_id:
            result = list(
                filter(
                    lambda job:
                        job_id == job['job_id'],
                        _jobs
                ))
        else:
            result = _jobs

        for jobdata in result:
            job = DBJob(
                jobdata['job_id'],
                self._client
            )
            jobs.append(job)

        return jobs

    def delete_job(self, job_id):
        """delete the created job based on job_id
        """
        if job_id is not None:
            JobsApi(self._client).delete_job(job_id)
        