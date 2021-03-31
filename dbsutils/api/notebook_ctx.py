import json

class NotebookContext(object):

    def __init__(self):
        self._ctx = json.loads(
                            dbutils
                            .notebook
                            .entry_point
                            .getDbutils()
                            .notebook()
                            .getContext()
                            .toJson()
                            )
    
    @property
    def run_id(self):
        return self._ctx['currentRunId']
    
    @property
    def notebook_path(self):
        return self._ctx['extraContext']['notebook_path']
    
    @property
    def browser_host_name(self):
        return self._ctx['tags']['browserHostName']

def current_ctx():
    return NotebookContext()
