from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from ibm_cloud_functions_airflow_plugin.hooks.ibm_cf_hook import IbmCloudFunctionsHook

import pywren_ibm_cloud

# FIXME : Parallel tasks not working?

class IbmCloudFunctionsOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            function_name,
            function_path,
            op_args=None,
            op_kwargs=None,
            *args, **kwargs):

        self.function_name = function_name
        self.function_path = function_path

        if (op_args and op_kwargs):
            raise AirflowException("Args and kwargs not permitted")
        
        self.args = op_args if not op_kwargs else op_kwargs
        
        # FIXME : Is this a bad approach?
        file_functions = {}
        function_file = open(function_path).read()
        exec(function_file, globals(), file_functions)
        self.function = file_functions[self.function_name]
            
        super().__init__(*args, **kwargs)

    def execute(self, context):
        self.executor = IbmCloudFunctionsHook().get_conn()
        return_value = self.execute_callable()
        self.log.info("Done. Returned value was: %s", return_value)
        return return_value

    def execute_callable(self):
        self.executor.call_async(self.function, self.args)
        return self.executor.get_result()
    
    # TODO : Map & mapreduce operators