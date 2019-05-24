from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from ibm_cloud_functions_airflow_plugin.hooks.ibm_cf_hook import IbmCloudFunctionsHook

import abc
import re

class IbmCloudFunctionsOperator(BaseOperator):
    ui_color = '#c4daff'

    @apply_defaults
    def __init__(
            self,
            map_function,
            reduce_function=None,
            op_args=None,
            *args, **kwargs):
        
        if (not callable(map_function)):
            raise AirflowException("Map function must be a python callable")
        
        if (reduce_function is not None and not callable(reduce_function)):
            raise AirflowException("Reduce function must be a python callable")
        
        self.op_args = op_args
        
        self.map_function = map_function
        self.reduce_function = reduce_function
            
        super().__init__(*args, **kwargs)

    def execute(self, context):
        self.op_args = self._parameter_setup(context)      
        self.log.info("Parameters: {}".format(self.op_args))
        self.hook = IbmCloudFunctionsHook()
        self.hook.get_conn()
        return_value = self.execute_callable()
        self.log.info("Done. Returned value was: %s", return_value)
        context['ti'].xcom_push(key=self.task_id, value=return_value)
        return return_value
    
    @abc.abstractclassmethod
    def execute_callable(self):
        pass
        
    def _parameter_setup(self, context):
        if self.op_args is None:
            return {}

        args = []

        for key,value in self.op_args.items():
            if (isinstance(value, str) and re.search("^TASK:*", value) is not None):
                self.op_args[key] = context['ti'].xcom_pull(key=re.sub("TASK:*", "", value))
            if value == 'iterdata':
                iter_key = key
            print(key, value)

        if 'iterdata' in self.op_args:
            iterdata = self.op_args['iterdata']
            del self.op_args['iterdata']
        
            for data in iterdata:
                param = self.op_args.copy()
                param[iter_key] = data
                args.append(param)
        else:
            args = self.op_args.copy()
        
        return args


class IbmCloudFunctionsBasicOperator(IbmCloudFunctionsOperator):
    def __init__(
            self,
            function,
            op_args=None,
            *args, **kwargs):
        
        super().__init__(
            map_function=function,
            op_args=op_args,
            *args, **kwargs)
    
    def execute_callable(self):
        return self.hook.invoke_call_async(self.map_function, self.op_args)

class IbmCloudFunctionsMapOperator(IbmCloudFunctionsOperator):
    def __init__(
            self,
            map_function,
            op_args=None,
            *args, **kwargs):
        
        super().__init__(
            map_function=map_function,
            op_args=op_args,
            *args, **kwargs)
    
    def execute_callable(self):
        return self.hook.invoke_map(self.map_function, self.op_args)

class IbmCloudFunctionsMapReduceOperator(IbmCloudFunctionsOperator):
    def __init__(
            self,
            map_function,
            reduce_function,
            op_args=None,
            *args, **kwargs):

        super().__init__(
            map_function=map_function, 
            reduce_function=reduce_function,
            op_args=op_args,
            *args, **kwargs)
    
    def execute_callable(self):
        return self.hook.invoke_map_reduce(self.map_function, self.op_args, self.reduce_function)