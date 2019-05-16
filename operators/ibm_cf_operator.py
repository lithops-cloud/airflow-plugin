from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from ibm_cloud_functions_airflow_plugin.hooks.ibm_cf_hook import IbmCloudFunctionsHook

import pywren_ibm_cloud
import abc

class IbmCloudFunctionsOperator(BaseOperator):
    ui_color = '#c4daff'

    @apply_defaults
    def __init__(
            self,
            module_path,
            map_function_name,
            reduce_function_name=None,
            op_args=None,
            op_kwargs=None,
            args_from_task=None,
            *args, **kwargs):

        if (op_args and op_kwargs):
            raise AirflowException("Args and kwargs not permitted")
        
        self.args = op_args if not op_kwargs else op_kwargs
        self.args_from_task = args_from_task
        
        file_functions = {}
        function_file = open(module_path).read()
        exec(function_file, globals(), file_functions)
        self.map_function = file_functions[map_function_name]
        self.reduce_function = file_functions[reduce_function_name] if reduce_function_name else None
            
        super().__init__(*args, **kwargs)

    def execute(self, context):

        if (self.args_from_task):
            aux_args = []
            for arg in self.args:
                dict_arg = {}
                for key in self.args_from_task:
                    dict_arg[key] = arg if self.args_from_task[key] == 'op_args' else context['ti'].xcom_pull(key=self.args_from_task[key])
                aux_args.append(dict_arg)
            self.args = aux_args
        
        print(self.args)
        self.executor = IbmCloudFunctionsHook().get_conn()
        return_value = self.execute_callable()
        self.log.info("Done. Returned value was: %s", return_value)
        context['ti'].xcom_push(key=self.task_id, value=return_value)
        return return_value
    
    @abc.abstractclassmethod
    def execute_callable(self):
        pass

class IbmCloudFunctionsBasicOperator(IbmCloudFunctionsOperator):
    def __init__(
            self,
            module_path,
            function_name,
            op_args=None,
            op_kwargs=None,
            args_from_task=None,
            *args, **kwargs):
        
        super().__init__(
            module_path=module_path, 
            map_function_name=function_name,
            op_args=op_args,
            op_kwargs=op_kwargs,
            args_from_task=args_from_task, 
            *args, **kwargs)
    
    def execute_callable(self):
        self.executor.call_async(self.map_function, self.args)
        return self.executor.get_result()

class IbmCloudFunctionsMapOperator(IbmCloudFunctionsOperator):
    def __init__(
            self,
            module_path,
            function_name,
            op_args=None,
            op_kwargs=None,
            args_from_task=None,
            *args, **kwargs):
        
        try:
            iter(op_args)
        except TypeError:
            raise AirflowException("Args must be iterable")

        super().__init__(
            module_path=module_path, 
            map_function_name=function_name,
            op_args=op_args,
            op_kwargs=op_kwargs,
            args_from_task=args_from_task,
            *args, **kwargs)
    
    def execute_callable(self):
        self.executor.map(self.map_function, self.args)
        return self.executor.get_result()

class IbmCloudFunctionsMapReduceOperator(IbmCloudFunctionsOperator):
    def __init__(
            self,
            module_path,
            map_function_name,
            reduce_function_name,
            op_args=None,
            op_kwargs=None,
            args_from_task=None,
            *args, **kwargs):
        
        try:
            iter(op_args)
        except TypeError:
            raise AirflowException("Args must be iterable")

        super().__init__(
            module_path=module_path, 
            map_function_name=map_function_name, 
            reduce_function_name=reduce_function_name,
            op_args=op_args,
            op_kwargs=op_kwargs,
            args_from_task=args_from_task,
            *args, **kwargs)
    
    def execute_callable(self):
        self.executor.map_reduce(self.map_function, self.args, self.reduce_function)
        return self.executor.get_result()