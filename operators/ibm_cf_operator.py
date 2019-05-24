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
        """
        Initializes IBM Cloud Functions Operator.
        :param map_function : Map function callable.
        :param reduce_function : Reduce function callable.
        :param op_args : Dictionary where the key is the function's 
        parameter name and value the parameter value.
        """
        
        # Check if map_function and reduce_functions are callables
        if (not callable(map_function)):
            raise AirflowException("Map function must be a python callable")
        
        if (reduce_function is not None and not callable(reduce_function)):
            raise AirflowException("Reduce function must be a python callable")
        
        self.op_args = op_args
        self.map_function = map_function
        self.reduce_function = reduce_function
        
        # Initialize BaseOperator
        super().__init__(*args, **kwargs)

    def execute(self, context):
        """
        Executes function and waits for result. Overrides 'execute' from BaseOperator.
        """
        # Setup parameters
        self.op_args = self._parameter_setup(context)      
        self.log.info("Parameters: {}".format(self.op_args))

        # Initialize IBM Cloud Functions hook
        self.hook = IbmCloudFunctionsHook()
        self.hook.get_conn()

        return_value = self.execute_callable()

        # Push result to xcom
        self.log.info("Done. Returned value was: %s", return_value)
        context['ti'].xcom_push(key=self.task_id, value=return_value)
        return return_value
    
    @abc.abstractclassmethod
    def execute_callable(self):
        pass
        
    def _parameter_setup(self, context):
        """
        Generates a list of dictionaries for the arguments of every map/map_reduce function.
        Example: op_args = {'iterdata' : [1,2,3], 'x' : 'iterdata', 'y' : 7}
        Returns: [{'x' : 1, 'y' : 7}, {'x' : 2, 'y' : 7}, {'x' : 3, 'y' : 7}]
        """
        # Return empty list if no parameters are provided
        if self.op_args is None:
            return []

        args = []

        for key,value in self.op_args.items():
            # Get value from a previous task
            if (isinstance(value, str) and re.search("^TASK:*", value) is not None):
                self.op_args[key] = context['ti'].xcom_pull(key=re.sub("TASK:*", "", value))
            # Get key argument to determine which argument recieves iterable data
            if value == 'iterdata':
                iter_key = key

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
        """
        Initializes IBM Cloud Function Basic Operator. Single function execution.
        :param function : Function callable.
        :param op_args : Dictionary where the key is the function's parameter 
        name and value the parameter value.
        """
        
        super().__init__(
            map_function=function,
            op_args=op_args,
            *args, **kwargs)
    
    def execute_callable(self):
        """
        Overrides 'execute_callable' from IbmCloudFunctionsOperator.
        Invokes a single function over IBM cloud functions.
        """
        return self.hook.invoke_call_async(self.map_function, self.op_args)

class IbmCloudFunctionsMapOperator(IbmCloudFunctionsOperator):
    def __init__(
            self,
            map_function,
            op_args=None,
            *args, **kwargs):
        """
        Initializes IBM Cloud Function Basic Operator. Multiple parallel function execution.
        :param map_function : Map function callable.
        :param op_args : Dictionary where the key is the function's parameter 
        name and value the parameter value.
        """
        
        super().__init__(
            map_function=map_function,
            op_args=op_args,
            *args, **kwargs)
    
    def execute_callable(self):
        """
        Overrides 'execute_callable' from IbmCloudFunctionsOperator.
        Invokes multiple parallel functions over IBM Cloud Functions.
        """
        return self.hook.invoke_map(self.map_function, self.op_args)

class IbmCloudFunctionsMapReduceOperator(IbmCloudFunctionsOperator):
    def __init__(
            self,
            map_function,
            reduce_function,
            op_args=None,
            *args, **kwargs):
        """
        Initializes IBM Cloud Function Basic Operator. Multiple parallel function execution
        with a reduce function that merges the results.
        :param map_function : Map function callable.
        :param op_args : Dictionary where the key is the function's parameter 
        name and value the parameter value.
        :param reduce_function : Reduce function callable.
        """

        super().__init__(
            map_function=map_function, 
            reduce_function=reduce_function,
            op_args=op_args,
            *args, **kwargs)
    
    def execute_callable(self):
        """
        Overrides 'execute_callable' from IbmCloudFunctionsOperator.
        Invokes multiple parallel functions over IBM Cloud Functions and a single function
        that merges map results.
        """
        return self.hook.invoke_map_reduce(self.map_function, self.op_args, self.reduce_function)