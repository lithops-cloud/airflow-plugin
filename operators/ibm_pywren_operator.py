
# Copyright 2019
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from ibm_cloud_functions_airflow_plugin.hooks.ibm_pywren_hook import IbmPyWrenHook, EXECUTION_TIMEOUT

class IbmPyWrenOperator(BaseOperator):
    ui_color = '#c4daff'

    @apply_defaults
    def __init__(
            self,
            executor_config: dict = {},
            wait_for_result: bool = True,
            fetch_results: bool = True,
            clean_data: bool = False,
            extra_env=None,
            runtime_memory=EXECUTION_TIMEOUT,
            include_modules=[],
            exclude_modules=[],
            *args, **kwargs):
        """
        Initializes IBM Cloud Functions Operator.
        :param map_function Map function callable.
        :param reduce_function Reduce function callable.
        :param op_args Key word arguments for function.
        :param wait_for_result If set to True, execution blocks until
        all functions are completed.
        :param fetch_results If set to True, the results of the functions will be
        pushed to local xcom 
        :param clean_data If set to True, PyWren metadata will be removed from 
        object storage
        """
        
        self.executor_config = executor_config
        self.wait_for_result = wait_for_result
        self.fetch_results = fetch_results
        self.clean_data = clean_data
        self.extra_env = extra_env
        self.runtime_memory = runtime_memory
        self.include_modules = include_modules
        self.exclude_modules = exclude_modules

        self.__function_result = None
        self.futures = None
        
        # Initialize BaseOperator
        super().__init__(*args, **kwargs)
    
    @property
    def result(self):
        if self.__function_result is None:
            self.__function_result = self.executor.get_result(self.futures)
        return self.__function_result

    def execute(self, context):
        """
        Executes function. Overrides 'execute' from BaseOperator.
        """
        # Initialize IBM Cloud Functions hook
        self.log.info("Context: {}".format(context))
        self.executor = IbmPyWrenHook().get_conn(self.executor_config)

        self.futures = self.execute_callable()
        self.log.info("Execution Done")

        if self.wait_for_result:
            self.log.info("Waiting for function completion")
            self.executor.wait(fs=self.futures)
        if self.fetch_results:
            self.log.info("Getting function results")
            self.__function_result = self.executor.get_result(fs=self.futures)
            self.log.info("Done. Returned value was: {}".format(self.__function_result))
            # Push function result to xcom
            context['ti'].xcom_push(key=self.task_id, value=self.__function_result)
        
        return_value = self.__function_result if self.fetch_results else self.futures
        return return_value
    
    def execute_callable(self):
        raise NotImplementedError()
        
    def __parameter_setup(self, context):
        """
        Generates a list of dictionaries for the arguments of every map/map_reduce function.
        Example: op_args = {'iterdata' : [1,2,3], 'x' : 'iterdata', 'y' : 7}
        Returns: [{'x' : 1, 'y' : 7}, {'x' : 2, 'y' : 7}, {'x' : 3, 'y' : 7}]
        """
        # Return empty list if no parameters are provided
        if self.op_args is None:
            return []

        # Check gather iterdata from bucket
        if 'bucket' in self.op_args:
            return self.op_args['bucket']

        args = []

        for key,value in self.op_args.items():
            # Get value from a previous task
            if (isinstance(value, str) and re.search("^FROM_TASK:*", value) is not None):
                self.op_args[key] = context['ti'].xcom_pull(key=re.sub("FROM_TASK:*", "", value))
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


class IbmPyWrenCallAsyncOperator(IbmPyWrenOperator):
    def __init__(
            self, func, data,
            **kwargs):
        """
        Initializes IBM Cloud Function Call Async Operator. Single function execution.
        :param function : Function callable.
        :param function_args : Function arguments.
        """

        self.func = func
        self.data = data
        
        super().__init__(**kwargs)
        
    def execute_callable(self):
        """
        Overrides 'execute_callable' from IbmPyWrenOperator.
        Invokes a single function.
        """
        return self.executor.call_async(
            self, 
            func=self.func, 
            data=self.data, 
            extra_env=self.extra_env, 
            runtime_memory=self.runtime_memory,
            timeout=self.timeout,
            include_modules=self.include_modules, 
            exclude_modules=self.exclude_modules)

class IbmPyWrenMapOperator(IbmPyWrenOperator):
    def __init__(
        self, map_function, map_iterdata,
        extra_params=None,
        chunk_size=None,
        chunk_n=None,
        remote_invocation=False,
        remote_invocation_groups=None,
        invoke_pool_threads=500,
        **kwargs):
        """
        Initializes IBM Cloud Function Map Operator. Multiple parallel function execution.
        :param map_function : Map function callable.
        :param map_iterdata : Iterable data structure.
        """
        super().__init__(**kwargs)
        
        self.map_function = map_function
        self.map_iterdata = map_iterdata
        self.extra_params = extra_params
        self.chunk_size = chunk_size
        self.chunk_n = chunk_n
        self.remote_invocation = remote_invocation
        self.remote_invocation_groups = remote_invocation_groups
        self.invoke_pool_threads = invoke_pool_threads
    
    def execute_callable(self):
        """
        Overrides 'execute_callable' from IbmPyWrenOperator.
        Invokes multiple parallel functions over IBM Cloud Functions.
        """
        return self.executor.map(
            map_function=self.map_function,
            map_iterdata=self.map_iterdata,
            extra_params=self.extra_params,
            extra_env=self.extra_env,
            runtime_memory=self.runtime_memory,
            chunk_size=self.chunk_size,
            chunk_n=self.chunk_n,
            remote_invocation=self.remote_invocation,
            remote_invocation_groups=self.remote_invocation_groups,
            timeout=self.timeout,
            invoke_pool_threads=self.invoke_pool_threads,
            include_modules=self.include_modules,
            exclude_modules=self.exclude_modules)


class IbmPyWrenMapReduceOperator(IbmPyWrenOperator):
    def __init__(
            self,
            map_function, map_iterdata, reduce_function, 
            extra_params=None, 
            map_runtime_memory=None, 
            reduce_runtime_memory=None, 
            chunk_size=None, 
            chunk_n=None, 
            remote_invocation=False, 
            remote_invocation_groups=None, 
            reducer_one_per_object=False, 
            reducer_wait_local=False, 
            invoke_pool_threads=500,
            **kwargs):
        """
        Initializes IBM Cloud Function Basic Operator. Multiple parallel function execution.
        with a reduce function that merges the results.
        :param map_function : Map function callable.
        :param map_iterdata Iterable data structure.
        :param reduce_function : Reduce function callable.
        """

        self.map_function = map_function
        self.map_iterdata = map_iterdata
        self.reduce_function = reduce_function
        self.extra_params = extra_params
        self.map_runtime_memory = map_runtime_memory
        self.reduce_runtime_memory = reduce_runtime_memory
        self.chunk_size = chunk_size
        self.chunk_n = chunk_n
        self.remote_invocation = remote_invocation
        self.remote_invocation_groups = remote_invocation_groups
        self.reducer_one_per_object = reducer_one_per_object
        self.reducer_wait_local = reducer_wait_local
        self.invoke_pool_threads = invoke_pool_threads

        super().__init__(**kwargs)

    def execute_callable(self):
        """
        Overrides 'execute_callable' from IbmCloudFunctionsOperator.
        Invokes multiple parallel functions over IBM Cloud Functions and a single function
        that merges map results.
        """
        return self.executor.map_reduce(
            map_function=self.map_function,
            map_iterdata=self.map_iterdata,
            reduce_function=self.reduce_function,
            extra_params=self.extra_params,
            extra_env=self.extra_env,
            map_runtime_memory=self.map_runtime_memory,
            reduce_runtime_memory=self.reduce_runtime_memory,
            chunk_size=self.chunk_size,
            chunk_n=self.chunk_n,
            remote_invocation=self.remote_invocation, 
            remote_invocation_groups=self.remote_invocation_groups,
            timeout=self.timeout,
            reducer_one_per_object=self.reducer_one_per_object,
            reducer_wait_local=self.reducer_wait_local,
            invoke_pool_threads=self.invoke_pool_threads,
            include_modules=self.include_modules,
            exclude_modules=self.exclude_modules)