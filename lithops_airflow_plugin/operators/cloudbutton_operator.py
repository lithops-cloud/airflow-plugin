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
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from cloudbutton_airflow_plugin.hooks.cloudbutton_toolkit_hook import CloudbuttonToolkitHook


class CloudbuttonOperator(BaseOperator):
    ui_color = '#c4daff'

    @apply_defaults
    def __init__(self,
                 cloudbutton_engine_config: dict = {},
                 wait_for_result: bool = True,
                 fetch_result: bool = True,
                 clean_data: bool = False,
                 extra_env=None,
                 runtime_memory=256,
                 timeout=600,
                 include_modules=[],
                 exclude_modules=[],
                 *args, **kwargs):
        """
        Abstract base clase for Cloudbutton operators. Initializes common variables.
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

        self.cloudbutton_engine_config = cloudbutton_engine_config
        self.wait_for_result = wait_for_result
        self.fetch_result = fetch_result
        self.clean_data = clean_data

        self.extra_env = extra_env
        self.runtime_memory = runtime_memory
        self.timeout = timeout
        self.include_modules = include_modules
        self.exclude_modules = exclude_modules

        self._function_result = None
        self._futures = None
        self._executor = None

        # Initialize BaseOperator
        super().__init__(*args, **kwargs)

    def execute(self, context):
        """
        Executes function. Overrides 'execute' from BaseOperator.
        """
        # Initialize Cloudbutton engine hook
        self._executor = CloudbuttonToolkitHook().get_conn(self.cloudbutton_engine_config)

        self._futures = self.execute_callable(context)
        self.log.info("Execution Done")

        if self.wait_for_result:
            self._executor.wait(fs=self._futures)
        else:
            self.log.info("Done: Not waiting for result")

        if self.fetch_result and self.wait_for_result:
            self._function_result = self._executor.get_result(fs=self._futures)
            self.log.info("Returned value was: {}".format(
                self._function_result))
        else:
            self.log.info("Done: Not downloading results")

        return self._function_result if self.fetch_result else self._futures

    def execute_callable(self, context):
        raise NotImplementedError()


class CloudbuttonCallAsyncOperator(CloudbuttonOperator):
    def __init__(self, func, data={}, data_from_task={}, **kwargs):
        """
        Executes an asyncronoys single function execution.
        :param function : Function callable.
        :param function_args : Function arguments.
        """

        self.func = func
        self.data = data
        self.data_from_task = data_from_task

        super().__init__(**kwargs)

    def execute_callable(self, context):
        """
        Overrides 'execute_callable' from CloudbuttonOperator.
        Wrap of cloudbutton engine call async function.
        """
        for k, v in self.data_from_task.items():
            self.data[k] = context['task_instance'].xcom_pull(task_ids=v)

        self.log.debug("Params: {}".format(self.data))
        return self._executor.call_async(self.func, self.data,
                                         extra_env=self.extra_env,
                                         runtime_memory=self.runtime_memory,
                                         timeout=self.timeout,
                                         include_modules=self.include_modules,
                                         exclude_modules=self.exclude_modules)


class CloudbuttonMapOperator(CloudbuttonOperator):
    def __init__(self,
                 map_function,
                 map_iterdata=None,
                 iterdata_from_task=None,
                 extra_args=None,
                 chunk_size=None,
                 chunk_n=None,
                 invoke_pool_threads=500,
                 **kwargs):
        """
        Executes a parallel map function.
        :param map_function : Map function callable.
        :param map_iterdata : Iterable data structure.
        """
        super().__init__(**kwargs)

        if map_iterdata is None and iterdata_from_task is None:
            raise AirflowException(
                'At least map_iterdata or iterdata_from_task must be set')

        self.map_function = map_function
        self.map_iterdata = map_iterdata
        self.iterdata_from_task = iterdata_from_task
        self.extra_args = extra_args
        self.chunk_size = chunk_size
        self.chunk_n = chunk_n
        self.invoke_pool_threads = invoke_pool_threads

    def execute_callable(self, context):
        """
        Overrides 'execute_callable' from IbmPyWrenOperator.
        Wrap of cloudbutton engine map function.
        """
        if self.iterdata_from_task is not None:
            if isinstance(self.iterdata_from_task, dict):
                kwarg, task_id = list(self.iterdata_from_task.items()).pop()
                values = context['task_instance'].xcom_pull(task_ids=task_id)
                self.map_iterdata = [{kwarg: value} for value in values]
            else:
                self.map_iterdata = context['task_instance'].xcom_pull(
                    task_ids=self.iterdata_from_task)

        self.log.debug("Params: {}".format(self.map_iterdata))

        return self._executor.map(map_function=self.map_function,
                                  map_iterdata=self.map_iterdata,
                                  extra_args=self.extra_args,
                                  extra_env=self.extra_env,
                                  runtime_memory=self.runtime_memory,
                                  chunk_size=self.chunk_size,
                                  chunk_n=self.chunk_n,
                                  timeout=self.timeout,
                                  invoke_pool_threads=self.invoke_pool_threads,
                                  include_modules=self.include_modules,
                                  exclude_modules=self.exclude_modules)


class CloudbuttonMapReduceOperator(CloudbuttonOperator):
    def __init__(
            self,
            map_function, reduce_function,
            map_iterdata=None,
            iterdata_from_task=None,
            extra_args=None,
            map_runtime_memory=None,
            reduce_runtime_memory=None,
            chunk_size=None,
            chunk_n=None,
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

        if map_iterdata is None and iterdata_from_task is None:
            raise AirflowException(
                'At least map_iterdata or iterdata_from_task must be set')

        self.map_iterdata = map_iterdata
        self.iterdata_from_task = iterdata_from_task
        self.reduce_function = reduce_function
        self.extra_args = extra_args
        self.map_runtime_memory = map_runtime_memory
        self.reduce_runtime_memory = reduce_runtime_memory
        self.chunk_size = chunk_size
        self.chunk_n = chunk_n
        self.reducer_one_per_object = reducer_one_per_object
        self.reducer_wait_local = reducer_wait_local
        self.invoke_pool_threads = invoke_pool_threads

        super().__init__(**kwargs)

    def execute_callable(self, context):
        """
        Overrides 'execute_callable' from IbmCloudFunctionsOperator.
        Wrap of cloudbutton engine map reduce function.
        """
        if self.iterdata_from_task is not None:
            if isinstance(self.iterdata_from_task, dict):
                kwarg, task_id = list(self.iterdata_from_task.items()).pop()
                values = context['task_instance'].xcom_pull(task_ids=task_id)
                self.map_iterdata = [{kwarg: value} for value in values]
            else:
                self.map_iterdata = context['task_instance'].xcom_pull(
                    task_ids=self.iterdata_from_task)

        self.log.debug("Params: {}".format(self.map_iterdata))

        return self._executor.map_reduce(map_function=self.map_function,
                                         map_iterdata=self.map_iterdata,
                                         reduce_function=self.reduce_function,
                                         extra_args=self.extra_args,
                                         extra_env=self.extra_env,
                                         map_runtime_memory=self.map_runtime_memory,
                                         reduce_runtime_memory=self.reduce_runtime_memory,
                                         chunk_size=self.chunk_size,
                                         chunk_n=self.chunk_n,
                                         timeout=self.timeout,
                                         reducer_one_per_object=self.reducer_one_per_object,
                                         reducer_wait_local=self.reducer_wait_local,
                                         invoke_pool_threads=self.invoke_pool_threads,
                                         include_modules=self.include_modules,
                                         exclude_modules=self.exclude_modules)
