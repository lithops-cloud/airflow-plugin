#
# Copyright Cloudlab URV 2020
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
#

from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from lithops_airflow_plugin.hooks.lithops_hook import LithopsHook


class LithopsOperator(BaseOperator):
    ui_color = '#c4daff'

    @apply_defaults
    def __init__(self,
                 type: str = None,
                 config: dict = None,
                 backend: str = None,
                 storage: str = None,
                 runtime: str = None,
                 runtime_memory: int = None,
                 rabbitmq_monitor: bool = None,
                 workers: int = None,
                 remote_invoker: bool = None,
                 async_invoke: bool = False,
                 get_result: bool = True,
                 *args, **kwargs):
        """
        Wrapper around Lithops FunctionExecutor
        :param type Type of executor, one of ['serverless', 'localhost', 'standalone'] 
        :param config Lithops config. None to load from file or from Airflow connections config.
        :param backend Compute backend to use.
        :param storage Storage backend to use.
        :param runtime Docker image to use as runtime.
        :param runtime_memory Memory to assign to each function.
        :param rabbitmq_monitor Use RabbitMQ queues to retrieve status results instead of Storage.
        :param workers Maximum number of workers to use.
        :param remote_invoker Use remote invocation functionality. 
        :param async_invoke Asynchronous invocation, does not wait for functions to end execution.
        :param get_result Get functions result.
        """

        self.lithops_config = config if config is not None else {}
        self.async_invoke = async_invoke
        self.get_result = get_result

        self._executor_params = {
            'type': type,
            'config': config,
            'backend': backend,
            'storage': storage,
            'runtime': runtime,
            'runtime_memory': runtime_memory,
            'rabbitmq_monitor': rabbitmq_monitor,
            'workers': workers,
            'remote_invoker': remote_invoker
        }

        self._function_result = None
        self._futures = None
        self._executor = None

        # Initialize BaseOperator
        super().__init__(*args, **kwargs)

    def execute(self, context):
        """
        Executes function. Overrides 'execute' from BaseOperator.
        """
        # Initialize lithops hook
        self._executor = LithopsHook().get_conn(self.lithops_config)

        self._futures = self.execute_callable(context)
        self.log.info("Execution Done")

        if not self.async_invoke:
            self._executor.wait(fs=self._futures)
        else:
            self.log.info("Done: Not waiting for result")

        if self.get_result and not self.async_invoke:
            self._function_result = self._executor.get_result(fs=self._futures)
            self.log.debug("Returned value was: {}".format(
                self._function_result))
        else:
            self.log.info("Done: Not downloading results")

        return self._function_result if self.get_result else self._futures

    def execute_callable(self, context):
        raise NotImplementedError()


class LithopsCallAsyncOperator(LithopsOperator):
    def __init__(self,
                 func,
                 data,
                 data_from_task=None,
                 extra_env=None,
                 runtime_memory=None,
                 timeout=None,
                 include_modules=[],
                 exclude_modules=[],
                 **kwargs):
        """
        For running one function execution asynchronously

        :param func: the function to map over the data
        :param data: input data
        :param data_from_task: get data from another task as input
        :param extra_env: Additional environment variables for action environment. Default None.
        :param runtime_memory: Memory to use to run the function. Default None (loaded from config).
        :param timeout: Time that the functions have to complete their execution before raising a timeout.
        :param include_modules: Explicitly pickle these dependencies.
        :param exclude_modules: Explicitly keep these modules from pickled dependencies.
        """

        self.func = func
        self.data = data if data is not None else {}
        self.data_from_task = data_from_task if data_from_task is not None else {}
        self.extra_env = extra_env
        self.runtime_memory = runtime_memory
        self.timeout = timeout
        self.include_modules = include_modules
        self.exclude_modules = exclude_modules

        super().__init__(**kwargs)

    def execute_callable(self, context):
        """
        Overrides 'execute_callable' from LithopsOperator.
        Wrap of Lithops call async function.
        """
        for kwarg, value in self.data_from_task.items():
            data = context['task_instance'].xcom_pull(task_ids=value)
            if isinstance(self.data, list):
                self.data.append(data)
            elif isinstance(self.data, dict):
                self.data[kwarg] = data

        self.log.debug("Params: {}".format(self.data))
        return self._executor.call_async(func=self.func,
                                         data=self.data,
                                         extra_env=self.extra_env,
                                         runtime_memory=self.runtime_memory,
                                         timeout=self.timeout,
                                         include_modules=self.include_modules,
                                         exclude_modules=self.exclude_modules)


class LithopsMapOperator(LithopsOperator):
    def __init__(self,
                 map_function,
                 map_iterdata=None,
                 iterdata_from_task=None,
                 extra_args=None,
                 extra_env=None,
                 runtime_memory=None,
                 chunk_size=None,
                 chunk_n=None,
                 timeout=None,
                 invoke_pool_threads=500,
                 include_modules=[],
                 exclude_modules=[],
                 **kwargs):
        """
        Executes a parallel map function.

        :param map_function: the function to map over the data
        :param map_iterdata: An iterable of input data
        :param extra_args: Additional arguments to pass to the function activation. Default None.
        :param extra_env: Additional environment variables for action environment. Default None.
        :param runtime_memory: Memory to use to run the function. Default None (loaded from config).
        :param chunk_size: the size of the data chunks to split each object. 'None' for processing
                           the whole file in one function activation.
        :param chunk_n: Number of chunks to split each object. 'None' for processing the whole
                        file in one function activation.
        :param remote_invocation: Enable or disable remote_invocation mechanism. Default 'False'
        :param timeout: Time that the functions have to complete their execution before raising a timeout.
        :param invoke_pool_threads: Number of threads to use to invoke.
        :param include_modules: Explicitly pickle these dependencies.
        :param exclude_modules: Explicitly keep these modules from pickled dependencies.
        """
        super().__init__(**kwargs)

        if map_iterdata is None and iterdata_from_task is None:
            raise AirflowException(
                'At least map_iterdata or iterdata_from_task must be set')

        self.map_function = map_function
        self.map_iterdata = map_iterdata
        self.iterdata_from_task = iterdata_from_task
        self.extra_args = extra_args
        self.extra_env = extra_env
        self.runtime_memory = runtime_memory
        self.chunk_size = chunk_size
        self.chunk_n = chunk_n
        self.timeout = timeout
        self.invoke_pool_threads = invoke_pool_threads
        self.include_modules = include_modules
        self.exclude_modules = exclude_modules

    def execute_callable(self, context):
        """
        Overrides 'execute_callable' from LithopsOperator.
        Wrap of Lithops map function.
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


class LithopsMapReduceOperator(LithopsOperator):
    def __init__(self,
                 map_function,
                 reduce_function,
                 map_iterdata=None,
                 iterdata_from_task=None,
                 extra_args=None,
                 extra_env=None,
                 map_runtime_memory=None,
                 reduce_runtime_memory=None,
                 chunk_size=None,
                 chunk_n=None,
                 timeout=None,
                 invoke_pool_threads=500,
                 reducer_one_per_object=False,
                 reducer_wait_local=False,
                 include_modules=[],
                 exclude_modules=[],
                 **kwargs):
        """
        Map the map_function over the data and apply the reduce_function across all futures.
        This method is executed all within CF.

        :param map_function: the function to map over the data
        :param map_iterdata:  the function to reduce over the futures
        :param reduce_function:  the function to reduce over the futures
        :param extra_env: Additional environment variables for action environment. Default None.
        :param extra_args: Additional arguments to pass to function activation. Default None.
        :param map_runtime_memory: Memory to use to run the map function. Default None (loaded from config).
        :param reduce_runtime_memory: Memory to use to run the reduce function. Default None (loaded from config).
        :param chunk_size: the size of the data chunks to split each object. 'None' for processing
                           the whole file in one function activation.
        :param chunk_n: Number of chunks to split each object. 'None' for processing the whole
                        file in one function activation.
        :param timeout: Time that the functions have to complete their execution before raising a timeout.
        :param reducer_one_per_object: Set one reducer per object after running the partitioner
        :param reducer_wait_local: Wait for results locally
        :param invoke_pool_threads: Number of threads to use to invoke.
        :param include_modules: Explicitly pickle these dependencies.
        :param exclude_modules: Explicitly keep these modules from pickled dependencies.
        """

        self.map_function = map_function

        if map_iterdata is None and iterdata_from_task is None:
            raise AirflowException(
                'At least map_iterdata or iterdata_from_task must be set')

        self.map_iterdata = map_iterdata
        self.iterdata_from_task = iterdata_from_task
        self.reduce_function = reduce_function
        self.extra_args = extra_args
        self.extra_env = extra_env
        self.map_runtime_memory = map_runtime_memory
        self.reduce_runtime_memory = reduce_runtime_memory
        self.chunk_size = chunk_size
        self.chunk_n = chunk_n
        self.timeout = timeout
        self.reducer_one_per_object = reducer_one_per_object
        self.reducer_wait_local = reducer_wait_local
        self.invoke_pool_threads = invoke_pool_threads
        self.include_modules = include_modules
        self.exclude_modules = exclude_modules

        super().__init__(**kwargs)

    def execute_callable(self, context):
        """
        Overrides 'execute_callable' from LithopsOperator.
        Wrap of Lithops map reduce function.
        """
        if self.iterdata_from_task is not None:
            if isinstance(self.iterdata_from_task, dict):
                kwarg, task_id = list(self.iterdata_from_task.items()).pop()
                values = context['task_instance'].xcom_pull(task_ids=task_id)
                self.map_iterdata = [{kwarg: value} for value in values]
            else:
                self.map_iterdata = context['task_instance'].xcom_pull(task_ids=self.iterdata_from_task)

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
