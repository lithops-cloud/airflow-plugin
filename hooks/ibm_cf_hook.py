from airflow.hooks import BaseHook
from airflow.exceptions import AirflowException

import pywren_ibm_cloud


class IbmCloudFunctionsHook(BaseHook):

    def __init__(self, conn_id='ibm_cf_config'):
        """
        Initializes hook for IBM Cloud Functions.
        """
        # Get config from Airflow's config parameters
        conn = self.get_connection(conn_id)
        self.config = conn.extra_dejson

        executor_conf = {}
        if ("executor" in self.config):
            executor_conf = self.config["executor"]
            del self.config["executor"]

        self.runtime = executor_conf["runtime"] if "runtime" in executor_conf else None
        self.runtime_memory = executor_conf["runtime_memory"] if "runtime_memory" in executor_conf else None
        self.rabbitmq_monitor = executor_conf["rabbitmq_monitor"] if "rabbitmq_monitor" in executor_conf else None
        self.log_level = executor_conf["log_level"] if "log_level" in executor_conf else None

    def get_conn(self):
        """
        Initializes PyWren IBM-Cloud executor.
        """
        self.executor = None
        if (self.executor is None):
            try:
                self.executor = pywren_ibm_cloud.ibm_cf_executor(
                    config=self.config, 
                    runtime=self.runtime, 
                    runtime_memory=self.runtime_memory, 
                    rabbitmq_monitor=self.rabbitmq_monitor,
                    log_level=self.log_level)
            except Exception as e:
                log = "Error while getting an executor for IBM Cloud Functions: {}".format(e)
                raise AirflowException(log)

    def invoke_call_async(self, func, data):
        """
        Calls method 'call_async' from PyWren IBM-Cloud and waits for result.
        """
        self.executor.call_async(func, data)
        return self.executor.get_result()
    
    def invoke_map(self, map_function, map_iterdata, chunk_size, data_all_as_one, exclude_modules):
        """
        Calls method 'map' from PyWren IBM-Cloud and waits for result.
        """
        self.executor.map(map_function, map_iterdata, chunk_size=chunk_size, 
            data_all_as_one=data_all_as_one, exclude_modules=exclude_modules)
        return self.executor.get_result()
    
    def invoke_map_reduce(self, map_function, map_iterdata, reduce_function, chunk_size,
        reducer_one_per_object, reducer_wait_local, data_all_as_one, exclude_modules):
        """
        Calls method 'map_reduce' from PyWren IBM-Cloud and waits for result.
        """
        self.executor.map_reduce(map_function, map_iterdata, reduce_function,
            chunk_size=chunk_size, reducer_one_per_object=reducer_one_per_object,
            reducer_wait_local=reducer_wait_local, data_all_as_one=data_all_as_one,
            exclude_modules=exclude_modules)
        return self.executor.get_result()
