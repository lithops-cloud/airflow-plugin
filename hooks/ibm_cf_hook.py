from airflow.hooks import BaseHook

import pywren_ibm_cloud


class IbmCloudFunctionsHook(BaseHook):

    def __init__(self, conn_id='ibm_cf_config'):
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
        self.executor = None
        if (self.executor is None):
            self.executor = pywren_ibm_cloud.ibm_cf_executor(
                config=self.config, 
                runtime=self.runtime, 
                runtime_memory=self.runtime_memory, 
                rabbitmq_monitor=self.rabbitmq_monitor,
                log_level=self.log_level)
        return self.executor

    def invoke_call_async(self, func, data, extra_env, extra_meta):
        self.executor.call_async(func, data, extra_env=extra_env, extra_meta=extra_meta)
        return self.executor.get_result()
    
    def invoke_map(self, map_function, map_iterdata):
        self.executor.map(map_function, map_iterdata)
        return self.executor.get_result()
    
    def invoke_mapreduce(self, map_function, map_iterdata, reduce_function):
        self.executor.map_reduce(map_function, map_iterdata, reduce_function)
        return self.executor.get_result()
