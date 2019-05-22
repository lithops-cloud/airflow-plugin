from airflow.hooks import BaseHook

import pywren_ibm_cloud


class IbmCloudFunctionsHook(BaseHook):

    def __init__(self, conn_id='ibm_cf_config'):
        conn = self.get_connection(conn_id)
        self.config = conn.extra_dejson

    def get_conn(self):
        executor = None
        if (executor is None):
            executor = pywren_ibm_cloud.ibm_cf_executor(config=self.config)
        
        return executor