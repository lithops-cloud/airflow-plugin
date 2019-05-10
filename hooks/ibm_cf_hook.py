from airflow.hooks import BaseHook

import pywren_ibm_cloud


class IbmCloudFunctionsHook(BaseHook):

    def __init__(self):
        self.exec_active = False

    def get_conn(self):
        executor = None
        if (not self.exec_active):
            executor = pywren_ibm_cloud.ibm_cf_executor()
            self.exec_active = True
        
        return executor