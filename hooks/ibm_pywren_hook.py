from airflow.hooks import BaseHook
from airflow.exceptions import AirflowException

from pywren_ibm_cloud.config import EXECUTION_TIMEOUT
import pywren_ibm_cloud


class IbmPyWrenHook(BaseHook):

    def __init__(self, conn_id='ibm_pywren_config'):
        """
        Initializes hook for IBM-PyWren
        """
        # Get config from Airflow's config parameters
        conn = self.get_connection(conn_id)
        self.credentials = conn.extra_dejson

    def get_conn(self, executor_config):
        """
        Initializes PyWren IBM-Cloud executor.
        """
        try:
            executor = pywren_ibm_cloud.function_executor(config=self.credentials, **executor_config)
        except Exception as e:
            log = "Error while getting an executor for IBM Cloud Functions: {}".format(e)
            raise AirflowException(log)
        return executor