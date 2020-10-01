from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

from cloudbutton.engine import function_executor


class CloudbuttonToolkitHook(BaseHook):

    def __init__(self, conn_id='ibm_pywren_config'):
        """
        Initializes hook for IBM-PyWren
        """
        # Get config from Airflow's config parameters
        try:
            conn = self.get_connection(conn_id)
            self.cloudbutton_config = conn.extra_dejson
            self.log.debug('Cloudbutton config load from Airflow connections')
        except AirflowException:
            self.log.warning('Could not find Cloudbutton config in Airflow connections, \
            loading from ~/.cloudbutton_config instead')
            self.cloudbutton_config = {}

    def get_conn(self, pywren_executor_config):
        """
        Initializes PyWren IBM-Cloud executor.
        """
        pywren_executor_config['log_level'] = 'DEBUG'
        pywren_executor_config['config'] = self.cloudbutton_config
        return function_executor(**pywren_executor_config)
