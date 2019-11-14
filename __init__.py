from airflow.plugins_manager import AirflowPlugin
from ibm_cloud_functions_airflow_plugin.hooks.ibm_pywren_hook import IbmPyWrenHook
from ibm_cloud_functions_airflow_plugin.operators.ibm_pywren_operator import IbmPyWrenCallAsyncOperator
from ibm_cloud_functions_airflow_plugin.operators.ibm_pywren_operator import IbmPyWrenMapOperator
from ibm_cloud_functions_airflow_plugin.operators.ibm_pywren_operator import IbmPyWrenMapReduceOperator

class IbmPyWrenPlugin(AirflowPlugin):
    name = "ibm_pywren_plugin"
    operators = [IbmPyWrenCallAsyncOperator, IbmPyWrenMapOperator, IbmPyWrenMapReduceOperator]
    hooks = [IbmPyWrenHook]