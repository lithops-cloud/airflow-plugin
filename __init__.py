from airflow.plugins_manager import AirflowPlugin
from ibm_cloud_functions_airflow_plugin.hooks.ibm_cf_hook import IbmCloudFunctionsHook
from ibm_cloud_functions_airflow_plugin.operators.ibm_cf_operator import IbmCloudFunctionsOperator

class IbmCloudFuntionsPlugin(AirflowPlugin):
    name = "ibm_cloud_functions_plugin"
    operators = [IbmCloudFunctionsOperator]
    hooks = [IbmCloudFunctionsHook]