from airflow.plugins_manager import AirflowPlugin

from .hooks.ibm_pywren_hook import IBMPyWrenHook
from .operators.ibm_pywren_operator import IBMPyWrenCallAsyncOperator
from .operators.ibm_pywren_operator import IBMPyWrenMapOperator
from .operators.ibm_pywren_operator import IBMPyWrenMapReduceOperator

class IBMPyWrenAirflowPlugin(AirflowPlugin):
    name = "pywren_ibm_cloud_airflow"
    operators = [IBMPyWrenCallAsyncOperator, IBMPyWrenMapOperator, IBMPyWrenMapReduceOperator]
    hooks = [IBMPyWrenHook]
