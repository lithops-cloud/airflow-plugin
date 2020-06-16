from airflow.plugins_manager import AirflowPlugin

from cloudbutton_airflow_plugin.hooks.cloudbutton_toolkit_hook import CloudbuttonToolkitHook
from cloudbutton_airflow_plugin.operators.cloudbutton_operator import (
    CloudbuttonCallAsyncOperator,
    CloudbuttonMapOperator,
    CloudbuttonMapReduceOperator
)


class CloudbuttonAirflowPlugin(AirflowPlugin):
    name = "cloudbutton_airflow_plugin"
    operators = [CloudbuttonCallAsyncOperator,
                 CloudbuttonMapOperator,
                 CloudbuttonMapReduceOperator]
    hooks = [CloudbuttonToolkitHook]
