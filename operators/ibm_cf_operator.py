from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from ibm_cloud_functions_airflow_plugin.hooks.ibm_cf_hook import IbmCloudFunctionsHook


class IbmCloudFunctionsOperator(BaseOperator):
    template_fields = ('templates_dict', 'op_args', 'op_kwargs')
    ui_color = '#ffefeb'

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects(e.g protobuf).
    shallow_copy_attrs = ('python_callable', 'op_kwargs',)

    @apply_defaults
    def __init__(
            self,
            python_callable,
            op_args=None,
            op_kwargs=None,
            provide_context=False,
            templates_dict=None,
            templates_exts=None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not callable(python_callable):
            raise AirflowException('`python_callable` param must be callable')
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.provide_context = provide_context
        self.templates_dict = templates_dict
        if templates_exts:
            self.template_ext = templates_exts
        
        #self.executor = IbmCloudFunctionsHook().get_conn()

    def execute(self, context):
        # Export context to make it available for callables to use.
        # airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        # self.log.info("Exporting the following env vars:\n%s",
        #               '\n'.join(["{}={}".format(k, v)
        #                          for k, v in airflow_context_vars.items()]))
        # os.environ.update(airflow_context_vars)

        # if self.provide_context:
        #     context.update(self.op_kwargs)
        #     context['templates_dict'] = self.templates_dict
        #     self.op_kwargs = context

        return_value = self.execute_callable()
        self.log.info("Done. Returned value was: %s", return_value)
        return return_value

    def execute_callable(self):
        self.executor = IbmCloudFunctionsHook().get_conn()
        self.executor.call_async(self.python_callable, 5)
        # FIXME : Fix 'failed to import unusual_import_*_imb_cf_operator' at serverless function execution
        return self.executor.get_result()