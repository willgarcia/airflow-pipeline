from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.checks=checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for check in self.checks:
            sql = check.get('check_sql')
            exp_result = check.get('is_greater_than')
            result = redshift.get_records(sql)[0][0]
            if result <= exp_result:
                failing_tests.append(check)
                self.log.error("Failed - check {} / is_greater_than {}".format(sql, result))
            else:
                self.log.info("Success - check {} / is_greater_than {}".format(sql, result))
        if len(failing_tests) > 0:
            raise ValueError("DataQualityOperator failed.")

