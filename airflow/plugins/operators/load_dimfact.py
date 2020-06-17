from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 sql_query='',
                 *args, **kwargs):
        super(LoadDimFactOperator, self).__init__(*args, **kwargs)
        self.sql_query=sql_query

    def execute(self, context):
        PostgresHook(postgres_conn_id=self.redshift_conn_id).run(self.sql_query)