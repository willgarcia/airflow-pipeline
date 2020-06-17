from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 sql_insert='',
                 sql_truncate='',
                 append_data=True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql_insert=sql_insert
        self.sql_truncate=sql_truncate
        self.append_data=append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data == False:
            self.log.info("Executing Redshift TRUNCATE on the Dimension table")
            redshift.run(self.sql_truncate)    

        self.log.info("Executing Redshift INSERT into the Dimension table")
        redshift.run(self.sql_insert)
