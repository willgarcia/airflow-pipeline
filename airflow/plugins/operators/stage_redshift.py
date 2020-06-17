from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    sql_template = """
    DROP TABLE IF EXISTS {destination_table};
    COPY {destination_table} 
        FROM '{source_s3bucket}'
        JSON '{json_path}'
        CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
    """

    @apply_defaults
    def __init__(self,
                 destination_table='',
                 source_s3bucket='',
                 json_path='',
                 redshift_conn_id='redshift',
                 aws_conn_id = 'aws_credentials',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.source_s3bucket = source_s3bucket
        self.json_path = json_path

    def execute(self, context):
        hook = AwsHook(aws_conn_id='aws_credentials')
        credentials_from_hook = hook.get_credentials()
        self.log.info("Executing Redshift COPY INTO {} from S3".format(self.destination_table))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql = StageToRedshiftOperator.sql_template.format(
            destination_table=self.destination_table,
            source_s3bucket=self.source_s3bucket,
            json_path=self.json_path,
          aws_access_key_id=credentials_from_hook.access_key,
       aws_secret_access_key=credentials_from_hook.secret_key
        )
        redshift.run(sql)
