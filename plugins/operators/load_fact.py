from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 sql_query="",
                 append_only="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.sql_query = sql_query
        self.append_only = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if not self.append_only:
            self.log.info("deleting data from Redshift table")
            redshift_hook.run("DELETE FROM {}".format(self.table))
        self.log.info("inserting data to Redshift table")
        redshift_hook.run(str(self.sql_query))
