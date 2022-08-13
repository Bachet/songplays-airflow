from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"

    def __init__(
        self,
        redshift_conn_id="",
        target_table="",
        insert_query="",
        *args,
        **kwargs
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.insert_query = insert_query

    def execute(self, context):
        self.log.info(f"Start executing LoadFactOperator for {self.target_table} table")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Running insert statement query")
        redshift.run(f"INSERT INTO {self.target_table} {self.insert_query}")
