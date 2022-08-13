from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    def __init__(
        self,
        redshift_conn_id="",
        quality_check_stmts=None,
        *args,
        **kwargs
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        if quality_check_stmts is None:
            quality_check_stmts = {}
        self.redshift_conn_id = redshift_conn_id
        self.quality_check_stmts = quality_check_stmts

    def execute(self, context):
        self.log.info("Start executing DataQualityOperator")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for view, stmt in self.quality_check_stmts.items():
            records = redshift.get_records(stmt)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {view} returned no results")
            num_records = records[0][0]

            if num_records < 1:
                raise ValueError(f"Data quality on {view} failed with 0 records")

            self.log.info(f"Data quality on {view} check passed with {records[0][0]} records")
