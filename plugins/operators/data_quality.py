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

        for table, check in self.quality_check_stmts.items():
            records = redshift.get_records(check["stmt"])
            if not int(records[0][0]) == check["expected"]:
                raise ValueError(f"Data quality check failed for table {table} with {records[0][0]} records "
                                 f"while expected {check['expected']}")
            self.log.info(f"Data quality check for {table} check passed with {records[0][0]} records")
