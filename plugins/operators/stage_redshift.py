from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"

    def __init__(
            self,
            s3_bucket_name="",
            s3_key="",
            redshift_conn_id="",
            aws_credentials_id="",
            table="",
            log_jsonpath="auto",
            *args,
            **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket_name = s3_bucket_name
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.log_jsonpath = log_jsonpath

    def execute(self, context):
        s3_path = f"s3://{self.s3_bucket_name}/{self.s3_key}"
        self.log.info(f"Start executing StageToRedshiftOperator for {s3_path} to {self.table}")
        aws_hook = AwsBaseHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        redshift.run(
            f"""
            COPY {self.table} FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY '{credentials.secret_key}'
            JSON '{self.log_jsonpath}'
            """
        )
