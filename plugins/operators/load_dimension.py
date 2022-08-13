from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from enum import Enum


class InsertMode(str, Enum):
    truncate_insert = "TRUNCATE_INSERT"
    append = "APPEND"


class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"

    def __init__(
        self,
        redshift_conn_id="",
        target_table="",
        insert_query="",
        insert_mode=InsertMode.truncate_insert,
        *args,
        **kwargs
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.insert_query = insert_query
        self.insert_mode = insert_mode

    def execute(self, context):
        self.log.info(f"Start executing LoadDimensionOperator for {self.target_table} table")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Running insert statement query with mode: {self.insert_mode.value}")
        if self.insert_mode == InsertMode.truncate_insert:
            redshift.run(f"DELETE FROM {self.target_table}")
            redshift.run(f"INSERT INTO {self.target_table} {self.insert_query}")
        elif self.insert_mode.append:
            redshift.run(f"INSERT INTO {self.target_table} {self.insert_query}")
        else:
            raise ValueError(f"Selected mode {self.insert_mode.value} is not available ATM")
