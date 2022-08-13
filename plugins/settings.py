from dotenv import find_dotenv, load_dotenv
from pydantic import BaseModel, BaseSettings

load_dotenv(find_dotenv())


class S3Settings(BaseModel):
    bucket_name: str
    log_key: str
    song_key: str


class StagingTables(BaseModel):
    log_table: str
    log_jsonpath: str
    song_table: str


class SchemaTables(BaseModel):
    fact_songplays: str
    dim_song: str
    dim_artist: str
    dim_user: str
    dim_time: str


class Settings(BaseSettings):
    redshift_conn_id: str
    aws_cred_id: str
    s3: S3Settings
    staging: StagingTables
    table: SchemaTables

    class Config:
        env_nested_delimiter = "__"
        env_file = ".env"


settings = Settings()
