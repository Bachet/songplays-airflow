from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from plugins.helpers import SqlQueries
from plugins.operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from plugins.operators.load_dimension import InsertMode
from plugins.settings import settings


default_args = {
    "owner": "udacity",
    "start_date": datetime(2022, 8, 1),
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "udac_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
)

start_operator = EmptyOperator(task_id="Begin_execution", dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    s3_bucket_name=settings.s3.bucket_name,
    s3_key=settings.s3.log_key,
    redshift_conn_id=settings.redshift_conn_id,
    aws_credentials_id=settings.aws_cred_id,
    table=settings.staging.log_table,
    log_jsonpath=settings.staging.log_jsonpath,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    s3_bucket_name=settings.s3.bucket_name,
    s3_key=settings.s3.song_key,
    redshift_conn_id=settings.redshift_conn_id,
    aws_credentials_id=settings.aws_cred_id,
    table=settings.staging.song_table,
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id=settings.redshift_conn_id,
    target_table=settings.table.fact_songplays,
    insert_query=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id=settings.redshift_conn_id,
    target_table=settings.table.dim_user,
    insert_query=SqlQueries.user_table_insert,
    insert_mode=InsertMode.truncate_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id=settings.redshift_conn_id,
    target_table=settings.table.dim_song,
    insert_query=SqlQueries.song_table_insert,
    insert_mode=InsertMode.truncate_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id=settings.redshift_conn_id,
    target_table=settings.table.dim_artist,
    insert_query=SqlQueries.artist_table_insert,
    insert_mode=InsertMode.truncate_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id=settings.redshift_conn_id,
    target_table=settings.table.dim_time,
    insert_query=SqlQueries.time_table_insert,
    insert_mode=InsertMode.truncate_insert,
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id=settings.redshift_conn_id,
    quality_check_stmts={
        "songplays": SqlQueries.check_songplays,
        "users": SqlQueries.check_users,
        "songs": SqlQueries.check_songs,
        "artists": SqlQueries.check_artists,
        "time": SqlQueries.check_time,
        "paid users": SqlQueries.check_paid_users,
    },
)

end_operator = EmptyOperator(task_id="Stop_execution", dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
