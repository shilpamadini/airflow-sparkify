from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('sparkify',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    s3_path="s3://udacity-dend/log_data",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region="us-west-2",
    data_format="JSON"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    s3_path="s3://udacity-dend/song_data",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region="us-west-2",
    data_format="JSON"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    table="songplays",
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert,
    append_only=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context=True,
    table="users",
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.user_table_insert,
    append_only=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context=True,
    table="songs",
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.song_table_insert,
    append_only=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context=True,
    table="artists",
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.artist_table_insert,
    append_only=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,
    table="time",
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.time_table_insert,
    append_only=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    tables=["artists", "time", "songs", "users", "songplays"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Setup Task Dependacies

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_songs_to_redshift >> load_artist_dimension_table
load_artist_dimension_table >> load_song_dimension_table
stage_events_to_redshift >> load_user_dimension_table
stage_events_to_redshift >> load_time_dimension_table

load_user_dimension_table >> load_songplays_table
load_song_dimension_table >> load_songplays_table
load_time_dimension_table >> load_songplays_table

load_songplays_table >> run_quality_checks

run_quality_checks >> end_operator
