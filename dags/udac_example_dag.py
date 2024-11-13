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
    'owner': 'smit patel',
    'start_date': datetime(2024, 11, 13),
    'depends_on_past': False,  # Ensures the DAG does not depend on past runs
    'retries': 3,  # Retry tasks 3 times on failure
    'retry_delay': timedelta(minutes=5),  # Retry every 5 minutes
    'catchup': False,  # Turn off catchup to avoid backfilling
    'email_on_retry': False  # Do not email on retry
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='smit-airflow-project',
    s3_key='log-data/2018/11/',  # Path for event data
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='smit-airflow-project',
    s3_key='song-data/A/A/A/',  # Path for song data
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    table='factSongPlay',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',  # Your connection ID to Redshift
    table='dimUser',  # Name of the dimension table in Redshift
    insert_query=SqlQueries.user_table_insert,  # The SQL query to select the data
    truncate_table=True, 
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    table='dimSong',
    insert_query=SqlQueries.song_table_insert,
    truncate_table=True,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    table='dimArtist',
    insert_query=SqlQueries.artist_table_insert,
    truncate_table=True,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    table='dimTime',
    insert_query=SqlQueries.time_table_insert,
    truncate_table=True,
    dag=dag
)

 # Quality checks on each table
tables_to_check = ['staging_events', 'staging_songs', 'factSongPlay', 'dimUser', 'dimSong', 'dimArtist', 'dimTime']
quality_check_tasks = []

for table in tables_to_check:
        quality_check_task = DataQualityOperator(
            task_id=f"Data_quality_check_{table}",
            dag=dag,
            redshift_conn_id="redshift",
            table=table
            
        )
        quality_check_tasks.append(quality_check_task)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_artist_dimension_table, load_song_dimension_table, load_time_dimension_table, load_user_dimension_table]
[load_artist_dimension_table, load_song_dimension_table, load_time_dimension_table, load_user_dimension_table] >> quality_check_tasks
quality_check_tasks >> end_operator