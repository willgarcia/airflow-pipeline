from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, RedshiftQueryOperator,
                               DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'Will Garcia',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('s3_redshift',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_song_table = RedshiftQueryOperator(
    task_id='Create_staging_song_table',
    dag=dag,
    sql_query=SqlQueries.staging_songs_sql
)

create_staging_events_table = RedshiftQueryOperator(
    task_id='Create_staging_events_table',
    dag=dag,
    sql_query=SqlQueries.staging_events_sql
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    destination_table='staging_events',
    source_s3bucket='s3://udacity-dend/log_data',
    json_path='s3://udacity-dend/log_json_path.json',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    destination_table='staging_songs',
    source_s3bucket='s3://udacity-dend/song_data',
    json_path='auto',
    dag=dag
)

load_songplays_table = RedshiftQueryOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = RedshiftQueryOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    sql_query=SqlQueries.user_table_insert
)
    
load_song_dimension_table = RedshiftQueryOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    sql_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = RedshiftQueryOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    sql_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = RedshiftQueryOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    sql_query=SqlQueries.time_table_insert
)
    

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    tables=['factsongplay', 'dimuser', 'dimsong', 'dimartist', 'dimtime'],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_staging_events_table
start_operator >> create_staging_song_table

create_staging_events_table >> stage_events_to_redshift
create_staging_song_table >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table>> run_quality_checks
load_time_dimension_table  >> run_quality_checks

run_quality_checks >> end_operator