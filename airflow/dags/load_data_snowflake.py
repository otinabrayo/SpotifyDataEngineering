from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeCheckOperator
from datetime import timedelta, datetime


dag = DAG(
    'playlist_table_making',
    default_args={'start_date': datetime.now() - timedelta(days=1)},
    catchup=False,
    schedule='0 23 * * *'
)

load_table= SnowflakeCheckOperator(
    task_id='data_consumer_task',
    sql='./sqls/songs.sql',
    snowflake_conn_id='snowflake_conn_id',
    dag=dag
)

load_table