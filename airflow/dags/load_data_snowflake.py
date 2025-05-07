from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeCheckOperator
from datetime import timedelta, datetime
from plugins.email_trigger import EmailTrigger


def sending_mail(context):
    EmailTrigger(
        subject='Spotify playlist loaded to snowflake ✅',
        body='Spotify data successfully fetched from kafka and loaded to snowflake for analysis'
    )

dag = DAG(
    dag_id='playlist_table_making',
    default_args={'start_date': datetime.now() - timedelta(days=1)},
    catchup=False,
    schedule='0 23 * * *',
    on_success_callback=sending_mail
)

load_table= SnowflakeCheckOperator(
    task_id='data_consumer_task',
    sql='./sqls/songs_loading.sql',
    snowflake_conn_id='snowflake_conn_id',
    dag=dag
)

load_table