# from airflow.sensors.external_task import ExternalTaskSensor
# from airflow.operators.python import PythonOperator
# from airflow import DAG
# from datetime import datetime, timedelta
# import subprocess

# def run_consumer():
#     subprocess.run(["python", "/opt/airflow/dags/playlist_consumer.py"])

# default_args = {
#     'owner': 'brian',
#     'depends_on_past': False,
#     'start_date': datetime(2025, 5, 1),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'consumer_dag',
#     default_args=default_args,
#     schedule='*/15 * * * *',
#     catchup=False
# )

# wait_for_producer = ExternalTaskSensor(
#     task_id='waiting_for_producer',
#     external_dag_id='spotify_playlist_produce',
#     external_task_id='sending_to_consumer',
#     mode='poke',
#     timeout=3600,
#     dag=dag
# )

# consume_task = PythonOperator(
#     task_id='consume_from_kafka',
#     python_callable=run_consumer,
#     dag=dag
# )

# wait_for_producer >> consume_task