import pandas as pd
import json
from kafka import KafkaConsumer
from json import loads
from s3fs import S3FileSystem

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.email_trigger import EmailTrigger

def data_transfer_to_S3():
    s3 = S3FileSystem()
    bucket = 'reddit-airflow-bucket-otina'
    prefix = 'spotify_songs/'

    consumer = KafkaConsumer(
        'kafma',
        bootstrap_servers=['13.60.188.23:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='spotify-consumer-group',
        max_poll_interval_ms=600000
    )

    for message in consumer:
        batch = message.value  # this is a list of songs

        for song_data in batch:
            song_id = song_data.get('uri', '').split(':')[-1]
            s3_path = f"s3://{bucket}/{prefix}/{song_id}.json"

            if not song_id:
                print("❌ Missing song ID, skipping...")
                continue
            if not s3.exists(s3_path):
                with s3.open(s3_path, 'w') as file:
                    json.dump(song_data, file)
                print(f"✅ Saved: {song_data['name']}")
            else:
                print(f"⏩ Skipped duplicate: {song_data['name']}")


def sending_mail(context):
    EmailTrigger(
        subject='Spotify playlist consumed ✅',
        body='Spotify data successfully consumed and loaded to amazon S3'
    )


dag = DAG(
    'spotify_playlist_consume',
    default_args={'start_date': datetime.now() - timedelta(days=1)},
    catchup=False,
    schedule='0 23 * * *',
    on_success_callback=sending_mail
)

send_to_s3 = PythonOperator(
    task_id='sending_to_S3',
    python_callable=data_transfer_to_S3,
    dag=dag
)

send_to_s3