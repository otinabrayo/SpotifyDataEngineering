import pandas as pd
import json
from kafka import KafkaConsumer
# from json import loads
from s3fs import S3FileSystem
# from time import sleep

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from plugins.email_trigger import EmailTrigger

def data_transfer_to_S3():
    s3 = S3FileSystem()
    bucket = 'reddit-airflow-bucket-otina'
    prefix = 'spotify_songs/'

    consumer = KafkaConsumer(
        'kafma',
        bootstrap_servers=['51.20.31.212:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='spotify-consumer-group',
        max_poll_interval_ms=600000,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=300000
    )

    try:
        total_processed = 0
        empty_polls = 0
        max_empty_polls = 3

        while True:
            messages = consumer.poll(timeout_ms=10000)

            if not messages:
                empty_polls += 1
                print(f"No new messages received. Empty poll count: {empty_polls}")
                if empty_polls >= max_empty_polls:
                    print("No new messages for a while. Checking if we're done...")
                    if total_processed > 0:
                        print(f"Processed {total_processed} songs in total. Exiting...")
                        break
                    else:
                        print("No songs processed yet. Waiting for more messages...")
                        empty_polls = 0
                continue

            empty_polls = 0

            for tp, msgs in messages.items():
                for message in msgs:
                    batch = message.value
                    processed_songs = 0
                    has_duplicates = False

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
                            processed_songs += 1
                            total_processed += 1
                        else:
                            has_duplicates = True
                    if has_duplicates:
                        print("⏳ Skipping duplicates in s3 bucket..")

                    consumer.commit()
                    print(f"Processed batch with {processed_songs} new songs")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        print(f"Final stats: Processed {total_processed} songs in total")
        consumer.close()
        print("Consumer closed successfully")


def sending_mail(context):
    EmailTrigger(
        subject='Spotify playlist consumed ✅',
        body='Spotify data successfully consumed and loaded to amazon S3'
    )

dag = DAG(
    dag_id='spotify_playlist_consume',
    default_args={'start_date': datetime.now() - timedelta(days=1)},
    catchup=False,
    schedule='0 23 * * *',
    on_success_callback=sending_mail
)

trigger_loading_to_snowflake = TriggerDagRunOperator(
    task_id='trigger_loading_to_snowflake',
    trigger_dag_id='playlist_table_making',
    wait_for_completion=False,
    dag=dag
)

send_to_s3 = PythonOperator(
    task_id='sending_to_S3',
    python_callable=data_transfer_to_S3,
    dag=dag
)

send_to_s3 >> trigger_loading_to_snowflake