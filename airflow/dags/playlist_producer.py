import pandas as pd
import requests
from kafka import KafkaProducer
from time import sleep
from json import dumps
from s3fs import S3FileSystem

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from plugins.email_trigger import EmailTrigger
from plugins.get_token import Get_Token

# Fetching Spotify Data
def get_all_tracks(access_token, playlist_id):
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    all_songs = []
    while url:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print("❌ Error fetching tracks:", response.status_code)
            print(response.text)
            break

        data = response.json()

        for item in data['items']:
            track = item['track']
            if track and track['id']:  # Skip removed or unavailable tracks
                song_info = {
                    "id": track['id'],
                    "name": track['name'],
                    "artists": [artist['name'] for artist in track['artists']],
                    "album": track['album']['name'],
                    "album_id": track['album']['id'],
                    "duration_ms": track['duration_ms'],
                    "external_url": track['external_urls']['spotify'],
                    "artist_ids": [artist['id'] for artist in track['artists']],
                    "release_date": track['album']['release_date'],
                    "track_number": track['track_number'],
                    "disc_number": track['disc_number'],
                    "duration_ms": track['duration_ms'],
                    "explicit": track['explicit'],
                    "popularity": track['popularity'],  # 0-100 popularity score
                    "uri": track['uri'],
                }
                all_songs.append(song_info)

        url = data['next']

    return all_songs

# Sending the data to consumer
def send_to_consumer():
    access_token=Get_Token()
    playlist_id = "37i9dQZF1E4A5nTVr5srnD"

    all_songs = get_all_tracks(access_token, playlist_id)

    producer = KafkaProducer(
        bootstrap_servers=['51.20.31.212:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    s3 = S3FileSystem()
    bucket_path = 'reddit-airflow-bucket-otina/spotify_songs/'

    try:
        existing_files = s3.ls(bucket_path)
        existing_ids = set(f.split('/')[-1].replace('.json', '') for f in existing_files)
    except FileNotFoundError:
        existing_ids = set()

    # ✅ Filter new songs only
    new_songs = [song for song in all_songs if song['id'] not in existing_ids]

    # ✅ Send in batches of 10
    batch_size = 10
    total_batches = len(all_songs) // batch_size + (1 if len(all_songs) % batch_size > 0 else 0)

    i = None
    for i in range(0, len(new_songs), batch_size):
        batch = new_songs[i:i+batch_size]
        for song in batch:
            producer.send('kafma', value=batch)
        print(f"✅ Sent batch {i // batch_size + 1} with {len(batch)} songs")
        sleep(10)

        producer.flush()

    if i != total_batches - 1:
        print(f"⏳ Batches over: With {len(all_songs)} songs processed")

# Mail confirming complete dag
def sending_mail(context):
    EmailTrigger(
        subject='Spotify playlist produced ✅',
        body='Spotify data successfully fetched to be consumed'
    )


dag = DAG(
    dag_id='spotify_playlist_produce',
    default_args={'start_date': datetime.now() - timedelta(days=1)},
    catchup=False,
    schedule='0 23 * * *',
    on_success_callback=sending_mail
)

trigger_consumer = TriggerDagRunOperator(
    task_id='trigger_consumer',
    trigger_dag_id='spotify_playlist_consume',
    wait_for_completion=False,
    dag=dag
)

send_to_consumer = PythonOperator(
    task_id='sending_to_consumer',
    python_callable=send_to_consumer,
    dag=dag
)

send_to_consumer >> trigger_consumer