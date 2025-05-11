from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime, timedelta
import requests
import json
import os
import logging
from airflow.exceptions import AirflowException
import pytz

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
LOCATIONS = [
    {"lat": "54.6778816", "lon": "-5.9249199"},
    {"lat": "52.6362", "lon": "-1.1331969"},
    {"lat": "51.456659", "lon": "-0.9696512"},
    {"lat": "54.1775283", "lon": "-6.337506"},
    {"lat": "51.4867", "lon": "0.2433"},
    {"lat": "53.4071991", "lon": "-2.99168"},
    {"lat": "53.3045372", "lon": "-1.1028469453936067"},
]


def fetch_weather_data(lat, lon, api_key):
    """Fetch weather data from OpenWeather API."""
    current_url = "https://api.openweathermap.org/data/2.5/weather"

    params = {
        'lat': lat,
        'lon': lon,
        'units': 'metric',
        'appid': api_key
    }

    try:
        current_response = requests.get(current_url, params=params)
        current_response.raise_for_status()
        current_weather = current_response.json()

        return {"current": current_weather}
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data: {e}")
        raise AirflowException(f"Failed to fetch weather data: {str(e)}")


def fetch_and_upload_to_azure(**kwargs):
    """Fetch data from OpenWeather and upload to Azure Blob."""
    if not OPENWEATHER_API_KEY:
        raise AirflowException("OPENWEATHER_API_KEY is not set")

    blob_hook = WasbHook(wasb_conn_id='azure_blob_default')
    run_date = kwargs['ds']
    file_name = f"weather_{run_date}.json"

    data = []
    for loc in LOCATIONS:
        try:
            result = fetch_weather_data(
                loc['lat'], loc['lon'], OPENWEATHER_API_KEY)
            data.append({
                "date": run_date,
                'weather': result['current']
            })
        except Exception as e:
            logging.error(f"Error processing location {loc}: {e}")
            continue

    if not data:
        raise AirflowException("No weather data was collected")

    # serialize to JSON string
    data_str = json.dumps(data)

    try:
        # upload to Azure Blob
        blob_hook.load_string(
            data_str,
            container_name="weather-data",
            blob_name=f"daily/{file_name}",
            overwrite=True
        )
        return {"file_path": f"daily/{file_name}"}
    except Exception as e:
        raise AirflowException(f"Failed to upload data to Azure: {str(e)}")


default_args = {
    "owner": "eddie",
    "start_date": datetime(2025, 5, 1, tzinfo=pytz.timezone('Europe/Stockholm')),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "timezone": 'Europe/Stockholm'
}

with DAG(
    dag_id="weather_to_azure",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False
) as dag:

    fetch_and_upload = PythonOperator(
        task_id="fetch_and_upload",
        python_callable=fetch_and_upload_to_azure,
        provide_context=True
    )

    trigger_load_data = TriggerDagRunOperator(
        task_id='trigger_load_data',
        trigger_dag_id='load_data_to_snowflake',
        wait_for_completion=True,
        poke_interval=60,
        conf={
            "file_path": "{{ task_instance.xcom_pull(task_ids='fetch_and_upload')['file_path'] }}",
            "run_date": "{{ ds }}"
        }
    )

    fetch_and_upload >> trigger_load_data
