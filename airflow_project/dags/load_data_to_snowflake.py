from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import json
import logging
from airflow.exceptions import AirflowException

CONTAINER_NAME = "weather-data"


def transform_data(raw_data):
    """transform data to snowflake table structure"""
    transformed_data = []

    for data in raw_data:
        record = data['weather']
        date = data['date']
        coord = record.get("coord", {})
        weather = record.get("weather", [{}])[0]
        main = record.get("main", {})
        wind = record.get("wind", {})
        clouds = record.get("clouds", {})
        sys_info = record.get("sys", {})

        sunrise = datetime.fromtimestamp(
            sys_info["sunrise"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        sunset = datetime.fromtimestamp(
            sys_info["sunset"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        dt = datetime.fromtimestamp(
            record["dt"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        transformed_record = {
            "date": date,
            "lon": coord.get("lon"),
            "lat": coord.get("lat"),
            "main": weather.get("main"),
            "description": weather.get("description"),
            "icon": weather.get("icon"),
            "temp": main.get("temp"),
            "feels_like": main.get("feels_like"),
            "temp_min": main.get("temp_min"),
            "temp_max": main.get("temp_max"),
            "pressure": main.get("pressure"),
            "humidity": main.get("humidity"),
            "wind_speed": wind.get("speed"),
            "wind_deg": wind.get("deg"),
            "clouds": clouds.get("all"),
            "country": sys_info.get("country"),
            "sunrise": sunrise,
            "sunset": sunset,
            "dt": dt
        }
        transformed_data.append(transformed_record)

    return transformed_data


def load_data_to_snowflake(**kwargs):
    """load data to snowflake"""

    conf = kwargs.get('dag_run').conf
    if not conf:
        raise AirflowException("No configuration provided by trigger")

    file_path = conf.get('file_path')
    if not file_path:
        raise AirflowException("No file path provided in configuration")

    blob_hook = WasbHook(wasb_conn_id='azure_blob_default')
    logging.info(f"Downloading {file_path} from Azure Blob...")

    try:
        file_content = blob_hook.read_file("weather-data", file_path)
        data = json.loads(file_content)
    except Exception as e:
        logging.error(f"Error reading or parsing file: {e}")
        raise AirflowException(f"Failed to read or parse file: {str(e)}")

    transformed_data = transform_data(data)

    insert_query = """
    INSERT INTO weather_data (
        date, lon, lat, main, description, icon, temp, feels_like,
        temp_min, temp_max, pressure, humidity, wind_speed, wind_deg,
        clouds, country, sunrise, sunset, dt
    ) VALUES (
        %(date)s, %(lon)s, %(lat)s, %(main)s, %(description)s, %(icon)s, %(temp)s, %(feels_like)s,
        %(temp_min)s, %(temp_max)s, %(pressure)s, %(humidity)s, %(wind_speed)s, %(wind_deg)s,
        %(clouds)s, %(country)s, %(sunrise)s, %(sunset)s, %(dt)s
    );
    """
    for record in transformed_data:
        snowflake_op = SnowflakeOperator(
            task_id=f"load_record_{record['lon']}_{record['lat']}",
            snowflake_conn_id="snowflake_default",
            sql=insert_query,
            parameters=record,
            autocommit=True,
        )
        snowflake_op.execute(kwargs)


# DAG
default_args = {
    "owner": "eddie",
    "start_date": datetime(2025, 5, 10),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="load_data_to_snowflake",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    load_task = PythonOperator(
        task_id="load_data_to_snowflake",
        python_callable=load_data_to_snowflake,
        provide_context=True
    )
