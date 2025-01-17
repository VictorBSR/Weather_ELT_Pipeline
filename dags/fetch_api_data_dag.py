import requests
import json
import os
import pandas as pd
from io import StringIO
from io import BytesIO
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# init global and keys
load_dotenv()
api_key = os.getenv("API_KEY")
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
API_KEY = api_key
CITIES = ["London,GB", "New York,US", "São Paulo,BR"]
# Base URL for the API
BASE_URL = "https://api.openweathermap.org/data/2.5/forecast"
# Folder to save the raw JSON data
OUTPUT_FOLDER = os.path.expanduser("~/elt_pipeline/raw_weather_data")
AZURE_CONTAINER = "vbsr-filesystem"
RAW_DIR = "raw_weather"
PROCESSED_DIR = "processed_weather"
CURATED_DIR = "curated_weather"

# Initialize Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(AZURE_CONTAINER)

def extract_weather_data():
    # Create output folder it it doesn't exist
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    def fetch_data(city):
        try:
            response = requests.get(BASE_URL, params={"q": city, "appid": API_KEY, "units": "metric"})
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city}: {e}")
            return None
        
    def save_to_file(data, city):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{OUTPUT_FOLDER}/{city.replace(',','_')}_{timestamp}.json"
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)
        print(f"Saved data for {city} to {filename}")

        filename = f"{city.replace(',','_')}_{timestamp}.json"
        return filename

    # Upload file to Azure Blob Storage
    def upload_to_blob(filepath, filename):
        """
        Uploads a file to Azure Blob Storage.

        Args:
            filepath (str): The local path to the file to be uploaded.
            filename (str): The name to be used for the file in Blob Storage.

        Raises:
            Exception: If there is an error during the upload process.

        Returns:
            None
        """
        blob_name = f"{RAW_DIR}/{filename}"  # Path in Blob Storage
        try:
            with open(filepath, "rb") as data:
                container_client.upload_blob(name=blob_name, data=data, overwrite=True)
            print(f"Uploaded {filename} to Azure Blob Storage at {blob_name}")
        except Exception as e:
            print(f"Error uploading {filename} to Azure: {e}")


    ### main ###

    for city in CITIES:
        print(f"Fetching for {city}")
        data = fetch_data(city)
        if data:
            filename = save_to_file(data, city)
            filepath = os.path.join(OUTPUT_FOLDER, filename)
            upload_to_blob(filepath, filename)

def transform_weather_data():
    def move_processed_file(blob_name):
        # Define the new path in the processed directory
        new_blob_name = blob_name.replace(RAW_DIR, f"{RAW_DIR}/processed")

        # Get a blob client for the source and destination blobs
        source_blob_client = container_client.get_blob_client(blob_name)
        destination_blob_client = container_client.get_blob_client(new_blob_name)

        # Copy the blob to the new location
        copy_url = source_blob_client.url
        destination_blob_client.start_copy_from_url(copy_url)
        print(f"Copied {blob_name} to {new_blob_name}")

        # Delete the original raw file after moving it
        container_client.delete_blob(blob_name)
        print(f"Deleted original raw file: {blob_name}")

    # List all blobs in the raw_weather directory
    blobs = container_client.list_blobs(name_starts_with=RAW_DIR)
    for blob in blobs:
        blob_name = blob.name
        print(f'Blob found: {blob_name}')

        # Skip already processed blobs
        if f"{RAW_DIR}/processed" in blob_name:
            print(f"Skipping already processed blob: {blob_name}")
            continue

        # Skip non-JSON blobs
        if not blob_name.endswith('.json'):
            print(f"Skipping blob: {blob_name}")
            continue

        download_stream = container_client.download_blob(blob_name)
        raw_content = download_stream.readall()
        #print(f"Blob content for {blob_name}: {raw_content}")

        if not raw_content.strip():  # Check for empty content
            print(f"Blob {blob_name} is empty or has no valid content.")
            continue

        # Decode JSON content
        try:
            data = json.loads(raw_content)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for {blob_name}: {e}")
            continue

        # Flatten and transform the data (example transformation)
        transformed_data = [
            {
                "city": data["city"]["name"],
                "country": data["city"]["country"],
                "coordinates": f'{data["city"]["coord"]["lat"]},{data["city"]["coord"]["lon"]}',
                "datetime": forecast["dt_txt"],
                "temperature": forecast["main"]["temp"],
                "feels_like": forecast["main"]["feels_like"],
                "humidity": forecast["main"]["humidity"],
                "weather": forecast["weather"][0]["description"],
                "wind_speed": forecast["wind"]["speed"]
            }
            for forecast in data["list"]
        ]

        # Save as JSON to processed directory
        filename = blob_name.replace(RAW_DIR, PROCESSED_DIR).replace('/','/processed_')
        container_client.upload_blob(
            name=filename,
            data=json.dumps(transformed_data, indent=4),
            overwrite=True
        )
        print(f"Processed and saved data to: {filename}")

        move_processed_file(blob_name)

def curate_weather_data():

    def move_curated_file(blob_name):
        # Define the new path in the 'processed' subfolder
        new_blob_name = blob_name.replace(PROCESSED_DIR, f"{PROCESSED_DIR}/processed")

        # Get a blob client for the source and destination blobs
        source_blob_client = container_client.get_blob_client(blob_name)
        destination_blob_client = container_client.get_blob_client(new_blob_name)

        # Copy the blob to the new location
        copy_url = source_blob_client.url
        destination_blob_client.start_copy_from_url(copy_url)
        print(f"Copied {blob_name} to {new_blob_name}")

        # Delete the original file after moving it
        container_client.delete_blob(blob_name)
        print(f"Deleted original processed file: {blob_name}")
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CurateWeatherData") \
        .getOrCreate()
    
    # List blobs in the 'processed_weather' directory
    processed_blobs = container_client.list_blobs(name_starts_with=PROCESSED_DIR)
    
    for blob in processed_blobs:
        blob_name = blob.name
        
        # Skip already processed blobs
        if f"{PROCESSED_DIR}/processed/" in blob_name:
            print(f"Skipping already processed blob: {blob_name}")
            continue

        print(f"Curating blob: {blob_name}")

        # Skip non-JSON blobs
        if not blob_name.endswith('.json'):
            print(f"Skipping blob: {blob_name}")
            continue

        download_stream = container_client.download_blob(blob_name)
        raw_content = download_stream.readall().decode('utf-8')

        # Load JSON content into a Spark DataFrame
        try:
            rdd = spark.sparkContext.parallelize([raw_content])
            df = spark.read.json(rdd)
        except Exception as e:
            print(f"Error loading JSON for {blob_name}: {e}")
            continue

        # Convert datetime to date and add partition_date column
        df = df.withColumn("datetime", col("datetime").cast("timestamp"))
        df = df.withColumn("partition_date", to_date(col("datetime")))

        # Partition and write the data in Parquet format
        output_path = f"{CURATED_DIR}/"
        df.coalesce(1).write.partitionBy("partition_date").mode("overwrite").parquet(output_path)

        # List files in CURATED_DIR
        for root, dirs, files in os.walk(CURATED_DIR):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, CURATED_DIR)
                partitioned_blob_name = f"{CURATED_DIR}/{relative_path}"

                # Upload each file
                with open(file_path, "rb") as data:
                    container_client.upload_blob(
                        name=partitioned_blob_name,
                        data=data,
                        overwrite=True
                    )
                    print(f"Uploaded {partitioned_blob_name} to Azure Blob Storage")


        move_curated_file(blob_name)

default_args = {
    'owner':'VictorBSR',
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id='elt_pipeline_weather_data_dag',
    default_args=default_args,
    description='DAG for ELT Pipeline project',
    start_date=datetime(2024, 12, 12),
    schedule_interval=None,
    catchup=False,
    tags=["elt", "azure", "api", "storage"]
) as dag:
    # Task 1: Extract API data and Loat to blob
    extract_task = PythonOperator(
        task_id = 'extract_weather_data', 
        python_callable = extract_weather_data,
    )

    # Task 2: Transform raw json data into parquet
    transform_task = PythonOperator(
        task_id = 'transform_weather_data',
        python_callable = transform_weather_data,
    )

    # Task 3: Final Curation Placeholder
    curated_task = PythonOperator(
        task_id='curate_weather_data',
        python_callable = curate_weather_data,
    )


    # Task Dependencies
    extract_task >> transform_task >> curated_task
