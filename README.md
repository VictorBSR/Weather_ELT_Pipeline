# Weather Data ELT Pipeline
ELT pipeline for loading and transfoming API files to Azure Blob Storage to be queried as structured SQL data

## About
This personal project aims at building an end-to-end data pipeline for weather data collection, transformation, and curation. The API used was [Open Weather](https://openweathermap.org/), more specifically the [5 day/3 hour forecast](https://openweathermap.org/forecast5). By performing the API call with a given city as parameter, we get a JSON file containing the predicted forecast of various weather features for every 3 hour in the next 5 days as response. The problem is that, even though this extraction is automated, the data is in a semi-structured format and hard to work with, specially for analytical purposes. Thus, I intended to create a scalable, automated process to fetch the weather data via API, transform it, and store it in a partitioned and appropriate format for analytical querying. The key tools used were:

- Azure Blob Storage: data storage in all stages
- Apache Airflow: orchestrating workflows and ensuring automated execution
- Apache Spark: data transformation and partitioning
- Python: main scripting tool in every step

## Architecture 

![alt text](/images/diagram.png)

The flow of data is as follows:

A series of three Airflow DAGs manage the pipeline, including extraction, transformation, and loading steps.
Weather data is fetched from an external free API and stored in Azure Blob Storage as raw files (JSON). Then the data is flattened and saved in an intemediate blob. Finally, it is transformed using Apache Spark and written as partitioned Parquet files in a curated folder.
The final curated data can be queried in Azure Synapse, by using serverless SQL pools, or loaded into tools like Power BI for analysis.


## Prerequisites

- [Azure Blob Storage Bucket](https://azure.microsoft.com/en-us/products/storage/blobs)
- [Airflow](https://airflow.apache.org/docs/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Python](https://www.python.org/)
- [Spark](https://spark.apache.org/docs/latest/)

## Setup

The most important steps to get the solution running were:

**Azure Environment Setup:** create an Azure Blob Storage account with containers for raw and curated data. Configured credentials to enable secure API access through Python libraries.

![alt text](/images/storage_browser_1.png)

**Airflow Initialization:** Installed Apache Airflow and initialized the environment with:

    $ airflow standalone

I created 3 DAGs to manage the pipeline tasks, with the option of triggering them manually or scheduling for more automation.

![alt text](/images/Airflow-DAG.gif)

**Python and PySpark Setup:** all DAGs were created using Python and PySpark is used as the Spark interface since it's easier to implement keep all code concentrated into a single file. The Python scripts perform the API calls, transformations (flattening), and movements within Azure Blob using specific libraries.

![alt text](/images/python.png)

## Results

![alt text](/images/storage_browser_2.png)
![alt text](/images/storage_browser_3.png)

After all the implementation and testing, the pipeline successfully automated the download and storage of weather data in raw and curated formats.
The solution ensures that the data is partitioned by date for optimal querying and storage efficiency. The choice of curated file format as Parquet files lies in their advantages: it's a column-oriented, analytics-ready format for downstream use, which can be partitioned and queried in Azure Synapse or loaded into visualization tools like Power BI.

![alt text](/images/sql_query.png)
