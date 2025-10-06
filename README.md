# Sales Analysis Project: Silver, Bronze, Gold Layers

This project follows a layered data architecture:

- **Bronze Layer:** Raw, unprocessed data ingested from source systems. Acts as the landing zone for all incoming data.
- **Silver Layer:** Cleaned and transformed data. Here, data quality checks, filtering, and standardization are performed to make the data analytics-ready.
- **Gold Layer:** Curated, business-ready datasets. These are aggregated or modeled specifically for reporting, dashboards, and advanced analytics.

## Folder Explanations

- **dags:** Contains workflow  DAG definitions for orchestrating data pipelines,  using  Apache Airflow.
- **metrics_and_views:** Stores SQL scripts for generating metrics tables and views.
- **config** Database configuration are defined

 You can run the whole pipeline by running: **main.py** file to manually trigger the pipeline, or orchestrate pipeline using airflow job: with **etl_pipeline.py** file in dag folder

 Additionally: the code has inline commenting explaining though process behind the logic, where ever necessary
