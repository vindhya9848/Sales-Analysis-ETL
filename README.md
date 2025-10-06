# Sales Analysis Project: Silver, Bronze, Gold Layers

This project follows a layered data architecture:

- **Bronze Layer:** Raw, unprocessed data ingested from source systems. Acts as the landing zone for all incoming data.
- **Silver Layer:** Cleaned and transformed data. Here, data quality checks, filtering, and standardization are performed to make the data analytics-ready.
- **Gold Layer:** Curated, business-ready datasets. These are aggregated or modeled specifically for reporting, dashboards, and advanced analytics.


<img width="1030" height="531" alt="image" src="https://github.com/user-attachments/assets/c3a60c4c-d540-4863-88db-fb95fad14d3b" />


## Folder Explanations

- **dags:** Contains workflow  DAG definitions for orchestrating data pipelines,  using  Apache Airflow.
- **metrics_and_views:** Stores SQL scripts for generating metrics tables and views.
- **config** Database configuration are defined

 You can run the whole pipeline by running: **main.py** file to manually trigger the pipeline, or orchestrate pipeline using airflow job: with **etl_pipeline.py** file in dag folder

 Additionally: the code has inline commenting explaining though process behind the logic, where ever necessary


The airflow dag checks for the file in a specific directory or datalake for the current date and then sequentially orchestrates the pipeline at a daily schedule to incrementally load fresh data inside the metrics tables and also to refresh the views
BRONZE => Silver => Gold
I created a sample airflow job to orchestrate the pipeline: Please find dag design:
•	We can use file_sensors in airflow: to detect the presence of file_name_DDMMYY.xlsx, 
•	Exact the business_date from current execution time or file_name
•	And then trigger  the pipeline in the order by specifying the dags
•	We can also implement failure and retry mechanism: (retries=3 and retry_delay=5min) 
•	Can configure to send slack alerts using on_failure_callback
For late arriving data:
•	Use a time window buffer: allow sensors to wait up to 6 hours for a late file
•	Maintain a checkpoint log or audit table to record processed business_date
•	If a late file arrives next day, backfill DAG manually or auto-trigger with execution_date override
