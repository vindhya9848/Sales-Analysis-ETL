
import warnings
import psycopg2
from data_ingestion import data_ingestion
from config.get_db_config import get_db_config
import pandas as pd
from silver.staged_to_cleaned_sales import staged_to_cleaned_sales
from silver.staged_to_cleaned_countries import staged_to_cleaned_countries


def load_data_from_staging_to_cleaned_tables(target_date):

    for table in ['sales', 'countries']:
        query = f"SELECT * FROM {table} where date(business_date)= %s"
        db_config = get_db_config()
        conn = psycopg2.connect(**db_config)
        
        try:
            df = pd.read_sql(query, con=conn, params=(target_date,))
                
            if table == 'sales':
                cleaned_df, quarantined_df = staged_to_cleaned_sales(df, list(df.columns))
            elif table == 'countries':
                cleaned_df, quarantined_df = staged_to_cleaned_countries(df)

            data_ingestion(quarantined_df, list(quarantined_df.columns), f'rejected_{table}', db_config)
            data_ingestion(cleaned_df, list(cleaned_df.columns), f'cleaned_{table}', db_config)

        finally:
            conn.close()

    





    

