import psycopg
from datetime import datetime
import pandas as pd
import numpy as np

def data_ingestion_raw(df, columns, table_name, db_config, target_date):
    """
   Insert records for specic business_date from excel sheet to staged tables: (sales,countries)
   adding business_date column to the dataframe for auditing purpose and facilitating incremental data loading.
   
    """
    # to insert null values as None
    df = df.where(pd.notnull(df), None)
    df = df.replace({np.nan: None})

    # Add business_date for auditing purpose 
    if 'business_date' not in columns:
        df['business_date'] = target_date
        # df['business_date'] = datetime.now()
        columns.append('business_date')


    # Prepare connection string
    conn_str = (
        f"host={db_config['host']} port={db_config['port']} dbname={db_config['dbname']} "
        f"user={db_config['user']} password={db_config['password']}"
    )
    
    # Prepare insert query
    cols_str = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
    
    # Insert rows
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            for row in df[columns].itertuples(index=False, name=None):
                try:
                    cur.execute(query, row)
                except Exception as e:
                    print("Failed row:", row)
                    raise e              
      #          print(f"Inserted row into {table_name}: {row}")
        conn.commit()
    
    print(f"Data loaded successfully into '{table_name}' table, {len(df)} rows inserted.")
