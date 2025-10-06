import warnings
import psycopg
from datetime import datetime
import pandas as pd
import numpy as np


def data_ingestion(df, columns, table_name, db_config):
    """ this method is used to insert data into the database table"""

    # to handle missing text or numeric values for all: nan,na,None to just : None
    df = df.where(pd.notnull(df), None)
    df = df.replace({np.nan: None})

    #prepare connection string
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
                cur.execute(query, row)
      #          print(f"Inserted row into {table_name}: {row}")
        conn.commit()
    
    print(f"Data loaded successfully into '{table_name}' table, {len(df)} rows inserted.")
