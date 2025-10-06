import pandas as pd
import psycopg
import numpy as np

def data_ingestion_unique(df, columns, table_name, db_config, unique_keys):
    """
     This method is used to ingest unique data to table: meaning it only inserts
     records that do not exist in the table already based on unique keys: (combination of columns to determine uniqueness)

     Returns:
     cleaned_df: Dataframe with unique records
     quarantined_df: Dataframe with records alredy existing in the table
    """
    print("--------------------Executing data_ingestion_unique--------------------")
    df = df.where(pd.notnull(df), None)
    df = df.replace({np.nan: None})

    conn_str = (
        f"host={db_config['host']} port={db_config['port']} dbname={db_config['dbname']} "
        f"user={db_config['user']} password={db_config['password']}"
    )

    # Query template to check existence
    where_clause = ' AND '.join([f"{col} = %s" for col in unique_keys])
    select_query = f"SELECT 1 FROM {table_name} WHERE {where_clause} LIMIT 1"

    cleaned_rows = []
    quarantined_rows = []

    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            for row in df[columns].itertuples(index=False, name=None):
                key_values = [row[columns.index(k)] for k in unique_keys]
                cur.execute(select_query, key_values)
                if cur.fetchone() is None:
                    cleaned_rows.append(row)
                else:
                    quarantined_rows.append(row)

    # Convert lists back to DataFrames
    cleaned_df = pd.DataFrame(cleaned_rows, columns=columns)

    df_quarantined = pd.DataFrame(quarantined_rows, columns=columns)
    if not df_quarantined.empty:
        df_quarantined['reason'] = 'Value already exists in the table'

    # print quarantined_df columns:
    if not df_quarantined.empty:
        print("------------quarantined df is not  empty", df_quarantined.columns.tolist())

    df_quarantined=df_quarantined.drop_duplicates(subset=unique_keys, keep='first')

    return cleaned_df, df_quarantined
