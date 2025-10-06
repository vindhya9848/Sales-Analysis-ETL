import pandas as pd

from bronze.data_ingestion_raw import data_ingestion_raw
from config.get_db_config import get_db_config

def load_data_from_source(file_path, target_date):

    """ Ingests data from excel sheets: and stores it as a dataframe which is later loaded into 
         the staged tables: sales and countries
    """

    sheets = ['Sales', 'Country map']
    db_config = get_db_config()

    # Map sheet names to target table names (adjust as needed)
    sheet_table_map = {
        'Sales': 'sales',
        'Country map': 'countries'
    }

    for sheet in sheets:
        df = pd.read_excel(file_path, sheet_name=sheet)
        columns = list(df.columns)
        table_name = sheet_table_map.get(sheet, sheet.lower().replace(" ", "_"))  # fallback table name formatting

        print(f"Ingesting sheet '{sheet}' into table '{table_name}' with columns: {columns}")


        data_ingestion_raw(df, columns, table_name, db_config, target_date)
