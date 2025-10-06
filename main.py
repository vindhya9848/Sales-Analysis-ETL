
from datetime import datetime
from bronze.load_data_from_source import load_data_from_source

from gold.execute_sql_scripts_with_date import execute_sql_scripts_with_date
from silver.load_data_from_staging_to_cleaned_tables import load_data_from_staging_to_cleaned_tables
from pathlib import Path

def main():
 

 file_path = r'.\Data_for_programming_20251006.xlsx' # Excel file name as: # Data_for_programming_YYYYMMDD.xlsx

 # file name must be appended with business date
 try:
     date_str = file_path.split('_')[-1].split('.')[0]  # Extract '20251006 YYYYMMDD' part
     target_date = datetime.strptime(date_str, '%Y%m%d').date()
 except ValueError:
    print("Error: The file name does not contain a valid date in the expected format (YYYYMMDD).")

 load_data_from_source(file_path, target_date) #target_date is the business_date for which we want the records from the dataset

 load_data_from_staging_to_cleaned_tables(target_date)

 sql_dir = Path('./metrics_and_views')

 sql_files = sorted(f.name for f in sql_dir.glob('*.sql'))

 if not sql_files:
    print("No SQL files found in the folder.")

 execute_sql_scripts_with_date(sql_files, sql_dir, target_date)
 
if __name__ == "__main__":
    main()
