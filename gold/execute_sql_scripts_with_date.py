from pathlib import Path
import psycopg
from config.get_db_config import get_db_config  
import sys
sys.path.append('/opt/airflow') 

def execute_sql_scripts_with_date(sql_files, sql_dir: Path, target_date):
    """
   Executes scripts in a given directory , %s is the place holder present in sql scripts 
   that is replaced by target_date for incremental insertion
    """
    db_config = get_db_config()

    conn_str = (
        f"host={db_config['host']} port={db_config['port']} dbname={db_config['dbname']} "
        f"user={db_config['user']} password={db_config['password']}"
    )

    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            for filename in sql_files:
                file_path = sql_dir / filename
                with open(file_path, "r") as f:
                    sql = f.read()
                    # if filename contains 'table' in the string
                    if "table" in filename.lower():
                        cur.execute(sql, (target_date,)) # since we only want incremental records for that particular business_date in our tables
                        print(f"✅ Executed  script for Table Insertion in gold layer: {filename}")
                    elif 'view' in filename.lower():
                        cur.execute(sql) # creates view on overall snapshot of the table
                        print(f"✅ Executed  script for Views creation in gold layer: {filename}")
        conn.commit()
