import os
from dotenv import load_dotenv

#load_dotenv('/opt/airflow/config.env') # comment when running local script: main.py

def get_db_config():
 #   path= '/opt/airflow/config.env'  #comment when running local script: main.py
    path ='.\config\config.env'
    if not os.path.exists(path):
        raise FileNotFoundError(f"Configuration file not found at {path}")
    with open(path) as f:
      for line in f:
        if line.strip() and not line.startswith('#'):
            k, v = line.strip().split('=', 1)
            os.environ[k] = v

    db_config = {
    'host': os.environ['DB_HOST'],
    'port': int(os.environ['DB_PORT']),
    'dbname': os.environ['DB_NAME'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD']
    }
    
    return db_config

