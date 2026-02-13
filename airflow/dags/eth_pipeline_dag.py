# eth_pipeline_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import pandas as pd
import networkx as nx
from sklearn.ensemble import IsolationForest

# your existing imports
from eth_fetch_to_mongo import fetch_blocks_to_mongo
from eth_mongo_to_postgres import mongo_to_postgres

# --- define incremental_wts inside the DAG file ---
def incremental_wts():
    pg = psycopg2.connect(
        host="postgres",
        database="analytics",
        user="airflow",
        password="airflow"
    )
    # ... rest of the function here ...
    print("✅ WTS task done.")

# ----------------------------- DAG -----------------------------
with DAG(
    dag_id="ethereum_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/2 * * * *",
    catchup=False,
    tags=["ethereum", "blockchain","elt"]
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_eth_to_mongo",
        python_callable=fetch_blocks_to_mongo
    )

    load_task = PythonOperator(
        task_id="mongo_to_postgres",
        python_callable=mongo_to_postgres
    )

    wts_task = PythonOperator(
        task_id="incremental_wallet_trust_score",
        python_callable=incremental_wts
    )

    fetch_task >> load_task >> wts_task
