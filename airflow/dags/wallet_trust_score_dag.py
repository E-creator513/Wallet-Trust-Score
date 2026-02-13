from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from eth_fetch_to_mongo import fetch_blocks_to_mongo
from eth_mongo_to_postgres import mongo_to_postgres
import psycopg2
import pandas as pd
import networkx as nx
from sklearn.ensemble import IsolationForest

# -----------------------------
# Incremental Wallet Trust Score
# -----------------------------
def incremental_wts():
    pg = psycopg2.connect(
        host="postgres",
        database="analytics",
        user="airflow",
        password="airflow"
    )

    # get last processed timestamp
    cur = pg.cursor()
    cur.execute("SELECT MAX(last_updated) FROM wallet_scores;")
    last_time = cur.fetchone()[0] or datetime(2026, 1, 1)

    # fetch only new txs
    df = pd.read_sql(
        f"SELECT from_address, to_address, value_eth, gas, block_number FROM transactions WHERE fetched_at > '{last_time}'",
        pg
    )
    if df.empty:
        print("No new transactions to score.")
        cur.close()
        pg.close()
        return

    # Outgoing features
    out_features = df.groupby('from_address').agg({
        'value_eth': ['sum','mean','std'],
        'gas': ['sum','mean'],
        'to_address': 'nunique'
    }).fillna(0)
    out_features.columns = ['_'.join(col) for col in out_features.columns]
    out_features.reset_index(inplace=True)
    out_features.rename(columns={'from_address':'wallet'}, inplace=True)

    # Incoming features
    in_features = df.groupby('to_address').agg({
        'value_eth': ['sum','mean','std'],
        'from_address': 'nunique'
    }).fillna(0)
    in_features.columns = ['in_' + '_'.join(col) for col in in_features.columns]
    in_features.reset_index(inplace=True)
    in_features.rename(columns={'to_address':'wallet'}, inplace=True)

    wallets = pd.merge(out_features, in_features, on='wallet', how='outer').fillna(0)

    # Graph features
    G = nx.from_pandas_edgelist(df, 'from_address', 'to_address', ['value_eth'], create_using=nx.DiGraph())
    wallets['pagerank'] = wallets['wallet'].apply(lambda x: nx.pagerank(G).get(x, 0))
    wallets['degree'] = wallets['wallet'].apply(lambda x: G.degree(x) if x in G else 0)

    # Isolation Forest for anomaly/WTS scoring
    feature_cols = [c for c in wallets.columns if c != 'wallet']
    model = IsolationForest(contamination=0.01, random_state=42)
    wallets['wts_score'] = -model.fit_predict(wallets[feature_cols])
    wallets['wts_score'] = ((wallets['wts_score'] - wallets['wts_score'].min()) /
                            (wallets['wts_score'].max() - wallets['wts_score'].min()) * 100).round(2)

    # Store scores incrementally
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_scores (
            wallet TEXT PRIMARY KEY,
            wts NUMERIC,
            last_updated TIMESTAMP DEFAULT NOW()
        );
    """)
    for _, row in wallets.iterrows():
        cur.execute("""
            INSERT INTO wallet_scores (wallet, wts, last_updated)
            VALUES (%s, %s, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                wts = EXCLUDED.wts,
                last_updated = NOW();
        """, (row['wallet'], row['wts_score']))

    pg.commit()
    cur.close()
    pg.close()
    print(f"✅ {len(wallets)} wallets updated with incremental WTS.")




