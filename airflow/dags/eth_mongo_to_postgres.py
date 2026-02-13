from pymongo import MongoClient
import psycopg2
from datetime import datetime

def mongo_to_postgres():
    mongo = MongoClient("mongodb://mongodb:27017")
    db = mongo["eth_db"]
    blocks_col = db["blocks"]
    tx_col = db["transactions"]

    pg = psycopg2.connect(
        host="postgres",
        database="analytics",
        user="airflow",
        password="airflow"
    )
    cur = pg.cursor()

    # ----------------------------
    # Blocks table
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS blocks (
            block_number BIGINT PRIMARY KEY,
            miner TEXT,
            timestamp TIMESTAMP,
            tx_count INT,
            gas_used BIGINT,
            fetched_at TIMESTAMP
        );
    """)
    cur.execute("TRUNCATE TABLE blocks;")  # clear old data safely

    for b in blocks_col.find():
        cur.execute("""
            INSERT INTO blocks (block_number, miner, timestamp, tx_count, gas_used, fetched_at)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING;
        """, (
            b.get("block_number"),
            b.get("miner"),
            b.get("timestamp"),
            b.get("tx_count"),
            b.get("gas_used", 0),
            b.get("fetched_at", datetime.utcnow())
        ))

    # ----------------------------
    # Transactions table
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            hash TEXT PRIMARY KEY,
            block_number BIGINT,
            from_address TEXT,
            to_address TEXT,
            value_eth NUMERIC,
            gas BIGINT,
            gas_price NUMERIC,
            fetched_at TIMESTAMP
        );
    """)
    cur.execute("TRUNCATE TABLE transactions;")  # clear old data safely

    for t in tx_col.find():
        cur.execute("""
            INSERT INTO transactions (hash, block_number, from_address, to_address, value_eth, gas, gas_price, fetched_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING;
        """, (
            t.get("hash"),
            t.get("block_number"),
            t.get("from_address"),
            t.get("to_address"),
            t.get("value_eth", 0),
            t.get("gas", 0),
            t.get("gas_price", 0),
            t.get("fetched_at", datetime.utcnow())
        ))

    pg.commit()
    cur.close()
    pg.close()
    mongo.close()

    print("✅ Data loaded into Postgres safely (tables truncated, views intact)")
