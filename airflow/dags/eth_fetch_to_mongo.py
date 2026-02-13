import requests
from pymongo import MongoClient
from datetime import datetime

ETHERSCAN_API_KEY = "VFTXASY731TPD79NZZKSRT972QGGZPGEE1"
BASE_URL = "https://api.etherscan.io/v2/api"
CHAIN_ID = 1  # Ethereum Mainnet

def fetch_blocks_to_mongo():
    """Fetch latest Ethereum blocks and transactions using Etherscan V2, store in MongoDB"""
    try:
        # latest block number
        r = requests.get(BASE_URL, params={
            "chainid": CHAIN_ID,
            "module": "block",
            "action": "getblocknobytime",
            "timestamp": int(datetime.utcnow().timestamp()),
            "closest": "before",
            "apikey": ETHERSCAN_API_KEY
        }).json()

        latest_block = int(r.get("result", 0))
        if latest_block == 0:
            print(" Failed to fetch latest block")
            return

    except Exception as e:
        print(" Error fetching latest block:", e)
        return

    # Connect to MongoDB
    mongo = MongoClient("mongodb://mongodb:27017")
    db = mongo["eth_db"]
    blocks_col = db["blocks"]
    tx_col = db["transactions"]

    # Fetch last 10 blocks
    for block_number in range(latest_block, latest_block - 10, -1):
        try:
            block_hex = hex(block_number)
            block_data = requests.get(BASE_URL, params={
                "chainid": CHAIN_ID,
                "module": "proxy",
                "action": "eth_getBlockByNumber",
                "tag": block_hex,
                "boolean": "true",
                "apikey": ETHERSCAN_API_KEY
            }).json().get("result")

            if not block_data:
                continue

            # Store block
            block_doc = {
                "block_number": block_number,
                "miner": block_data.get("miner"),
                "timestamp": datetime.utcfromtimestamp(int(block_data.get("timestamp", "0"), 16)),
                "tx_count": len(block_data.get("transactions", [])),
                "gas_used": int(block_data.get("gasUsed", "0"), 16),
                "fetched_at": datetime.utcnow()
            }
            blocks_col.update_one({"block_number": block_number}, {"$set": block_doc}, upsert=True)

            # Store first 20 transactions
            for tx in block_data.get("transactions", [])[:20]:
                tx_doc = {
                    "hash": tx.get("hash"),
                    "block_number": block_number,
                    "from": tx.get("from"),  # Safe even if missing
                    "to": tx.get("to"),
                    "value_eth": int(tx.get("value", "0"), 16) / 1e18,
                    "gas": int(tx.get("gas", "0"), 16),
                    "gas_price": int(tx.get("gasPrice", "0"), 16),
                    "fetched_at": datetime.utcnow()
                }
                tx_col.update_one({"hash": tx_doc["hash"]}, {"$set": tx_doc}, upsert=True)

        except Exception as e:
            print(f"Error processing block {block_number}: {e}")

    mongo.close()
    print("atest blocks & transactions stored in MongoDB")
