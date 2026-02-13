from pymongo import MongoClient
import random
import time
from datetime import datetime

client = MongoClient("mongodb://mongodb:27017")
db = client["iot"]
collection = db["air_quality"]

while True:
    doc = {
        "timestamp": datetime.utcnow(),
        "temperature": round(random.uniform(15, 30), 2),
        "humidity": round(random.uniform(30, 80), 2),
        "pm25": round(random.uniform(5, 100), 2),
    }

    collection.insert_one(doc)
    print("Inserted:", doc)
    time.sleep(5)
