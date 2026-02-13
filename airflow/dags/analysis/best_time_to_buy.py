import psycopg2
import pandas as pd


conn = psycopg2.connect(
    host="localhost",
    database="analytics",
    user="airflow",
    password="airflow"
)

query = """
SELECT 
    EXTRACT(DAY FROM depart_date - fetched_at) AS days_before_departure,
    AVG(price) AS avg_price
FROM flight_prices
GROUP BY days_before_departure
ORDER BY avg_price;
"""

df = pd.read_sql(query, conn)

best_row = df.iloc[0]
best_days = int(best_row["days_before_departure"])
best_price = round(best_row["avg_price"], 2)

print(f"📉 Best time to buy tickets: ~{best_days} days before departure")
print(f"💰 Average price at that time: ${best_price}")
