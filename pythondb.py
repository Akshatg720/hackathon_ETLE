import mysql.connector
from faker import Faker
import random
import json
from datetime import datetime, timedelta

# ---- CONFIGURATION ----
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'testdb'
}

# ---- SETUP ----
fake = Faker()

# ---- CONNECT TO MYSQL ----
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# ---- SQL INSERT TEMPLATE ----
insert_query = """
INSERT INTO BillEvent (event_id, SKU_id, AccountSID, `From`, `To`, Description)
VALUES (%s, %s, %s, %s, %s, %s)
"""

# ---- GENERATE AND INSERT 100 ROWS ----
for _ in range(100):
    event_id = f"E{random.randint(1000, 9999)}"
    sku_id = f"SKU{random.randint(1000, 9999)}"
    account_sid = f"AC{random.randint(10000, 99999)}"

    start_time = fake.date_time_between(start_date='-30d', end_date='now')
    end_time = start_time + timedelta(hours=random.randint(1, 5))

    description = {
        "type": random.choice(["usage", "billing", "support"]),
        "amount": round(random.uniform(10, 500), 2),
        "currency": random.choice(["USD", "EUR", "INR"]),
        "notes": fake.sentence()
    }

    # Insert into DB
    values = (
        event_id,
        sku_id,
        account_sid,
        start_time,
        end_time,
        json.dumps(description)
    )
    cursor.execute(insert_query, values)

# ---- COMMIT AND CLOSE ----
conn.commit()
cursor.close()
conn.close()

print("✅ Successfully inserted 100 rows into BillEvent!")
