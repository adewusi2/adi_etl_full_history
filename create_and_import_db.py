import sqlite3
import pandas as pd
import os

# Set file paths
csv_file = "adi_stock_data.csv"
db_path = "data/stocks.db"
table_name = "adi_stock"

# Ensure the data directory exists
os.makedirs(os.path.dirname(db_path), exist_ok=True)

# Load CSV into a DataFrame
try:
    df = pd.read_csv(csv_file)
    print(f"✅ Loaded CSV: {csv_file}")
except FileNotFoundError:
    print(f"❌ Error: File not found: {csv_file}")
    exit(1)

# Connect to SQLite database (creates if it doesn't exist)
conn = sqlite3.connect(db_path)
print(f"📂 Connected to database: {db_path}")

# Write DataFrame to table
try:
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"📥 Data imported into table: {table_name}")
except Exception as e:
    print(f"❌ Error writing to database: {e}")
finally:
    conn.close()
    print("🔒 Database connection closed.")
