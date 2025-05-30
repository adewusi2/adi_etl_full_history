import os
from datetime import datetime
import yfinance as yf
import pandas as pd
import sqlite3

# Constants
DB_PATH = "data/stocks.db"
TABLE_NAME = "adi_stock"
CSV_EXPORT_PATH = "adi_stock_data_updated.csv"

# Step 1: Ensure directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def get_last_date_from_db():
    """Fetch the last date in the database table."""
    if not os.path.exists(DB_PATH):
        return None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(Date) FROM {TABLE_NAME}")
        result = cursor.fetchone()
        conn.close()
        if result and result[0]:
            return result[0][:10]  # Return as YYYY-MM-DD
    except Exception as e:
        print(f"⚠️ Error fetching last date from DB: {e}")
    return None

def extract_new_data(ticker="ADI", start_date=None):
    """Download new stock data from Yahoo Finance."""
    if start_date:
        start_date = (pd.to_datetime(start_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        start_date = "1980-01-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    df = yf.download(ticker, start=start_date, end=end_date)
    df.reset_index(inplace=True)
    return df

def transform_data(df):
    """Clean and standardize the data."""
    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
    df.dropna(inplace=True)
    return df

def append_data_to_db(df):
    """Append the new data to the database."""
    conn = sqlite3.connect(DB_PATH)
    df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
    conn.close()

def export_to_csv(df):
    """Export the updated data to CSV."""
    df.to_csv(CSV_EXPORT_PATH, index=False)

if __name__ == "__main__":
    print("🚀 Starting daily ETL update...")
    last_date = get_last_date_from_db()
    print(f"📅 Last date in database: {last_date if last_date else 'None'}")

    new_data = extract_new_data(start_date=last_date)
    if not new_data.empty:
        cleaned = transform_data(new_data)
        append_data_to_db(cleaned)
        export_to_csv(cleaned)
        print(f"✅ Added {len(cleaned)} new rows to database and exported to CSV.")
    else:
        print("✅ No new data to add today.")
