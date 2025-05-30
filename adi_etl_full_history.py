import os
import yfinance as yf
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

# Set absolute path to your project directory
BASE_DIR = "/Users/yusufadewusi/Documents/Python/adi_etl_full_history"
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "stocks.db")
CSV_PATH = os.path.join(BASE_DIR, "adi_stock_data.csv")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)
print("📁 Current working directory:", os.getcwd())
print(f"📌 Saving DB to: {DB_PATH}")

def extract_stock_data(ticker="ADI"):
    stock = yf.Ticker(ticker)
    df = stock.history(period="max", interval="1d")
    df.reset_index(inplace=True)
    return df

def transform_data(df):
    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
    df.dropna(inplace=True)
    return df

def load_data_to_db(df, db_path=DB_PATH, table_name="adi_stock"):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"✅ Data loaded to database: {db_path}")

if __name__ == "__main__":
    # ETL process
    data = extract_stock_data()
    cleaned_data = transform_data(data)
    load_data_to_db(cleaned_data)
    
    # Save CSV
    cleaned_data.to_csv(CSV_PATH, index=False)
    print(f"📄 CSV saved to: {CSV_PATH}")

    # Analysis
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM adi_stock", conn)
    conn.close()
    
    print(df.tail())
    print(df.describe())

    # Plot
    plt.figure(figsize=(12, 6))
    plt.plot(pd.to_datetime(df["Date"], utc=True), df["Close"], label="Close Price", color='blue')
    plt.title("ADI Stock Price (Historical)")
    plt.xlabel("Date")
    plt.ylabel("Price (USD)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
