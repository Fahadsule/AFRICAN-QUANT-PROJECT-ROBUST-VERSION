import pandas as pd
import yfinance
import sqlite3

import yfinance as yf
import pandas as pd
import sqlite3

# 1️⃣ Load your tickers CSV
tickers_df = pd.read_csv("JSE/data/jse_list.csv")  # your CSV with a column 'ticker'

# Clean column name just in case
tickers_df.columns = tickers_df.columns.str.strip().str.lower()
tickers = tickers_df["ticker"].tolist()

# 2️⃣ Ask user for the date
input_date = input("Enter the date to fetch (YYYY-MM-DD): ").strip()
fetch_date = pd.Timestamp(input_date)

# 3️⃣ Prepare list to collect today’s data
today_data = []
missing_data = []  # NEW: to track tickers with missing data

# 4️⃣ Loop through each ticker
for ticker in tickers:
    try:
        print(f"Downloading {ticker} for {input_date}...")
        df = yf.download(
            ticker,
            start=input_date,
            end=(fetch_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=False,
            progress=False
        )
        df = df[df.index.date == fetch_date.date()]
        if df.empty:
            print(f"No data for {ticker} on {input_date}")
            missing_data.append({"ticker": ticker, "missing_date": input_date})  # ADD
            continue

        df.reset_index(inplace=True)
        
        # Flatten multi-index if exists
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        # Add ticker column
        df["ticker"] = ticker
        
        # Rename columns
        df.rename(columns={
            "Date": "trade_date",
            "Open": "opening_price",
            "High": "high",
            "Low": "low",
            "Close": "closing_price",
            "Adj Close": "adj_close",
            "Volume": "volume"
        }, inplace=True)

        # Drop adj_close since you don’t need it
        if "adj_close" in df.columns:
            df = df.drop(columns=["adj_close"])

        today_data.append(df)

    except Exception as e:
        print(f"Failed {ticker}: {e}")
        missing_data.append({"ticker": ticker, "missing_date": input_date})  # ADD

# 5️⃣ Combine all tickers into one DataFrame
if today_data:
    master_df = pd.concat(today_data, ignore_index=True)
    desired_order = ['trade_date', 'ticker', 'opening_price', 'high', 'low', 'closing_price', 'volume']
    master_df = master_df[desired_order]
    
    print(f"✅ Data saved for JSE_data_{input_date}")
else:
    print("❌ No data found for any ticker on that date.")

# 6️⃣ Save missing tickers
if missing_data:
    missing_df = pd.DataFrame(missing_data)
    missing_df.to_csv(f"missing_JSE_data_{input_date}.csv", index=False)
    print(f"⚠️  Missing data saved to missing_JSE_data_{input_date}.csv")
else:
    print("🎉 No missing tickers, all data downloaded successfully.")


conn = sqlite3.connect("market_data.db")
master_df.to_sql("jse_sa_daily_ohlcv", conn, if_exists="append", index=False)
print("jse updated for_{input_date}")
