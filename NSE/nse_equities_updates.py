import pandas as pd
import glob
import re
from datetime import datetime
import os
from bs4 import BeautifulSoup
import sqlite3
import psycopg2
from sqlalchemy import create_engine

def extract_date_from_filename(filename):
    match = re.search(r'([A-Za-z]+ \d{1,2}, \d{4})', filename)
    if not match:
        raise ValueError(f"No date found in filename: {filename}")
    return datetime.strptime(match.group(1), "%B %d, %Y").date()


def extract_price_table(html_file):
    # Read HTML
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # DEBUG: Print the HTML structure
    print(f"DEBUG: Looking for table in {html_file}")
    
    # Find ALL text that might contain our target
    all_texts = soup.find_all(string=True)
    for text in all_texts:
        if 'price' in text.lower() and 'list' in text.lower():
            print(f"DEBUG: Found text: {text.strip()[:100]}...")
    
    # Find ALL tables
    all_tables = soup.find_all('table')
    print(f"DEBUG: Found {len(all_tables)} total tables")
    
    # Try each table
    for i, table in enumerate(all_tables):
        try:
            # Try to parse it as a DataFrame
            test_df = pd.read_html(str(table))[0]
            print(f"DEBUG: Table {i} has shape: {test_df.shape}")
            print(f"DEBUG: Table {i} columns: {list(test_df.columns)}")
            if len(test_df) > 5 and len(test_df.columns) >= 4:
                print(f"DEBUG: Table {i} might be the price table!")
                price_table = table
                break
        except:
            continue
    
    if not price_table:
        raise ValueError(f"Price table not found in {html_file}")
    
    # Convert to DataFrame
    df = pd.read_html(str(price_table))[0]
    
    # Clean column names
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(filter(None, col)).strip() 
                     for col in df.columns.values]
    
    # Add trade date
    df["trade_date"] = extract_date_from_filename(os.path.basename(html_file))
    
    return df

def main():
    html_files = glob.glob("*.html")

    if not html_files:
        raise RuntimeError("No HTML files found in current directory.")

    print(f"Found {len(html_files)} HTML files")

    all_days = []

    for file in html_files:
        try:
            print(f"Processing: {file}")
            df = extract_price_table(file)
            all_days.append(df)
            print(f"  ✓ Extracted {len(df)} rows")
        except Exception as e:
            print(f"  ✗ ERROR: {e}")

    final_df = pd.concat(all_days, ignore_index=True)
    final_df.sort_values("trade_date", inplace=True)
    print(final_df)
    final_df.columns=["code",
    "name",
    "low_12m",
    "high_12m",
    "low_day",
    "high_day",
    "price",
    "previous",
    "change_abs",
    "change_pct",
    "direction",
    "volume",
    "adjusted_price",
    "trade_date"]

    #define garbage patterns 
    SECTOR_PATTERNS = (
    "Agricultural|"
    "Automobiles and Accessories|"
    "Banking|"
    "Commercial and Services|"
    "Construction and Allied|"
    "Energy and Petroleum|"
    "Insurance|"
    "Investment$|"
    "Investment Services|"
    "Manufacturing and Allied|"
    "Telecommunication|"
    "Real Estate Investment Trusts|"
    "Exchange Traded Funds|"
    "Indices")

    ADS_PATTERN = r"Discover more \(adsbygoogle=.*?\)"

    #drop non security rows
    final_df = final_df[
    final_df["code"].notna() &
    ~final_df["code"].str.contains(SECTOR_PATTERNS, case=False, na=False) &
    ~final_df["code"].str.contains(ADS_PATTERN, regex=True, na=False)]

    #normalize nulls
    final_df=final_df.replace(["-", "", " ", "NA", "N/A"], pd.NA)

    #numeric conversions 
    numeric_cols = [
    "low_12m",
    "high_12m",
    "low_day",
    "high_day",
    "price",
    "previous",
    "change_abs",
    "volume",
    "adjusted_price"]


    for col in numeric_cols:
        final_df[col] = pd.to_numeric(final_df[col], errors="coerce")
    final_df["change_pct"] = pd.to_numeric(
    final_df["change_pct"].str.replace("%", "", regex=False),
    errors="coerce")

    final_df = final_df[
    final_df["price"].notna() &
    final_df["name"].notna()]

    #date formatting
    final_df["trade_date"] = pd.to_datetime(final_df["trade_date"], errors="coerce")
    final_df = (final_df.sort_values(["trade_date", "code"]).reset_index(drop=True))
    print(final_df)
    print("THIS SHOULD RETURN CLEANED ROWS")

    #rename columns
    final_df = final_df.rename(columns={
    "code": "ticker",
    "low_day": "low",
    "high_day": "high",
    "price": "closing_price",
    "name":"company_name"})

    #remove some columns
    cols_to_drop = [
    "low_12m",
    "high_12m",
    "change_abs",
    "change_pct",
    "direction",
    "adjusted_price",
    "previous"]
    final_df = final_df.drop(columns=cols_to_drop, errors="ignore")
    print(final_df)
    print("THIS TEST SHOULD RETURN RENAMED COLUMNS AND TRUE FINAL")

  
    TABLE_NAME = "nse_ke_daily_ohlcv"

    for col in ['low', 'high', 'closing_price', 'volume']:
        final_df[col] = pd.to_numeric(final_df[col], errors='coerce')
    
    db_connection_string = "postgresql://fahad:589Aupgradez2BdfK@localhost:5432/africanfinance_db"
    engine = create_engine(db_connection_string)

    
    final_df.to_sql("nse_ke_daily_ohlcv", engine, if_exists="append", index=False)

    print(f"✅ Data loaded into {TABLE_NAME} successfully!")


if __name__ == "__main__":
    main()

