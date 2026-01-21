import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import glob
import sqlite3
import os
import psycopg2
from sqlalchemy import create_engine

def extract_brvm_table_with_date(html_path):
    with open(html_path, 'r', encoding='utf-8') as file:
        soup = BeautifulSoup(file, 'html.parser')
    
    # Get trade date
    header_seance = soup.find('p', class_='header-seance')
    trade_date = "Unknown Date"
    if header_seance:
        date_part = header_seance.text.strip().split('-')[0].strip()
        try:
            trade_date = datetime.strptime(date_part, "%A, %d %B, %Y").strftime("%Y-%m-%d")
        except:
            trade_date = date_part
    
    # Extract table data
    data_rows = []
    table = soup.find('section', id='block-system-main').find('table')
    
    if table:
        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            if len(cells) >= 7:
                data_rows.append({
                    'trade_date': trade_date,
                    'ticker': cells[0].text.strip(),
                    'company_name': cells[1].text.strip(),
                    'volume': cells[2].text.strip(),
                    'opening_price': cells[4].text.strip(),
                    'closing_price': cells[5].text.strip()
                })
    
    return pd.DataFrame(data_rows)

# Process all HTML files starting with "brvm_stocks_"
html_files = glob.glob("brvm_stocks_*.html")

if not html_files:
    print("No brvm_stocks_*.html files found in current directory")
    df = pd.DataFrame()
else:
    print(f"Found {len(html_files)} BRVM HTML file(s):")
    for file in html_files:
        print(f"  - {file}")
    
    all_dfs = []

    for html_file in html_files:
        print(f"\nProcessing: {html_file}")
        df = extract_brvm_table_with_date(html_file)
        if len(df) > 0:
            all_dfs.append(df)
            print(f"✓ Extracted {len(df)} stocks")

    # Combine and clean data
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        
        # Clean numeric columns
        df['volume'] = df['volume'].str.replace(' ', '').astype(int)
        df['opening_price'] = df['opening_price'].str.replace(' ', '').str.replace(',', '.').astype(float)
        df['closing_price'] = df['closing_price'].str.replace(' ', '').str.replace(',', '.').astype(float)
        
        # Sort
        df = df.sort_values(['trade_date', 'ticker'])
        
        print(f"\n✅ Extracted {len(df)} records")
        db_connection_string = "postgresql://fahad:589Aupgradez2BdfK@localhost:5432/africanfinance_db"
        engine = create_engine(db_connection_string)

        df.to_sql("brvm_daily_ohlcv", engine, if_exists="append", index=False)
        print("DATA ADDED TO POSTGRESQL DATABASE✅✅✅")

    else:
        print("No data extracted")
        df = pd.DataFrame()
for html_file in glob.glob("brvm_stocks_*.html"):
    os.remove(html_file)
    print(f"🗑️ Deleted: {html_file}")