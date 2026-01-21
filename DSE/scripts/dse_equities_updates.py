import pandas as pd
import requests
from datetime import datetime
import time
import json
from typing import Optional, Dict, Any
import random
import sqlite3
import psycopg2
from sqlalchemy import create_engine


def fetch_with_persistent_retry(url: str, max_retries: int = 50, initial_delay: float = 2.0) -> Optional[Dict[str, Any]]:
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate',
    }
    
    last_status_code = None
    last_error = None
    response_time = None
    
    for attempt in range(max_retries + 1):
        try:
            print(f"   Attempt {attempt + 1}/{max_retries + 1}...", end=" ")
            
            # Add jitter to prevent thundering herd
            delay = initial_delay * (1 ** attempt) + random.uniform(0, 1)
            
            # Only wait after the first attempt
            if attempt > 0:
                print(f"Waiting {delay:.1f}s before retry...")
                time.sleep(delay)
            else:
                print()
            
            start_time = time.time()
            
            # Make the request with timeout
            response = requests.get(
                url,
                headers=headers,
                timeout=30,  # Increased timeout
                verify=True
            )
            
            response_time = time.time() - start_time
            last_status_code = response.status_code
            
            # Check HTTP status
            if response.status_code != 200:
                print(f"   ⚠️  HTTP {response.status_code} - Retrying...")
                continue
            
            # Try to parse JSON
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                print(f"   ⚠️  Invalid JSON - Retrying...")
                last_error = f"JSON decode error: {str(e)}"
                continue
            
            # Check if we got valid data (array or dict with data key)
            if isinstance(data, list):
                if len(data) == 0:
                    print(f"   ⚠️  Empty array returned - Retrying...")
                    last_error = "Empty data array"
                    continue
                
                print(f"   ✅ Success! Got {len(data)} records in {response_time:.2f}s")
                return {
                    'data': data,
                    'status_code': response.status_code,
                    'response_time': response_time,
                    'attempts': attempt + 1,
                    'raw_response': response
                }
            
            elif isinstance(data, dict):
                # Check for common data keys
                data_keys = ['data', 'results', 'items', 'records']
                for key in data_keys:
                    if key in data and isinstance(data[key], list) and len(data[key]) > 0:
                        print(f"   ✅ Success! Got {len(data[key])} records from '{key}' key in {response_time:.2f}s")
                        return {
                            'data': data[key],
                            'status_code': response.status_code,
                            'response_time': response_time,
                            'attempts': attempt + 1,
                            'raw_response': response
                        }
                
                # If dict contains data directly (not in a list)
                if len(data) > 0:
                    print(f"   ✅ Success! Got dictionary with {len(data)} keys in {response_time:.2f}s")
                    return {
                        'data': [data],  # Wrap in list for consistency
                        'status_code': response.status_code,
                        'response_time': response_time,
                        'attempts': attempt + 1,
                        'raw_response': response
                    }
                else:
                    print(f"   ⚠️  Empty dictionary - Retrying...")
                    last_error = "Empty dictionary"
                    continue
            
            else:
                print(f"   ⚠️  Unexpected data type: {type(data)} - Retrying...")
                last_error = f"Unexpected data type: {type(data)}"
                continue
                
        except requests.exceptions.Timeout:
            print(f"   ⚠️  Timeout - Retrying...")
            last_error = "Request timeout"
            continue
        except requests.exceptions.ConnectionError as e:
            print(f"   ⚠️  Connection error: {str(e)[:50]}... - Retrying...")
            last_error = f"Connection error: {str(e)[:50]}"
            continue
        except requests.exceptions.RequestException as e:
            print(f"   ⚠️  Request error: {str(e)[:50]}... - Retrying...")
            last_error = f"Request error: {str(e)[:50]}"
            continue
        except Exception as e:
            print(f"   ⚠️  Unexpected error: {str(e)[:50]}... - Retrying...")
            last_error = f"Unexpected error: {str(e)[:50]}"
            continue
    
    # If we get here, all retries failed
    print(f"   ❌ All {max_retries + 1} attempts failed")
    return {
        'data': None,
        'status_code': last_status_code,
        'response_time': response_time,
        'attempts': max_retries + 1,
        'error': last_error,
        'url': url
    }

def fetch_and_extract_latest_data():

    try:
        # Step 1: Read the datalinks.csv file
        print("📂 Reading datalinks.csv...")
        links_df = pd.read_csv('DSE/data/datalinks.csv')
        
        # Auto-detect URL column
        url_column = None
        for col in links_df.columns:
            if any(keyword in col.lower() for keyword in ['url', 'link', 'api', 'endpoint']):
                url_column = col
                break
        
        if url_column is None:
            # Use first column if no URL column found
            url_column = links_df.columns[0]
            print(f"⚠️  No URL column detected, using first column: '{url_column}'")
        
        urls = links_df[url_column].dropna().unique().tolist()
        print(f"✅ Found {len(urls)} URLs to process")
        
        # Step 2: Process each URL
        all_latest_rows = []
        failed_links = []
        
        # Statistics
        total_attempts = 0
        total_response_time = 0
        
        for i, url in enumerate(urls, 1):
            print(f"\n{'='*60}")
            print(f"🔗 Processing URL {i}/{len(urls)}")
            print(f"📌 URL: {url[:100]}..." if len(url) > 100 else f"📌 URL: {url}")
            
            # Fetch data with persistent retry
            result = fetch_with_persistent_retry(url, max_retries=49)  # 50 total attempts max
            
            if result['data'] is None:
                print(f"   ❌ Failed to fetch valid data after {result['attempts']} attempts")
                failed_links.append({
                    'url': url,
                    'error': result.get('error', 'Unknown error'),
                    'status_code': result.get('status_code', 'N/A'),
                    'attempts': result['attempts'],
                    'timestamp': datetime.now()
                })
                continue
            
            # Update statistics
            total_attempts += result['attempts']
            total_response_time += result.get('response_time', 0)
            
            try:
                # Process the data
                data = result['data']
                
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                    print(f"   📊 Processing {len(data)} records...")
                    
                elif isinstance(data, dict):
                    # Convert single dict to DataFrame with one row
                    df = pd.DataFrame([data])
                    print(f"   📊 Processing 1 record (dictionary format)...")
                else:
                    print(f"   ⚠️  Unexpected data format after retry: {type(data)}")
                    failed_links.append({
                        'url': url,
                        'error': f'Unexpected data format: {type(data)}',
                        'status_code': result.get('status_code', 'N/A'),
                        'attempts': result['attempts'],
                        'timestamp': datetime.now()
                    })
                    continue
                
                # Find date column (case-insensitive)
                date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
                
                if not date_columns:
                    # Try more specific patterns
                    date_patterns = ['trade_date', 'timestamp', 'created_at', 'updated_at', 'date_time']
                    for pattern in date_patterns:
                        if pattern in df.columns:
                            date_columns = [pattern]
                            break
                
                if not date_columns:
                    print(f"   ⚠️  No date column found in data")
                    print(f"   Available columns: {list(df.columns)}")
                    
                    # Try to save anyway without date extraction
                    latest_row = df.iloc[-1].copy()  # Use last row if no date
                    latest_row['source_url'] = url
                    latest_row['data_fetched_at'] = datetime.now()
                    latest_row['total_records'] = len(df)
                    latest_row['fetch_attempts'] = result['attempts']
                    latest_row['fetch_time_seconds'] = result.get('response_time', 0)
                    
                    all_latest_rows.append(latest_row)
                    print(f"   📝 Saved record (no date column, used last row)")
                    continue
                
                date_column = date_columns[0]
                
                try:
                    # Convert date column to datetime
                    df[date_column] = pd.to_datetime(df[date_column], errors='coerce', utc=True)
                    
                    # Check if any valid dates exist
                    valid_dates = df[date_column].dropna()
                    if len(valid_dates) == 0:
                        print(f"   ⚠️  No valid dates found in '{date_column}' column")
                        print(f"   Sample values: {df[date_column].head(3).tolist()}")
                        
                        # Save last record anyway
                        latest_row = df.iloc[-1].copy()
                        latest_row['source_url'] = url
                        latest_row['data_fetched_at'] = datetime.now()
                        latest_row['total_records'] = len(df)
                        latest_row['fetch_attempts'] = result['attempts']
                        latest_row['fetch_time_seconds'] = result.get('response_time', 0)
                        
                        all_latest_rows.append(latest_row)
                        print(f"   📝 Saved record (invalid dates, used last row)")
                        continue
                    
                    # Find the row with the latest date
                    latest_idx = df[date_column].idxmax()
                    latest_row = df.loc[latest_idx].copy()
                    
                    # Add metadata about the source
                    latest_row['source_url'] = url
                    latest_row['data_fetched_at'] = datetime.now()
                    latest_row['total_records'] = len(df)
                    latest_row['fetch_attempts'] = result['attempts']
                    latest_row['fetch_time_seconds'] = result.get('response_time', 0)
                    latest_row['date_column_used'] = date_column
                    
                    all_latest_rows.append(latest_row)
                    
                    latest_date = latest_row[date_column]
                    print(f"   📅 Extracted latest record: {latest_date}")
                    print(f"   ⚡ Fetch stats: {result['attempts']} attempts, {result.get('response_time', 0):.2f}s")
                    
                except Exception as e:
                    print(f"   ❌ Error processing dates: {e}")
                    
                    # Save last record even with date error
                    latest_row = df.iloc[-1].copy()
                    latest_row['source_url'] = url
                    latest_row['data_fetched_at'] = datetime.now()
                    latest_row['total_records'] = len(df)
                    latest_row['fetch_attempts'] = result['attempts']
                    latest_row['fetch_time_seconds'] = result.get('response_time', 0)
                    latest_row['date_error'] = str(e)
                    
                    all_latest_rows.append(latest_row)
                    print(f"   📝 Saved record (with date error, used last row)")
                    continue
                
                # Add a small delay between URLs to be respectful
                time.sleep(0.5)
                
            except Exception as e:
                print(f"   ❌ Error processing data: {e}")
                failed_links.append({
                    'url': url,
                    'error': f'Data processing error: {str(e)}',
                    'status_code': result.get('status_code', 'N/A'),
                    'attempts': result['attempts'],
                    'timestamp': datetime.now()
                })
                continue
        
        # Step 3: Save successful results to database
        if all_latest_rows:
            # Create DataFrame from all latest rows
            result_df = pd.DataFrame(all_latest_rows)
            
            # Reorder columns for better readability
            meta_cols = ['source_url', 'data_fetched_at', 'total_records', 
                        'fetch_attempts', 'fetch_time_seconds']
            
            # Add any other metadata columns
            other_meta_cols = [col for col in result_df.columns 
                             if col not in meta_cols and any(x in col.lower() 
                             for x in ['date_column', 'date_error', 'fetch'])]
            
            all_meta_cols = meta_cols + other_meta_cols
            other_cols = [col for col in result_df.columns if col not in all_meta_cols]
            
            result_df = result_df[all_meta_cols + other_cols]

            # Convert to date
            result_df["trade_date"] = pd.to_datetime(result_df["trade_date"]).dt.date

            # Remove some columns and rename some - FIX: Changed from set {} to list []
            result_df = result_df[["trade_date", "company", "volume", "high", "low", "opening_price", "closing_price"]]
            result_df.rename(columns={"company": "ticker"}, inplace=True)

            # Converting numeric columns
            num_cols = ["volume", "high", "low", "opening_price", "closing_price"]
            for col in num_cols:
                result_df[col] = pd.to_numeric(result_df[col], errors="coerce")  # turns invalid values to NaN

            db_connection_string = "postgresql://fahad:589Aupgradez2BdfK@localhost:5432/africanfinance_db"
            engine = create_engine(db_connection_string)
            

            # Append to table
            result_df.to_sql("dse_tz_daily_ohlcv", engine, if_exists="append", index=False)

            
            print("Data appended to dse_daily_ohlcv successfully!")

            
            print(f"\n{'='*60}")
            print(f"✅ SUCCESS! Saved {len(result_df)} latest records to database")
            print(f"📊 Successful URLs: {len(result_df)}")
            print(f"🎯 Total fetch attempts: {total_attempts}")
            print(f"⏱️  Total response time: {total_response_time:.2f}s")
            
            # Show statistics
            if len(result_df) > 0:
                avg_attempts = total_attempts / len(result_df)
                avg_time = total_response_time / len(result_df)
                print(f"📈 Average per URL: {avg_attempts:.1f} attempts, {avg_time:.2f}s")
            
            # Show sample of saved data - Note: source_url column was removed above
            date_cols = [col for col in result_df.columns if 'date' in col.lower()]
            if date_cols:
                date_col = date_cols[0]
                print(f"\n📋 Sample of latest records:")
                for idx, row in result_df.head(3).iterrows():
                    # Since source_url was removed, just show the ticker
                    if 'ticker' in row:
                        print(f"   • Ticker: {row['ticker']}")
                    if date_col in row and pd.notna(row[date_col]):
                        print(f"     Latest date: {row[date_col]}")
        else:
            print(f"\n⚠️  No successful data extractions.")
        
        # Step 4: Save failed links to failed_links.csv
        if failed_links:
            failed_df = pd.DataFrame(failed_links)
            
            # Reorder columns for better readability
            failed_cols = ['url', 'error', 'attempts', 'timestamp']
            if 'status_code' in failed_df.columns:
                failed_cols.insert(2, 'status_code')
            
            failed_df = failed_df.reindex(columns=failed_cols)
            failed_df.to_csv('failed_links.csv', index=False)
            
            print(f"\n⚠️  Saved {len(failed_df)} failed URLs to 'failed_links.csv'")
            print(f"📊 Failure rate: {len(failed_links)}/{len(urls)} URLs ({len(failed_links)/len(urls)*100:.1f}%)")
            
            # Show error summary
            print(f"\n📋 Error summary:")
            error_counts = failed_df['error'].value_counts()
            for error, count in error_counts.head(5).items():
                print(f"   • {error[:80]}{'...' if len(error) > 80 else ''}: {count} URLs")
            
            if len(error_counts) > 5:
                print(f"   • ... and {len(error_counts) - 5} other error types")
        
        # Summary report
        print(f"\n{'='*60}")
        print("📈 FINAL REPORT")
        print(f"   Total URLs processed: {len(urls)}")
        print(f"   Successful extractions: {len(all_latest_rows)}")
        print(f"   Failed URLs: {len(failed_links)}")
        print(f"   Total fetch attempts: {total_attempts}")
        print(f"   Total execution time: {total_response_time:.2f}s")
        
        if all_latest_rows:
            print(f"\n✅ Data saved to database successfully")
        if failed_links:
            print(f"⚠️  Failed URLs saved to: failed_links.csv")
        
        return True
        
    except FileNotFoundError:
        print("❌ ERROR: 'datalinks.csv' file not found.")
        print("Please ensure the file exists in the same directory.")
        print("\nExpected format:")
        print("url")
        print("https://api.example.com/data1")
        print("https://api.example.com/data2")
        return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def create_example_csv():
    """Create an example datalinks.csv if it doesn't exist"""
    example_data = {
        'api_endpoint': [
            'https://api.dse.co.tz/api/market-data/statistics?companyId=11&days=365',
            'https://api.dse.co.tz/api/market-data/statistics?companyId=12&days=365',
            'https://api.dse.co.tz/api/market-data/statistics?companyId=999&days=365',
            'https://jsonplaceholder.typicode.com/posts',
            'https://api.dse.co.tz/api/market-data/statistics?companyId=13&days=365'
        ]
    }
    
    df = pd.DataFrame(example_data)
    df.to_csv('datalinks.csv', index=False)
    print("📝 Created example datalinks.csv with 5 sample URLs")


if __name__ == "__main__":
    # Check if datalinks.csv exists
    import os
    
    print("="*70)
    print("🔗 API Data Extractor v3.0 - Persistent Retry Edition")
    print("="*70)
    
    if not os.path.exists('DSE/data/datalinks.csv'):
        print("datalinks.csv not found. Creating example file...")
        create_example_csv()
        print("\nPlease edit datalinks.csv with your actual URLs,")
        print("then run the script again.")
    else:
        # Run the main function
        start_time = time.time()
        success = fetch_and_extract_latest_data()
        total_time = time.time() - start_time
        
        print(f"\n{'='*70}")
        print(f"✨ Script execution complete!")
        print(f"⏱️  Total script runtime: {total_time:.2f} seconds")
        print("="*70)