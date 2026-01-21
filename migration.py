import sqlite3
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from tqdm import tqdm
import gc
from sqlalchemy import create_engine  # ADD THIS

def migrate_table_large(table_name, sqlite_conn, pg_conn, batch_size=10000):
    """Migrate large tables in batches"""
    print(f"Processing {table_name}...")
    
    # Get total rows
    total_rows = pd.read_sql_query(f'SELECT COUNT(*) as cnt FROM "{table_name}"', sqlite_conn)['cnt'][0]
    print(f"  Total rows: {total_rows:,}")
    
    if total_rows == 0:
        print(f"  Skipping empty table")
        return
    
    # Get schema
    schema = pd.read_sql_query(f"PRAGMA table_info('{table_name}')", sqlite_conn)
    
    # Create PostgreSQL table
    pg_cursor = pg_conn.cursor()
    
    # Drop if exists
    pg_cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    
    # Create table with mapped types
    columns = []
    type_map = {
        'INTEGER': 'BIGINT',
        'REAL': 'DOUBLE PRECISION',
        'TEXT': 'TEXT',
        'BLOB': 'BYTEA',
        'NUMERIC': 'NUMERIC',
        'BOOLEAN': 'BOOLEAN',
        'DATE': 'DATE',
        'DATETIME': 'TIMESTAMP'
    }
    
    for _, row in schema.iterrows():
        sqlite_type = row['type'].upper().split('(')[0] if row['type'] else 'TEXT'
        pg_type = type_map.get(sqlite_type, 'TEXT')
        nullable = '' if row['notnull'] else 'NULL'
        columns.append(f'"{row["name"]}" {pg_type} {nullable}')
    
    create_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns)})'
    pg_cursor.execute(create_sql)
    pg_conn.commit()
    
    # Migrate in batches
    offset = 0
    pbar = tqdm(total=total_rows, desc=f"  Migrating {table_name}")
    
    while offset < total_rows:
        # Read batch
        df_batch = pd.read_sql_query(
            f'SELECT * FROM "{table_name}" LIMIT {batch_size} OFFSET {offset}',
            sqlite_conn
        )
        
        if df_batch.empty:
            break
        
        # Prepare data
        col_names = [f'"{col}"' for col in df_batch.columns]
        placeholders = ', '.join(['%s'] * len(col_names))
        insert_sql = f'INSERT INTO "{table_name}" ({", ".join(col_names)}) VALUES ({placeholders})'
        
        # Convert to list of tuples
        records = [tuple(row) for row in df_batch.itertuples(index=False, name=None)]
        
        # Batch insert
        execute_batch(pg_cursor, insert_sql, records)
        pg_conn.commit()
        
        offset += batch_size
        pbar.update(len(df_batch))
        
        # Clean up memory
        del df_batch, records
        gc.collect()
    
    pbar.close()
    pg_cursor.close()
    print(f"  ✅ Completed {table_name}")

# Main execution
if __name__ == "__main__":
    sqlite_path = "db/market_data.db"  # UPDATE: Added db/ prefix
    sqlite_conn = sqlite3.connect(sqlite_path)
    
    # FIX: Create SQLAlchemy engine for pandas queries
    pg_engine = create_engine('postgresql://fahad:589Aupgradez2BdfK@localhost/africanfinance_db')
    
    # Keep psycopg2 connection for batch inserts
    pg_conn = psycopg2.connect(
        host="localhost",
        database="africanfinance_db",
        user="fahad",
        password="589Aupgradez2BdfK",
        port=5432
    )
    
    # Get all tables from SQLite
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", sqlite_conn)
    print(f"Found {len(tables)} tables in SQLite")
    
    # FIX: Use SQLAlchemy engine for this query
    existing_tables = pd.read_sql("SELECT tablename FROM pg_tables WHERE schemaname='public'", pg_engine)['tablename'].tolist()
    print(f"Found {len(existing_tables)} tables already in PostgreSQL")
    
    tables_to_migrate = [t for t in tables['name'] if t not in existing_tables]
    print(f"\nNeed to migrate {len(tables_to_migrate)} tables:")
    for table in tables_to_migrate:
        print(f"  • {table}")
    
    # Migrate tables
    for table in tables_to_migrate:
        migrate_table_large(table, sqlite_conn, pg_conn)
    
    # Cleanup
    sqlite_conn.close()
    pg_conn.close()
    pg_engine.dispose()
    print("✨ Migration complete!")