import subprocess
import sqlite3
import time
import os

def run_script(script_name):
    print(f"\n=== Running {script_name} ===")
    start = time.time()
    subprocess.run(["python", script_name], check=True)
    print(f"Finished {script_name} in {time.time() - start:.2f} seconds\n")


def run_sql_file(db_path, sql_file):
    print(f"\n=== Running SQL from {sql_file} ===")
    with open(sql_file, "r") as f:
        sql = f.read()
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(sql)
        conn.commit()
        print(f"Finished SQL script {sql_file}")
    except Exception as e:
        print("SQL execution failed:", e)
    finally:
        conn.close()

if __name__=="__main__":
    DB_PATH="db/market_data.db"
    run_script("DSE/scripts/dse_equities_updates.py")
    run_sql_file(DB_PATH,"DSE/scripts/autofill_dse.sql")
    run_script("NSE/nse_equities_updates.py")
    run_sql_file(DB_PATH,"NSE/autofill_nse.sql")
    run_script("JSE/jse_scripts/jse_equities_updates.py")
    run_script("JSE/jse_scripts/jse_indices_updates.py")
    run_sql_file(DB_PATH,"JSE/jse_scripts/autofill_jse.sql")
    run_script("BRVM/scripts/brvm_page.py")
    run_script("BRVM/scripts/brvm_equities_updates.py")