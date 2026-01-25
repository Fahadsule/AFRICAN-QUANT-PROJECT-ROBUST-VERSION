import pandas as pd
import psycopg2
from sqlalchemy import create_engine

db_connection_string = "postgresql://fahad:589Aupgradez2BdfK@localhost:5432/africanfinance_db"
engine = create_engine(db_connection_string)

def format_dates(file_path, columns):
    df=pd.read_csv(file_path)
    for column in columns:
    #convert date format to date
        df[column] = pd.to_datetime(df[column], dayfirst=True, format='%d-%b-%Y')
        df[column] = df[column].dt.strftime('%Y-%m-%d')
    return df

test_df=format_dates("CORPORATE_ACTIONS/nse_dividends.csv",["announcement_date" ,"record_date" ,"pay_date"])

def main():
    finals=[]
    file_paths=["CORPORATE_ACTIONS/nse_dividends.csv","CORPORATE_ACTIONS/nse_distributions.csv","CORPORATE_ACTIONS/nse_bonus_issues.csv","CORPORATE_ACTIONS/nse_rights.csv"]
    for file_path in file_paths:
        if file_path=="CORPORATE_ACTIONS/nse_dividends.csv":
            table_name="nse_corporate_actions_dividends"
            columns=["announcement_date" ,"record_date" ,"pay_date"]
        elif file_path=="CORPORATE_ACTIONS/nse_distributions.csv":
            table_name="nse_corporate_actions_distributions"
            columns=["announcement_date","record_date","pay_date"]
        elif file_path=="CORPORATE_ACTIONS/nse_bonus_issues.csv":
            table_name="nse_corporate_actions_bonus"
            columns=["announcement_date","book_closure_date","credit_date"]
        elif file_path=="CORPORATE_ACTIONS/nse_rights.csv":
            table_name="nse_corporate_actions_bonus"
            columns=["announcement_date","book_closure_date","credit_date"]
        df=format_dates(file_path, columns)
        df.to_sql(table_name, engine, if_exists="append", index=False)
        

if __name__=="__main__":
    main()

    

    
            



    
    
    
    



