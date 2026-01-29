IAM PARTICULARLY VERY PASSIONATE ABOUT QUANTITATIVE FINANCE AND WAS HEAVILY INTERESTED IN DEVELOPING MODELS FOR AFRICAN FINANCIAL MARKETS
A KEY PROBLEM THOUGH IS UNRELIABLE DATA , YOU CAN'T JUST YFINANCE YOUR WAY IN😂. CURRENTLY I HAVE DATA PIPELINES THAT SCRAPE API END POINTS AND SOME SCRAPE HTML FILES TO OBTAIN DATA, SO ALOT OF IT CURRENTLY IS DATA ACQUISTION, CLEANING AND STORING TO A DATABASE
SINCE THE DATABASE IM CURRENTLY USING IS SQLITE3 ITS BASICALLY USELESS FOR ANYONE WHO TAKES MY PIPELINES, UNLESS OFCOURSE THEY REPLACE THE END DATABASE BUT SOON ILL SHIFT TO A POSTGRESQL SERVER AND THE SCRIPTS SHALL BE USABLE FOR ANYONE WHO COPIES THE REPO.
THE NSE DATAPIPELINE THOUGH IS NOT REPLICABLE SINCE THERE IS A MANUAL PROCESS THAT I CAN'T AUTOMATE.
THE OTHER PIPELINES THOUGH ARE FUNCTIIONAL.
I PLAN TO ADD DATA FROM
1. FUNDAMENTAL DATA FROM ANNUAL AND QUARTERLY REPORTS PIPELINES
2. MACRO DATA STATISTICS PIPELINES
3. CORPORATE ACTIONS PIPELINES
4. ANALYSES AND MODELS IN THE FUTURE.

THOUGH REALISTICALLY THE PIPELINES WOULD HAVE SO MUCH MANUAL PROCESSES AND SO IT WOULD BE THE DATA MOSTLY VALUABLE.
I'VE ALREADY MIGRATED TO POSTGRES SO I CAN CONTINUE DATA COLLECTING AND BUILDING PIPELINES 

THE DIFFERENT PATHS
1. The data collection:
    Currently the data pipeline focuses on getting data from api endpoints and html pages scrapes,
    you'll be capable of getting daily updates for 
    a) DSE(The Dar-es-salaam Stock exchange) in Tanzania
        The are end point in path DSE/data/datalinks.csv. 
        The script DSE/scripts/dse_equities_updates.py will scrape the endpoints and store it directly into the database in your local host. I personally used postgresql but you could use sqlite3 for simplicity.
         For either users change the destinated database here
         def fetch_and_extract_latest_data():
         .............
         .............
                     db_connection_string = "postgresql://fahad:589Aupgradez2BdfK@localhost:5432/africanfinance_db"
        just change that to your local postgres database that you should create beforehand. and yes it's not a good idea to leave your password like i did so maybe store it in a separate file only available in your local system. I did so for simplicity.
        So just run the code at night and it will fetch the latest day data per available ticker.

    b) NSE(Nairobi stock exchange) in Kenya
        This gets data from a data provider that sadly is paid and the datapipelines works for me so be sure to just pass by this ill eventually share this data on some google sheets.
    
    c) JSE(johanneburg Stock Exchange) in South Africa
        Due to this being in the top 20 largest stock exchanges in the world. It does have alot of coverage so i used the yfinance library that does get the data automatically to your database. it will prompt a date ,use date in YYYY-MM-DD system as in 2/March/2026 would be 2026-03-02. Now sit back and relax as it loads the data straight to your database.
        its run by JSE/jse_scripts/jse_equities.py also JSE/jse_scripts/jse_indices.py for the stock indices
        For the either users change the database here
            db_connection_string = "postgresql://fahad:589Aupgradez2BdfK@localhost:5432/africanfinance_db"
        
    d) BRVM (Bourse Régionale des Valeurs Mobilières) its a coalition exchange for Niger, Ivory Coast etc
        For this im scraping the official site.
        You are still required to change the database input.
        You initially have to to run BRVM/scripts/brvm_page.py 
        then run BRVM/scripts/brvm_equities_updates.py

    TO AVOID THIS BACK AND FORTH YOU CAN JUST RUN main_update_pipeline,py
    BE SURE TO COMMENT THE run_script() or delete the line and maybe delete the run_sql_file() or comment. But as soon as you do the modifications you can run the main_update_pipeline.py script at 7:00PM EAT to get the data straight to your database except for nse


2. The data consolidation:




    

