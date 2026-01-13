import subprocess
import time 
import pandas as pd

start_time = time.time()

# Set the start date for the date range
start_date = pd.Timestamp('today').date() - pd.Timedelta(days=7)

print(start_date)
date_range = pd.date_range(start=start_date, end=pd.Timestamp('today'))

# Convert the date to a string in YYYY-MM-DD format for the script
start_date_str = str(start_date)
days_range = len(date_range)
db_path = r"C:\Users\onno\OneDrive\Beleggen\2025 - portefeuille database 02.03 - STOCKDATA.accdb"


# Run multiple scripts sequentially

subprocess.run(
    ['python', '1 A - stockprice ibkr fetch 1.1 - optimized.py', str(days_range)],
    cwd=r'C:\python_coding\database_scripts\daily_update\result_per_dag_update'
)
subprocess.run(['python', '1 B - stockprice merge into historical data correct.py'],cwd=r'C:\python_coding\database_scripts\daily_update\result_per_dag_update')
subprocess.run(['python', '1 C - delete temp stock price table.py'],cwd=r'C:\python_coding\database_scripts\daily_update\result_per_dag_update')
stock_price_time = time.time()


print(f"Time taken to run the script: {stock_price_time - start_time} seconds, for stockprices insert naar historicat_data_correct, script 1, A, B, C")



