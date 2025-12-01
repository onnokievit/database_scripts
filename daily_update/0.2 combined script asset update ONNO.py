import subprocess
import time 
import pandas as pd

start_time = time.time()

# Set the start date for the date range
start_date = pd.Timestamp('today').date() - pd.Timedelta(days=10)

print(start_date)
date_range = pd.date_range(start=start_date, end=pd.Timestamp('today'))

# Convert the date to a string in YYYY-MM-DD format for the script
start_date_str = str(start_date)
db_path = r"C:\Users\onno\OneDrive\Beleggen\2025 - portefeuille database 02.03 - ONNO.accdb"


# Run multiple scripts sequentially

# subprocess.run(['python', '1 A - stockprice ibkr fetch 1.1 - optimized.py'])
# subprocess.run(['python', '1 B - stockprice merge into historical data correct.py'])
# subprocess.run(['python', '1 C - delete temp stock price table.py'])
stock_price_time = time.time()

# Optie prijzen ophalen van IBKR (optioneel - uncomment om te activeren)
# subprocess.run(['python', '2 A - optionprice ibkr fetch 1.0.py', '--db', db_path],cwd=r'C:\python_coding\database_scripts\daily_update\result_per_dag_update')
option_price_time = time.time()

subprocess.run(['python', '3 asset_berekening_aandelen 1.5.5.py','--date', start_date_str,'--db',db_path],cwd=r'C:\python_coding\database_scripts\daily_update\result_per_dag_update')

asset_berekening_aandelen_time = time.time()
subprocess.run(['python', '4 asset_berekening_opties open tabel. 1.3.py', '--date', start_date_str,'--db',db_path],cwd=r'C:\python_coding\database_scripts\daily_update\result_per_dag_update')
opties_open_time_deel1 = time.time()
subprocess.run(['python', '5 asset_berekening_optie open to result table 2.2.py', '--date', start_date_str,'--db',db_path],cwd=r'C:\python_coding\database_scripts\daily_update\result_per_dag_update')
opties_open_time_deel2 = time.time()
subprocess.run(['python', '6 asset_berekening_optie closed to result table  2.1.py', '--date', start_date_str,'--db',db_path],cwd=r'C:\python_coding\database_scripts\daily_update\result_per_dag_update')
opties_closed_time = time.time()
subprocess.run(['python', '7 asset_berekening_sprinters 1.6.py', '--date', start_date_str,'--db',db_path],cwd=r'C:\python_coding\database_scripts\daily_update\result_per_dag_update')
sprinters_time = time.time()
subprocess.run(['python', '8 asset berekening dividend 1.1.py', '--date', start_date_str,'--db',db_path],cwd=r'C:\python_coding\database_scripts\daily_update\result_per_dag_update')
dividend_time = time.time()

end_time = time.time()

# Calculate the time taken
elapsed_time = end_time - start_time

print(f"Time taken to run the script: {stock_price_time - start_time} seconds, for stockprices insert naar historicat_data_correct, script 1, A, B, C")
print(f"Time taken to run the script: {option_price_time - stock_price_time} seconds, for optie prijzen ophalen van IBKR, script 2 A")
print(f"Time taken to run the script: {asset_berekening_aandelen_time - option_price_time} seconds, for aandeel insert naar per_dag_asset_result")
print(f"Time taken to run the script: {opties_open_time_deel1 - asset_berekening_aandelen_time} seconds, for optie_open hulptabel bijwerken")
print(f"Time taken to run the script: {opties_open_time_deel2 - opties_open_time_deel1} seconds, for opties_open insert naar per_dag_asset_result")
print(f"Time taken to run the script: {opties_closed_time - opties_open_time_deel2} seconds, for opties_closed insert naar per_dag_asset_result")
print(f"Time taken to run the script: {sprinters_time - opties_closed_time} seconds, for sprinter_open insert naar per_dag_asset_result")
print(f"Time taken to run the script: {dividend_time- sprinters_time} seconds, for dividend tabel bijwerken")

print(f"Time taken to run the script: {elapsed_time:.2f} seconds")


