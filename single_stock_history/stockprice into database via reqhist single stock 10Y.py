import pyodbc
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

import threading
import time
from datetime import datetime
import pandas as pd

class IBapi(EWrapper, EClient):
    def __init__(self, connection, cursor):
        EClient.__init__(self, self)
        self.connection = connection
        self.cursor = cursor

    def historicalData(self, reqId, bar):
        date_value = datetime.strptime(bar.date[:8], '%Y%m%d').date()
        data = (contract.symbol, date_value, bar.close)
        self.cursor.execute("INSERT INTO historical_data_correct (symbol, datum, close) VALUES (?, ?, ?)", data)
        self.connection.commit()

def run_loop():
    app.run()

# Connect to MS Access database
conn_str = (r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\temp\test.accdb')
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
app = IBapi(connection=conn, cursor=cursor)
app.connect('127.0.0.1', 7496, 6) # Connect to IB Gateway or TWS on port 7496. Make sure these are running before executing the script.

api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()


query = "SELECT ib_symbol, ib_currency FROM asset_rollup_data WHERE ib_symbol = 'RAND';" # # Execute a select query
df = pd.read_sql(query, conn) # Read data into a pandas DataFrame
symbols = df.values.tolist() # Convert DataFrame to a list
print(symbols) # Display the list

# List of symbols manually
#symbols = [['SNOW','USD']]

for symbol in symbols:
    contract = Contract()
    contract.symbol = symbol[0]
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = symbol[1]
    
    # Request historical candles
    print(symbol[0], symbol[1])
    app.reqHistoricalData(1 , contract, '', '10 Y', '1 day', 'MIDPOINT', 1, 1, False, [])
    time.sleep(2)  # Sleep to allow enough time for data to be returned
    

time.sleep(2)  # Sleep to allow enough time for data to be returned

app.disconnect()

# Close the database connection
conn.close()

print("Historical data saved to MS Access database")