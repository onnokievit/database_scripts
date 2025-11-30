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

    def check_record_existence(self, symbol, date_value):
        query = "SELECT COUNT(*) FROM historical_data_correct WHERE symbol=? AND datum=?;"
        self.cursor.execute(query, (symbol, date_value))
        count = self.cursor.fetchone()[0]
        return count > 0

    def historicalData(self, reqId, bar):
        date_value = datetime.strptime(bar.date[:8], '%Y%m%d').date()
        symbol = contract.symbol

        if self.check_record_existence(symbol, date_value):
            data = (symbol, date_value, bar.close, symbol, date_value)
            print(data)
            self.cursor.execute("UPDATE historical_data_correct SET symbol=? , datum=? , close=?  WHERE symbol=? AND datum=?", data)
            #self.cursor.execute("INSERT INTO historical_data_import (symbol, datum, close) VALUES (?, ?, ?)", data)
            self.connection.commit()
        if not self.check_record_existence(symbol, date_value):
            data = (symbol, date_value, bar.close)
            print(data)
            self.cursor.execute("INSERT INTO historical_data_correct (symbol, datum, close) VALUES (?, ?, ?)", data)
            self.connection.commit()        


def run_loop():
    app.run()

# Connect to MS Access database
conn_str = (r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\temp\test2.accdb')
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
app = IBapi(connection=conn, cursor=cursor)
app.connect('127.0.0.1', 7496, 6)  # Connect to IB Gateway or TWS on port 7496. Make sure these are running before executing the script.

api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()

query = "SELECT ib_symbol, ib_currency FROM asset_rollup_data;" # WHERE ib_symbol = 'AAPL';"  # Execute a select query
df = pd.read_sql(query, conn)  # Read data into a pandas DataFrame
symbols = df.values.tolist()  # Convert DataFrame to a list

i=1

for symbol in symbols:
    contract = Contract()
    contract.symbol = symbol[0]
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = symbol[1]
    
    # Request historical candles
    print(i, symbol[0], symbol[1])
    app.reqHistoricalData(i, contract, '', '1 W', '1 day', 'MIDPOINT', 1, 1, False, [])
    time.sleep(2)  # Sleep to allow enough time for data to be returned
    i=i+1


app.disconnect()
# Close the database connection
conn.close()

print("Historical data saved to MS Access database")
