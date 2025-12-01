"""
Script: 2 A - optionprice ibkr fetch 1.0.py

Dit script haalt optieprijzen op van Interactive Brokers (IBKR) voor
openstaande opties in de portefeuille en slaat ze op in een tijdelijke
tabel in de Access database.

Vereisten:
- Interactive Brokers TWS of IB Gateway actief op poort 7496
- Microsoft Access database met tabellen:
  - transacties_bron_data (voor optiegegevens)
  - asset_rollup_data (voor onderliggende waarde info)
"""

from ibapi import wrapper
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.utils import iswrapper
from ibapi.common import TickerId, TickAttrib
from time import sleep, time
import pandas as pd
import pyodbc
import random
import argparse
from datetime import datetime

# -------------------------
# CLI Arguments
# -------------------------
parser = argparse.ArgumentParser(description="Fetch option prices from IBKR")
parser.add_argument("--db", type=str, required=True, help="Pad naar Access database")
args = parser.parse_args()

# -------------------------
# Access connection
# -------------------------
db_path = args.db
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + db_path

# -------------------------
# SQL query voor openstaande opties
# -------------------------
sql_query_open_options = """
    SELECT DISTINCT 
        t.asset_rollup,
        t.optie_strike,
        t.optie_call_put,
        t.optie_exp_date,
        a.ib_symbol,
        a.ib_currency,
        a.exchange
    FROM transacties_bron_data t
    INNER JOIN asset_rollup_data a ON t.asset_rollup = a.asset_rollup
    WHERE t.asset_type = 'optie'
    AND t.optie_exp_date >= Date()
"""

# -------------------------
# Pacing / status codes
# -------------------------
PACING_ERRORS = {162, 321, 366}

INFO_STATUS_CODES = {
    2103, 2104, 2105, 2106, 2107, 2108,
    2158, 2159,
    1100, 1101, 1102
}

# -------------------------
# Windowed concurrency
# -------------------------
MAX_IN_FLIGHT = 1
pending = set()
next_idx = 0

# -------------------------
# Accumulator for option prices
# -------------------------
rows = []  # list of tuples: (asset_rollup, strike, call_put, exp_date, bid, ask, last, close)

# will be loaded in main()
all_options = pd.DataFrame()


def _safe_str(x):
    return "" if pd.isna(x) else str(x).strip()


def _format_expiry(exp_date) -> str:
    """Convert expiry date to IBKR format YYYYMMDD."""
    if pd.isna(exp_date):
        return ""
    if isinstance(exp_date, str):
        return exp_date.replace("-", "")[:8]
    return exp_date.strftime('%Y%m%d')


def _right_from_call_put(call_put: str) -> str:
    """Convert 'call'/'put' to 'C'/'P'."""
    if not call_put:
        return ""
    return "C" if call_put.lower() == "call" else "P"


class OptionApp(wrapper.EWrapper, EClient):
    def __init__(self):
        wrapper.EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.retry_counts = {}
        self._ready = False
        self.option_data = {}  # reqId -> {bid, ask, last, close}

    @iswrapper
    def tickPrice(self, reqId: TickerId, tickType: int, price: float, attrib: TickAttrib):
        """Handle price tick data."""
        if reqId not in self.option_data:
            self.option_data[reqId] = {'bid': None, 'ask': None, 'last': None, 'close': None}

        # TickType: 1=bid, 2=ask, 4=last, 9=close
        if tickType == 1:
            self.option_data[reqId]['bid'] = price
        elif tickType == 2:
            self.option_data[reqId]['ask'] = price
        elif tickType == 4:
            self.option_data[reqId]['last'] = price
        elif tickType == 9:
            self.option_data[reqId]['close'] = price

    @iswrapper
    def tickSnapshotEnd(self, reqId: int):
        """Called when snapshot data is complete."""
        super().tickSnapshotEnd(reqId)

        # Get the option data and save it
        if reqId in self.option_data and reqId < len(all_options):
            row = all_options.loc[reqId]
            data = self.option_data[reqId]

            rows.append((
                row['asset_rollup'],
                row['optie_strike'],
                row['optie_call_put'],
                row['optie_exp_date'],
                data.get('bid'),
                data.get('ask'),
                data.get('last'),
                data.get('close')
            ))
            print(f"Received option data for {row['asset_rollup']} "
                  f"{row['optie_call_put']} {row['optie_strike']} "
                  f"exp {row['optie_exp_date']}: "
                  f"bid={data.get('bid')}, ask={data.get('ask')}, "
                  f"last={data.get('last')}, close={data.get('close')}")

        self.retry_counts.pop(reqId, None)
        if reqId in pending:
            pending.discard(reqId)

        kick_off_more(self)

        if not pending and next_idx >= len(all_options):
            print("All option data processed. Disconnecting...")
            self.disconnect()

    @iswrapper
    def nextValidId(self, orderId: int):
        self._ready = True
        print(f"nextValidId: {orderId} -> starting requests...")
        kick_off_more(self)

    @iswrapper
    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson="", *args):
        if reqId == -1 and errorCode in INFO_STATUS_CODES:
            return

        print(f"ERROR {reqId} {errorCode} {errorString}")

        # Contract/currency problems: skip and move on
        if errorCode in {200, 406, 10168}:
            self.retry_counts.pop(reqId, None)
            if reqId >= 0 and reqId in pending:
                pending.discard(reqId)
            kick_off_more(self)
            if not pending and next_idx >= len(all_options):
                print("All requests completed (after skipping bad contracts). Disconnecting...")
                self.disconnect()
            return

        # Pacing/farm issues: single retry after brief backoff
        if errorCode in PACING_ERRORS:
            count = self.retry_counts.get(reqId, 0)
            if count < 1:
                self.retry_counts[reqId] = count + 1
                print("Pacing/farm issue. Backing off 15 seconds and retrying...")
                sleep(15)
                send_request_for_index(self, reqId)
                return
            else:
                print("Pacing/farm issue persisted after retry. Skipping this option.\n")
                self.retry_counts.pop(reqId, None)
                if reqId >= 0 and reqId in pending:
                    pending.discard(reqId)
                kick_off_more(self)
                if not pending and next_idx >= len(all_options):
                    print("All requests completed. Disconnecting...")
                    self.disconnect()
                return

        # Unexpected error: drop and proceed
        self.retry_counts.pop(reqId, None)
        if reqId >= 0 and reqId in pending:
            pending.discard(reqId)
        kick_off_more(self)
        if not pending and next_idx >= len(all_options):
            print("All requests completed. Disconnecting...")
            self.disconnect()

    @iswrapper
    def connectionClosed(self):
        print("Connection closed.")


def send_request_for_index(app: OptionApp, idx: int):
    """Submit one market data request for option at all_options row idx."""
    row = all_options.loc[idx]

    contract = Contract()
    contract.symbol = _safe_str(row.get('ib_symbol'))
    contract.secType = 'OPT'
    contract.exchange = _safe_str(row.get('exchange')) or "SMART"
    contract.currency = _safe_str(row.get('ib_currency'))
    contract.lastTradeDateOrContractMonth = _format_expiry(row.get('optie_exp_date'))
    contract.strike = float(row.get('optie_strike', 0))
    contract.right = _right_from_call_put(_safe_str(row.get('optie_call_put')))
    contract.multiplier = "100"  # Standard option multiplier

    print(f"Requesting data for {contract.symbol} {contract.right} "
          f"{contract.strike} exp {contract.lastTradeDateOrContractMonth}")

    # Request snapshot market data
    app.reqMktData(
        idx,        # reqId
        contract,
        "",         # genericTickList
        True,       # snapshot (get single snapshot, don't stream)
        False,      # regulatorySnapshot
        []          # mktDataOptions
    )


def kick_off_more(app: OptionApp):
    """Maintain up to MAX_IN_FLIGHT concurrent market data requests."""
    global next_idx
    if not getattr(app, "_ready", False):
        return

    while len(pending) < MAX_IN_FLIGHT and next_idx < len(all_options):
        reqId = next_idx
        pending.add(reqId)
        send_request_for_index(app, reqId)
        next_idx += 1
        sleep(0.5)  # Small delay between requests


def save_df_to_access_temp(df: pd.DataFrame, conn_str: str):
    """
    Creates temp_option_prices in Access if it doesn't exist,
    then inserts all rows from df (append).
    """
    if df.empty:
        print("No option data to write to Access.")
        return

    df_to_write = df.copy()
    df_to_write['datum'] = pd.Timestamp.now().normalize()

    # Ensure correct column order
    columns_order = ['datum', 'asset_rollup', 'optie_strike', 'optie_call_put',
                     'optie_exp_date', 'bid', 'ask', 'last', 'close_price']
    df_to_write = df_to_write.rename(columns={'close': 'close_price'})
    df_to_write = df_to_write[columns_order]

    # Convert datetime to Python datetime objects
    df_to_write['datum'] = df_to_write['datum'].dt.to_pydatetime()[0]
    if pd.api.types.is_datetime64_any_dtype(df_to_write['optie_exp_date']):
        df_to_write['optie_exp_date'] = df_to_write['optie_exp_date'].dt.to_pydatetime()

    temp_table_name = "temp_option_prices"
    create_temp_table_query = f"""
    CREATE TABLE {temp_table_name} (
        datum DATE,
        asset_rollup TEXT(255),
        optie_strike DOUBLE,
        optie_call_put TEXT(10),
        optie_exp_date DATE,
        bid DOUBLE,
        ask DOUBLE,
        last DOUBLE,
        close_price DOUBLE
    )
    """
    placeholders = ", ".join(["?"] * len(columns_order))
    insert_query = f"INSERT INTO {temp_table_name} ({', '.join(columns_order)}) VALUES ({placeholders})"

    data = [tuple(row) for row in df_to_write.to_numpy()]

    try:
        with pyodbc.connect(conn_str) as conn:
            cur = conn.cursor()

            # Try to create table — if exists, ignore
            try:
                cur.execute(create_temp_table_query)
                conn.commit()
                print(f"Table {temp_table_name} created.")
            except pyodbc.Error:
                print(f"Table {temp_table_name} already exists, skipping creation.")

            # Insert all rows (append)
            cur.executemany(insert_query, data)
            conn.commit()
            print(f"Inserted {len(data)} rows into {temp_table_name}.")

    except pyodbc.Error as e:
        print("Error writing temp table to Access:", e)


def main():
    global all_options, next_idx
    t0 = time()

    # -------- Load open options from Access --------
    with pyodbc.connect(conn_str) as connection:
        all_options = pd.read_sql(sql_query_open_options, connection)

    if all_options.empty:
        print("No open options found in database.")
        return

    # Reset to index starting at 0
    all_options = all_options.reset_index(drop=True)

    print(f"Found {len(all_options)} open options:")
    print(all_options.head(10))
    print()

    # Reset window state
    pending.clear()
    next_idx = 0
    rows.clear()

    # -------- Start IB connection --------
    app = OptionApp()
    client_id = random.randint(1, 10000)
    app.connect("127.0.0.1", 7496, clientId=client_id)
    print(f"Using client ID: {client_id}")
    print("serverVersion:%s connectionTime:%s" % (app.serverVersion(), app.twsConnectionTime()))

    app.run()

    # -------- Build DataFrame from rows --------
    all_option_prices_df = pd.DataFrame(
        rows,
        columns=['asset_rollup', 'optie_strike', 'optie_call_put',
                 'optie_exp_date', 'bid', 'ask', 'last', 'close']
    )

    print("All requests completed.")
    print("Collected option prices:", len(all_option_prices_df))
    print(all_option_prices_df.head())

    # -------- Append DataFrame to Access temp table --------
    save_df_to_access_temp(all_option_prices_df, conn_str)

    print(f"Total elapsed: {time() - t0:.2f}s")


if __name__ == "__main__":
    main()
