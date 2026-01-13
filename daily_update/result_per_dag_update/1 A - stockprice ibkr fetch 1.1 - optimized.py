from ibapi import wrapper
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.utils import iswrapper  # Just for decorator
from ibapi.common import BarData
from time import sleep
from time import time
import pandas as pd
import pyodbc
import random
import sys

# -------------------------
# Access connection (READ asset list + WRITE temp table)
# -------------------------
conn_str = (r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\\Users\\onno\\OneDrive\\Beleggen\\2025 - portefeuille database 02.03 - STOCKDATA.accdb')

# Keep the SQL query exactly as requested by you
# sql_query_stock_range = "SELECT * FROM asset_rollup_data WHERE asset_rollup = 'BNP'"
sql_query_stock_range = "SELECT * FROM asset_rollup_data"

# Column used to filter valid rows
currency_column = 'ib_currency'

# -------------------------
# Pacing / status codes
# -------------------------
# Common pacing/farm error codes for historical data
PACING_ERRORS = {162, 321, 366}

# Non-fatal, informational connection/farm status codes (reqId == -1)
INFO_STATUS_CODES = {
    2103, 2104, 2105, 2106, 2107, 2108,  # market/HMDS farm (dis)connected/OK
    2158, 2159,                           # sec-def farm (dis)connected/OK
    1100, 1101, 1102                      # connectivity between IB and TWS
}

# -------------------------
# Windowed concurrency (max in-flight historical requests)
# -------------------------
MAX_IN_FLIGHT = 3  # start veilig; later evt. 2-3
pending = set()      # reqIds currently active
next_idx = 0         # next row index to submit

# -------------------------
# Accumulator for bar rows (faster than per-bar DataFrame concat)
# -------------------------
rows = []  # list of tuples: (date, symbol, asset_rollup, open, high, low, close, volume, wap)

# will be loaded in main()
all_data = pd.DataFrame()


def _safe_str(x):
    return "" if pd.isna(x) else str(x).strip()


def _is_index_symbol(sym: str) -> bool:
    s = (sym or "").upper()
    return s in {"EOE", "AEX", "^AEX"}


def _infer_sec_type(row) -> str:
    """
    Heuristiek:
    - Als 'type' of 'sector' op index duiden → IND
    - Of symbool typisch index is (EOE/AEX) → IND
    - Anders STK
    """
    t = _safe_str(row.get('type', '')).lower()
    sec = _safe_str(row.get('sector', '')).lower()
    sym = _safe_str(row.get('ib_symbol', ''))

    if t in ('index', 'idx', 'ind') or sec == 'index' or _is_index_symbol(sym):
        return 'IND'
    return 'STK'


def _what_to_show(secType: str) -> str:
    return 'TRADES' if secType == 'STK' else 'MIDPOINT'


class TestApp(wrapper.EWrapper, EClient):
    def __init__(self):
        wrapper.EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.retry_counts = {}  # per-reqId retry counts for pacing errors
        self._ready = False     # start pas na nextValidId

    # -------- Historical data handlers --------
    @iswrapper
    def historicalData(self, reqId: int, bar: BarData):
        # Map reqId -> rij in all_data
        symbol = all_data.loc[reqId, 'ib_symbol']
        asset_rollup = all_data.loc[reqId, 'asset_rollup']

        # WAP bestaat niet in alle builds: fallback naar 'WAP', anders typprijs
        wap_val = getattr(bar, 'wap', None)
        if wap_val is None:
            wap_val = getattr(bar, 'WAP', None)
        if wap_val is None:
            # simpele fallback (typical price)
            try:
                wap_val = (float(bar.high) + float(bar.low) + float(bar.close)) / 3.0
            except Exception:
                wap_val = None

        rows.append((
            bar.date, symbol, asset_rollup,
            bar.open, bar.high, bar.low, bar.close, bar.volume, wap_val
        ))

    @iswrapper
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        self.retry_counts.pop(reqId, None)

        if reqId in pending:
            pending.discard(reqId)
        kick_off_more(self)

        if not pending and next_idx >= len(all_data):
            print("All historical data processed. Disconnecting...")
            self.disconnect()

    @iswrapper
    def historicalDataUpdate(self, reqId: int, bar: BarData):
        pass

    # -------- Connection lifecycle --------
    @iswrapper
    def nextValidId(self, orderId: int):
        self._ready = True
        print(f"nextValidId: {orderId} -> starting requests…")
        kick_off_more(self)

    # -------- Error / status handling --------
    @iswrapper
    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson="", *args):
        if reqId == -1 and errorCode in INFO_STATUS_CODES:
            return

        print(f"ERROR {reqId} {errorCode} {errorString}")

        # Contract/currency problems: skip and move on
        if errorCode in {200, 406}:
            self.retry_counts.pop(reqId, None)
            if reqId >= 0 and reqId in pending:
                pending.discard(reqId)
            kick_off_more(self)
            if not pending and next_idx >= len(all_data):
                print("All requests completed (after skipping bad contracts). Disconnecting...")
                self.disconnect()
            return

        # Pacing/farm issues: single retry after brief backoff
        if errorCode in PACING_ERRORS:
            count = self.retry_counts.get(reqId, 0)
            if count < 1:
                self.retry_counts[reqId] = count + 1
                print("Pacing/farm issue. Backing off 15 seconds and retrying this symbol once...")
                sleep(15)
                send_request_for_index(self, reqId)
                return
            else:
                print("Pacing/farm issue persisted after retry. Skipping this symbol.\n")
                self.retry_counts.pop(reqId, None)
                if reqId >= 0 and reqId in pending:
                    pending.discard(reqId)
                kick_off_more(self)
                if not pending and next_idx >= len(all_data):
                    print("All requests completed. Disconnecting...")
                    self.disconnect()
                return

        # Unexpected error: drop and proceed
        self.retry_counts.pop(reqId, None)
        if reqId >= 0 and reqId in pending:
            pending.discard(reqId)
        kick_off_more(self)
        if not pending and next_idx >= len(all_data):
            print("All requests completed. Disconnecting...")
            self.disconnect()

    @iswrapper
    def connectionClosed(self):
        print("Connection closed.")


def send_request_for_index(app: TestApp, idx: int):
    """Submit one historical-data request for all_data row idx."""
    row = all_data.loc[idx]

    contract = Contract()
    contract.symbol          = _safe_str(row.get('ib_symbol'))
    contract.secType         = _infer_sec_type(row)
    contract.exchange        = _safe_str(row.get('exchange')) or "SMART"
    contract.currency        = _safe_str(row.get('ib_currency'))

    pe = _safe_str(row.get('prim_exchange', ''))
    if pe:
        contract.primaryExchange = pe  # alleen zetten als niet leeg

    what_to_show = _what_to_show(contract.secType)

    duration_days = "5 D"
    try:
        if len(sys.argv) > 1:
            days = int(sys.argv[1])
            if days > 0:
                duration_days = f"{days} D"
    except Exception:
        duration_days = "5 D"

    app.reqHistoricalData(
        idx,            # reqId == index (so callbacks map back to all_data)
        contract,
        "",             # endDateTime ("" = now)
        duration_days,  # duration
        "1 day",        # barSize
        what_to_show,   # whatToShow
        1,              # useRTH
        1,              # formatDate
        False,          # keepUpToDate
        []              # chartOptions
    )


def kick_off_more(app: TestApp):
    """Maintain up to MAX_IN_FLIGHT concurrent historical requests."""
    global next_idx
    if not getattr(app, "_ready", False):
        return  # nog niet volledig connected

    while len(pending) < MAX_IN_FLIGHT and next_idx < len(all_data):
        reqId = next_idx
        pending.add(reqId)
        send_request_for_index(app, reqId)
        next_idx += 1


# -------------------------
# Write all rows to Access temp table (create-if-not-exists, then append)
# -------------------------
def save_df_to_access_temp(df: pd.DataFrame, conn_str: str):
    """
    Creates temp_stock_prices_temp in Access if it doesn't exist,
    then inserts all rows from df (append).
    Expects df columns: ['date','symbol','asset_rollup','open','high','low','close','volume','wap'].
    """
    if df.empty:
        print("No data to write to Access.")
        return

    # Prepare data: rename 'date' -> 'datum' and convert to datetime
    df_to_write = df.copy()
    df_to_write = df_to_write.rename(columns={'date': 'datum'})

    # Robust datetime parsing (handles 'YYYYMMDD' and 'YYYYMMDD HH:MM:SS')
    s = df_to_write['datum'].astype(str)
    parsed = pd.to_datetime(s, errors='coerce')
    mask_nat = parsed.isna()
    if mask_nat.any():
        try_again = pd.to_datetime(s[mask_nat], format='%Y%m%d', errors='coerce')
        parsed[mask_nat] = try_again
        mask_nat = parsed.isna()
    if mask_nat.any():
        try_again2 = pd.to_datetime(s[mask_nat], format='%Y%m%d %H:%M:%S', errors='coerce')
        parsed[mask_nat] = try_again2
    df_to_write['datum'] = parsed

    # Ensure correct column order
    columns_order = ['datum', 'symbol', 'asset_rollup', 'open', 'high', 'low', 'close', 'volume', 'wap']
    df_to_write = df_to_write[columns_order]

    # Convert datetime to Python datetime objects (pyodbc)
    if pd.api.types.is_datetime64_any_dtype(df_to_write['datum']):
        df_to_write['datum'] = df_to_write['datum'].dt.to_pydatetime()

    temp_table_name = "temp_stock_prices_temp"
    create_temp_table_query = f"""
    CREATE TABLE {temp_table_name} (
        datum DATE,
        symbol TEXT(255),
        asset_rollup TEXT(255),
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        wap DOUBLE
    )
    """
    placeholders = ", ".join(["?"] * len(columns_order))
    insert_temp_table_query = f"INSERT INTO {temp_table_name} ({', '.join(columns_order)}) VALUES ({placeholders})"

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
            cur.executemany(insert_temp_table_query, data)
            conn.commit()
            print(f"Inserted {len(data)} rows into {temp_table_name}.")

    except pyodbc.Error as e:
        print("Error writing temp table to Access:", e)


def main():
    global all_data, next_idx
    t0 = time()

    # -------- Load symbol universe from Access --------
    with pyodbc.connect(conn_str) as connection:
        all_data = pd.read_sql(sql_query_stock_range, connection)

    # Filter: require non-null currency
    all_data = all_data[all_data[currency_column].notna()].reset_index(drop=True)

    print("Filtered data (first 5 rows):")
    print(all_data.head())
    print()

    # Reset window state (in case of repeated runs in same interpreter)
    pending.clear()
    next_idx = 0
    rows.clear()

    # -------- Start IB connection --------
    app = TestApp()
    client_id = random.randint(1, 10000)
    app.connect("127.0.0.1", 7496, clientId=client_id)
    print(f"Using client ID: {client_id}")
    print("serverVersion:%s connectionTime:%s" % (app.serverVersion(), app.twsConnectionTime()))

    # Niet handmatig kick_off_more(); nextValidId start automatisch
    app.run()

    # -------- Build DataFrame once from rows --------
    all_stock_prices_df = pd.DataFrame(
        rows,
        columns=['date', 'symbol', 'asset_rollup', 'open', 'high', 'low', 'close', 'volume', 'wap']
    )

    print("All requests completed.")
    print("Collected bars:", len(all_stock_prices_df))
    print(all_stock_prices_df.head())

    # -------- Append DataFrame to Access temp table --------
    save_df_to_access_temp(all_stock_prices_df, conn_str)

    print(f"Total elapsed: {time() - t0:.2f}s")


if __name__ == "__main__":
    main()
