import pyodbc
import time

conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\onno\OneDrive\Beleggen\2025 - portefeuille database 02.03 - STOCKDATA.accdb'

MAIN_TABLE = "historical_data_correct"
TEMP_TABLE = "temp_stock_prices_temp"

# Zet deze alleen de EERSTE run op True; daarna False voor maximale snelheid
CREATE_INDEXES_FIRST_TIME = False   # <-- zet na eerste run op False
ONLY_UPDATE_IF_CHANGED = True       # scheelt veel writes
USE_DELETE_APPEND = False           # True = snelste als kolommen 1-op-1 gelijk zijn

COLUMNS = ["datum", "symbol", "asset_rollup", "open", "high", "low", "close", "volume", "wap"]

def ensure_indexes(cur, conn):
    for tbl in (MAIN_TABLE, TEMP_TABLE):
        idx_name = f"idx_{tbl}_datum_symbol"
        try:
            cur.execute(f'CREATE INDEX {idx_name} ON {tbl} ([datum],[symbol])')
            conn.commit()
            print(f'Index aangemaakt: {idx_name}')
        except pyodbc.Error:
            pass  # bestaat al

def null_safe_diff(alias_main: str, alias_temp: str, col: str) -> str:
    """
    Bouwt een NULL-veilige verschil-expressie voor Access/ODBC:
      (main.col <> temp.col)
       OR (main.col IS NULL AND temp.col IS NOT NULL)
       OR (main.col IS NOT NULL AND temp.col IS NULL)
    """
    m = f"{alias_main}.[{col}]"
    t = f"{alias_temp}.[{col}]"
    return f"({m} <> {t} OR ({m} IS NULL AND {t} IS NOT NULL) OR ({m} IS NOT NULL AND {t} IS NULL))"

def merge_delete_append(cur):
    # Verwijder bestaande matches en append vervolgens alles
    delete_q = f"""
        DELETE FROM {MAIN_TABLE}
        WHERE EXISTS (
            SELECT 1 FROM {TEMP_TABLE} t
            WHERE t.[datum]={MAIN_TABLE}.[datum]
              AND t.[symbol]={MAIN_TABLE}.[symbol]
        )
    """
    cols = ",".join(f"[{c}]" for c in COLUMNS)
    insert_q = f"INSERT INTO {MAIN_TABLE} ({cols}) SELECT {cols} FROM {TEMP_TABLE}"

    cur.execute(delete_q)
    try:
        deleted = cur.rowcount
    except Exception:
        deleted = None
    cur.execute(insert_q)
    try:
        inserted = cur.rowcount
    except Exception:
        inserted = None
    return deleted, inserted

def merge_update_insert(cur):
    cols = ", ".join(f"[{c}]" for c in COLUMNS)
    update_cols = ["asset_rollup", "open", "high", "low", "close", "volume", "wap"]
    set_clause = ", ".join(f"main.[{c}] = temp.[{c}]" for c in update_cols)

    where_diff = ""
    if ONLY_UPDATE_IF_CHANGED:
        diffs = [null_safe_diff("main", "temp", c) for c in update_cols]
        where_diff = "WHERE " + " OR ".join(diffs)

    update_q = f"""
        UPDATE {MAIN_TABLE} AS main
        INNER JOIN {TEMP_TABLE} AS temp
            ON main.[datum]=temp.[datum] AND main.[symbol]=temp.[symbol]
        SET {set_clause}
        {where_diff}
    """

    insert_q = f"""
        INSERT INTO {MAIN_TABLE} ({cols})
        SELECT {cols}
        FROM {TEMP_TABLE} AS temp
        WHERE NOT EXISTS (
            SELECT 1
            FROM {MAIN_TABLE} AS main
            WHERE main.[datum]=temp.[datum]
              AND main.[symbol]=temp.[symbol]
        )
    """

    cur.execute(update_q)
    try:
        updated = cur.rowcount
    except Exception:
        updated = None

    cur.execute(insert_q)
    try:
        inserted = cur.rowcount
    except Exception:
        inserted = None

    return updated, inserted

def main():
    t0 = time.time()
    with pyodbc.connect(conn_str) as conn:
        conn.autocommit = False  # één grote transactie
        cur = conn.cursor()

        if CREATE_INDEXES_FIRST_TIME:
            ti = time.time()
            ensure_indexes(cur, conn)
            conn.commit()
            print(f"Indexfase: {time.time()-ti:.2f}s")

        if USE_DELETE_APPEND:
            tm = time.time()
            deleted, inserted = merge_delete_append(cur)
            conn.commit()
            print(f"Delete+Append: {time.time()-tm:.2f}s  (deleted={deleted}, inserted={inserted})")
        else:
            tm = time.time()
            updated, inserted = merge_update_insert(cur)
            conn.commit()
            print(f"Update+Insert: {time.time()-tm:.2f}s  (updated={updated}, inserted={inserted})")

    print(f"Totaal: {time.time()-t0:.2f}s")

if __name__ == "__main__":
    main()