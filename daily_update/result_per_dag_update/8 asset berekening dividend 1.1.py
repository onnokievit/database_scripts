import pyodbc
import pandas as pd
import time
import argparse
from datetime import datetime

start_time = time.time()

# ----------------------
# Argument parsing
# ----------------------
parser = argparse.ArgumentParser(description="Script that accepts a date")
parser.add_argument('--date', type=str, help="Date in YYYY-MM-DD format")
parser.add_argument("--db", type=str, required=True, help="Pad naar Access database")
args = parser.parse_args()

if args.date:
    try:
        parsed_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        print(f"Parsed Date: {parsed_date}")
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        exit(1)
else:
    parsed_date = pd.Timestamp('today').date() - pd.Timedelta(days=50)
    print(f"No date provided. Using default date: {parsed_date}")

# ----------------------
# Database connectie
# ----------------------
db_path = args.db
# db_path = r'C:\\Users\\onno\\OneDrive\\Beleggen\\2025 - portefeuille database 02.03.accdb'
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + db_path

conn = None
cursor = None

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    conn.autocommit = False

    # ----------------------
    # Data ophalen
    # ----------------------
    asset_list = pd.read_sql("SELECT DISTINCT asset_rollup FROM asset_rollup_data", conn)['asset_rollup'].tolist()
    fees_dividend_list = pd.read_sql("SELECT * FROM fees_dividend", conn)

    # ----------------------
    # Filter alleen relevante fee types
    # ----------------------
    relevant_fees = ["dividend", "div_belasting", "871_fee", "transactiebelasting"]
    fees_dividend_list = fees_dividend_list[fees_dividend_list['fee_type'].isin(relevant_fees)]

    # ----------------------
    # Berekeningen in Pandas
    # ----------------------
    start_date = parsed_date
    end_date = pd.Timestamp('today').date()
    date_range = pd.date_range(start=start_date, end=end_date)

    update_data_list = []

    for asset in asset_list:
        if asset in fees_dividend_list['asset'].dropna().unique():
            df_asset = fees_dividend_list[fees_dividend_list['asset'] == asset].copy()

            # Aggregeer per datum
            df_asset = df_asset.groupby('datum', as_index=False).agg({'amount':'sum'})
            df_asset.sort_values('datum', inplace=True)

            # Cumulatieve som maken
            df_asset['cumsum'] = df_asset['amount'].cumsum()

            # Reindex naar volledige datumbereik
            df_asset = df_asset.set_index('datum').reindex(date_range, method='ffill').reset_index()
            df_asset.rename(columns={'index':'datum','cumsum':'dividend'}, inplace=True)
            df_asset['asset'] = asset

            # Vul ontbrekende waarden met 0
            df_asset['dividend'] = df_asset['dividend'].fillna(0)

            update_data_list.append(df_asset[['datum','asset','dividend']])
        else:
            # Geen fees/dividend voor dit asset → altijd 0
            df_empty = pd.DataFrame({'datum': date_range, 'asset': asset, 'dividend': 0})
            update_data_list.append(df_empty)

    update_data = pd.concat(update_data_list, ignore_index=True)
    update_data.rename(columns={'asset':'asset_rollup','dividend':'fees_dividend_belasting'}, inplace=True)

    elapsed_time = time.time() - start_time
    print(f"Build update_data: {elapsed_time:.2f} seconds")

    # ----------------------
    # Schrijven naar DB
    # ----------------------
    try:
        cursor.execute("DROP TABLE TempUpdates")
    except Exception:
        pass

    cursor.execute("""
        CREATE TABLE TempUpdates (
            datum DATE,
            asset_rollup TEXT(255),
            fees_dividend_belasting DOUBLE
        )
    """)

    cursor.executemany("""
        INSERT INTO TempUpdates (datum, asset_rollup, fees_dividend_belasting)
        VALUES (?, ?, ?)
    """, update_data[['datum','asset_rollup','fees_dividend_belasting']].values.tolist())

    update_query = """
        UPDATE per_dag_asset_result
        INNER JOIN TempUpdates ON per_dag_asset_result.datum = TempUpdates.datum
                              AND per_dag_asset_result.asset_rollup = TempUpdates.asset_rollup
        SET per_dag_asset_result.fees_dividend_belasting = TempUpdates.fees_dividend_belasting
    """
    cursor.execute(update_query)
    cursor.execute("DROP TABLE TempUpdates")

    conn.commit()
    print("✅ Bulk update completed successfully.")

except Exception as e:
    if conn:
        conn.rollback()
    print(f"❌ Error: {e}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()

end_time = time.time()
print(f"Time taken: {end_time - start_time:.2f} seconds")
