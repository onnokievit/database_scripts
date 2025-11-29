import pyodbc
import pandas as pd
import time
import argparse
from datetime import datetime

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

start_time = time.time()

# ----------------------
# Database connectie
# ----------------------
db_path = args.db
CONN_STR = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + db_path

# DB_PATH = (
#     r'C:\Users\onno\OneDrive\Beleggen\2025 - portefeuille database 02.03.accdb'
# )
# CONN_STR = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + DB_PATH

conn = None
cursor = None

try:
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    conn.autocommit = False

    # ----------------------
    # Data ophalen
    # ----------------------
    asset_list = pd.read_sql(
            "SELECT DISTINCT asset_rollup FROM asset_rollup_data", conn
        )['asset_rollup'].tolist()

    sprinter_list = pd.read_sql("SELECT * FROM sprinters_referentie_data", conn)
    
    transacties_all = pd.read_sql(
        "SELECT * FROM transacties_bron_data WHERE asset_type='sprinter'", conn
    )
    hist_data = pd.read_sql("SELECT * FROM hist_data_per_asset_symbol", conn)

    # Dictionaries voor snelle lookup
    price_dict = {asset: df for asset, df in hist_data.groupby('asset_rollup')}
    price_lookup = {
        asset: df.set_index('datum')['close'].to_dict()
        for asset, df in price_dict.items()
    }

    # ----------------------
    # Functie
    # ----------------------
    def get_close_price(asset, date):
        lookup = price_lookup.get(asset, {})
        teller_max = 10
        while teller_max > 0:
            if date in lookup and lookup[date] != 0:
                return lookup[date]
            date -= pd.Timedelta(days=1)
            teller_max -= 1
        return 0

    # ----------------------
    # Berekeningen
    # ----------------------
    bulk_data = []
    date_range = pd.date_range(parsed_date, end=pd.Timestamp('today'))

    for asset in asset_list:
        df_transacties = transacties_all[transacties_all['asset_rollup'] == asset]

        if df_transacties.empty:
            for date in date_range:
                bulk_data.append((0, 0, asset, date.date(), 0))
            continue

        for date in date_range:
            date_only = date.date()
            sprinter_resultaat = 0
            sprinter_aantal_bezit = 0

            df_till_date = df_transacties[df_transacties['datum'] <= date]

            df_grouped = df_till_date.groupby(
                ['uniek_id', 'multiplier_close_price'], as_index=False
            ).agg({
                'broker': 'first',
                'asset_rollup': 'first',
                'asset_detail': 'first',
                'transactie_euro_totaal': 'sum',
                'transactie_aantal': 'sum',
                'transactie_fee': 'sum'
            })

            close_price = get_close_price(asset, date)

            for _, row in df_grouped.iterrows():
                if row['transactie_aantal'] != 0:
                    sprinter_funding = sprinter_list.loc[
                        sprinter_list['asset_detail'] == row['asset_detail'],
                        'sprinter_funding'
                    ].iloc[0]
                    sprinter_ratio = sprinter_list.loc[
                        sprinter_list['asset_detail'] == row['asset_detail'],
                        'sprinter_ratio'
                    ].iloc[0]

                    adj_close_price = close_price * row['multiplier_close_price']

                    open_sprinter_resultaat = (
                        (row['transactie_aantal'] * adj_close_price) / sprinter_ratio
                        - (row['transactie_aantal'] * sprinter_funding) / sprinter_ratio
                        + row['transactie_euro_totaal']
                    )

                    sprinter_resultaat += open_sprinter_resultaat
                    sprinter_aantal_bezit += row['transactie_aantal'] / sprinter_ratio

            cumulative_close_resultaat = df_grouped[
                df_grouped['transactie_aantal'] == 0
            ]['transactie_euro_totaal'].sum()

            cumulative_fee = df_grouped['transactie_fee'].sum()
            sprinter_resultaat += cumulative_close_resultaat

            bulk_data.append((
                sprinter_resultaat,
                cumulative_fee,
                asset,
                date_only,
                sprinter_aantal_bezit
            ))

    bulk_df = pd.DataFrame(
        bulk_data,
        columns=[
            'sprinter_resultaat', 'sprinter_fee',
            'asset_rollup', 'datum', 'sprinter_aantal_bezit'
        ]
    )
    bulk_df.fillna(0, inplace=True)

    print("Aantal records naar TempUpdates (in DataFrame):", len(bulk_df))
    print(bulk_df.head())

    # ----------------------
    # Schrijven naar DB
    # ----------------------
    try:
        cursor.execute("DROP TABLE TempUpdates")
        conn.commit()
    except Exception:
        pass

    cursor.execute("""
        CREATE TABLE TempUpdates (
            datum DATE,
            asset_rollup TEXT(255),
            sprinter_resultaat DOUBLE,
            sprinter_fee DOUBLE,
            sprinter_aantal_bezit DOUBLE
        )
    """)
    conn.commit()

    # Row-by-row insert + commit
    inserted = 0
    for i, row in bulk_df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO TempUpdates (
                    sprinter_resultaat, sprinter_fee, asset_rollup, datum, sprinter_aantal_bezit
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                float(row['sprinter_resultaat']),
                float(row['sprinter_fee']),
                str(row['asset_rollup']),
                row['datum'] if isinstance(row['datum'], datetime) else pd.to_datetime(row['datum']).date(),
                float(row['sprinter_aantal_bezit'])
            ))
            inserted += 1
        except Exception as e:
            print(f"❌ Fout bij row {i}: {e}")
            print(row)
            break

    conn.commit()
    print(f"✅ TempUpdates gevuld met {inserted} rijen.")

    # Debug: tel records in TempUpdates
    cursor.execute("SELECT COUNT(*) FROM TempUpdates")
    print("Aantal rijen in TempUpdates (DB):", cursor.fetchone()[0])

    # Update eindtabel
    update_query = """
        UPDATE per_dag_asset_result
        INNER JOIN TempUpdates
        ON per_dag_asset_result.datum = TempUpdates.datum
        AND per_dag_asset_result.asset_rollup = TempUpdates.asset_rollup
        SET per_dag_asset_result.sprinter_resultaat = TempUpdates.sprinter_resultaat,
            per_dag_asset_result.sprinter_fee = TempUpdates.sprinter_fee,
            per_dag_asset_result.sprinter_aantal_bezit = TempUpdates.sprinter_aantal_bezit
    """
    cursor.execute(update_query)
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
