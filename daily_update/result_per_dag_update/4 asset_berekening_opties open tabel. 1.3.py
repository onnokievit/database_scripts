import pyodbc
import pandas as pd
import numpy as np
from numba import njit
import time
import argparse
from datetime import datetime

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


# -------------------------------
# Numba kernel voor optie berekening
# -------------------------------
@njit
def compute_option_arrays(call_put_int, strike, close_price, amount):
    n = len(strike)
    itm_otm = np.empty(n, dtype=np.int32)
    option_value = np.empty(n, dtype=np.float64)
    open_value_itm = np.empty(n, dtype=np.float64)

    for i in range(n):
        cp = call_put_int[i]
        k = strike[i]
        s = close_price[i]
        a = amount[i]

        if not (s == s):  # NaN-check
            itm_otm[i] = 0
            option_value[i] = 0.0
            open_value_itm[i] = 0.0
            continue

        if cp == 0:  # call
            if k <= s:
                itm_otm[i] = 1
                v = s - k
                option_value[i] = v
                open_value_itm[i] = v * a
            else:
                itm_otm[i] = 0
                option_value[i] = 0.0
                open_value_itm[i] = 0.0
        else:  # put
            if k <= s:
                itm_otm[i] = 0
                option_value[i] = 0.0
                open_value_itm[i] = 0.0
            else:
                itm_otm[i] = 1
                v = k - s
                option_value[i] = v
                open_value_itm[i] = v * a

    return itm_otm, option_value, open_value_itm


# -------------------------------
# Close-prijs ophalen met terugkijkvenster
# -------------------------------
def get_close_price(df_asset_close_price, date, lookback_days=10):
    date = pd.to_datetime(date).normalize()
    for offset in range(lookback_days + 1):
        check_date = date - pd.Timedelta(days=offset)
        row = df_asset_close_price.loc[df_asset_close_price['datum'] == check_date, 'close']
        if not row.empty:
            return row.values[0]
    return np.nan


# -------------------------------
# Main script
# -------------------------------
start_time = time.time()

# --- DB connect ---
db_path = args.db
# db_path = r'C:\Users\onno\OneDrive\Beleggen\2025 - portefeuille database 02.03.accdb'
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + db_path
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# --- Assets ---
cursor.execute("SELECT asset_rollup FROM asset_rollup_data")
rows = cursor.fetchall()
asset_list = [row[0] for row in rows]

# --- Transacties (opties) ---
cursor.execute("SELECT * FROM transacties_bron_data WHERE asset_type='optie'")
rows = cursor.fetchall()
columns = [column[0] for column in cursor.description]
df_opties_alle_transacties = pd.DataFrame.from_records(rows, columns=columns)

# --- Historische prijzen ---
cursor.execute("SELECT * FROM hist_data_per_asset_symbol")
rows = cursor.fetchall()
columns = [column[0] for column in cursor.description]
df_asset_close_price_all = pd.DataFrame.from_records(rows, columns=columns)

# ✅ Datum normaliseren
if 'datum' in df_opties_alle_transacties.columns:
    df_opties_alle_transacties['datum'] = pd.to_datetime(df_opties_alle_transacties['datum']).dt.normalize()
if 'datum' in df_asset_close_price_all.columns:
    df_asset_close_price_all['datum'] = pd.to_datetime(df_asset_close_price_all['datum']).dt.normalize()

updates_open_opties = pd.DataFrame()

# --- Datumbereik ---
start_date = pd.to_datetime(parsed_date).normalize()
if not df_opties_alle_transacties.empty and df_opties_alle_transacties['datum'].notna().any():
    first_date_asset = pd.to_datetime(df_opties_alle_transacties['datum'].min()).normalize()
    if pd.isna(first_date_asset):
        first_date_asset = start_date
else:
    first_date_asset = start_date
start_date = max(start_date, first_date_asset)

date_range = pd.date_range(start=start_date, end=pd.Timestamp('today').normalize())

# --- Openstaande opties opbouwen ---
for date in date_range:
    date_only = date.normalize()
    df_optie_open_transacties_till_date = df_opties_alle_transacties[
        (df_opties_alle_transacties['optie_exp_date'] >= date_only) &
        (df_opties_alle_transacties['datum'] <= date_only)
    ]

    if df_optie_open_transacties_till_date.empty:
        continue

    # ⚠️ Multiplier meenemen in de grouping
    group_keys = ['uniek_id', 'multiplier_close_price'] if 'multiplier_close_price' in df_opties_alle_transacties.columns else ['uniek_id']

    df_grouped = df_optie_open_transacties_till_date.groupby(group_keys, as_index=False).agg({
        'broker': 'first',
        'asset_rollup': 'first',
        'optie_exp_date': 'first',
        'optie_strike': 'first',
        'optie_call_put': 'first',
        'transactie_euro_totaal': 'sum',
        'transactie_aantal': 'sum',
        'transactie_fee': 'sum'
    })

    df_grouped_open = df_grouped[df_grouped['transactie_aantal'] != 0].copy()
    if df_grouped_open.empty:
        continue

    df_grouped_open.loc[:, 'datum'] = date_only
    updates_open_opties = pd.concat([updates_open_opties, df_grouped_open], ignore_index=True)

# --- Kolommen hernoemen ---
updates_open_opties.rename(columns={
    'transactie_aantal': 'optie_aantal',
    'transactie_euro_totaal': 'optie_premie',
    'transactie_fee': 'optie_fee',
}, inplace=True)

# --- Close-prijzen per asset & datum ---
asset_close_price_every_date = pd.DataFrame(columns=['asset', 'datum', 'asset_close'])

for asset in asset_list:
    print(f"Fetching prices for {asset}...")
    df_asset_close_price = df_asset_close_price_all[df_asset_close_price_all['asset_rollup'] == asset].copy()

    for datum in date_range:
        cp = get_close_price(df_asset_close_price, datum)
        new_row = pd.DataFrame({'asset': [asset], 'datum': [datum], 'asset_close': [cp]})
        asset_close_price_every_date = pd.concat([asset_close_price_every_date, new_row], ignore_index=True)

asset_close_price_every_date.rename(columns={'asset': 'asset_rollup'}, inplace=True)

# --- Merge ---
df_merged = updates_open_opties.merge(
    asset_close_price_every_date[['asset_rollup', 'datum', 'asset_close']],
    how='left',
    on=['asset_rollup', 'datum']
)

# ✅ Multiplier toepassen op close_price
if 'multiplier_close_price' in df_merged.columns:
    df_merged['asset_close'] = df_merged['asset_close'] * df_merged['multiplier_close_price']
else:
    df_merged['multiplier_close_price'] = 1.0

missing_close_values = df_merged['asset_close'].isna().sum()
if missing_close_values > 0:
    print(f"⚠️ Warning: {missing_close_values} rows have missing asset_close values after the merge.")

# --- Batched berekening ---
cp_int = np.where(df_merged['optie_call_put'].values == 'call', 0, 1).astype(np.int32)
strike = df_merged['optie_strike'].astype(np.float64).values
close_price = df_merged['asset_close'].astype(np.float64).values
amount = df_merged['optie_aantal'].astype(np.int32).values

_ = compute_option_arrays(cp_int[:1], strike[:1], close_price[:1], amount[:1])  # warmup
itm_otm, optie_waarde, open_optie_waarde_itm = compute_option_arrays(cp_int, strike, close_price, amount)

df_merged['itm_otm'] = itm_otm
df_merged['optie_waarde'] = optie_waarde
df_merged['open_optie_waarde_itm'] = open_optie_waarde_itm
df_merged['winst_verlies'] = df_merged['optie_premie'] + df_merged['open_optie_waarde_itm']

print(df_merged.head())

# -------------------------------
# DataFrame kolommen herschikken volgens tabel Access (zonder Id)
# -------------------------------
final_columns = [
    'datum', 'broker', 'asset_rollup', 'uniek_id', 'optie_exp_date',
    'optie_strike', 'optie_call_put', 'optie_aantal', 'optie_premie',
    'asset_close', 'itm_otm', 'open_optie_waarde_itm', 'winst_verlies',
    'optie_fee', 'optie_waarde', 'multiplier_close_price'
]
df_final = df_merged[final_columns]

# --- Schrijf naar Access ---
print('delete data from table: per_dag_open_opties_opgerold')
cursor.execute("DELETE FROM per_dag_open_opties_opgerold WHERE datum >= ?", start_date)
conn.commit()

data_to_insert = [tuple(x) for x in df_final.to_numpy()]
columns = ', '.join(final_columns)
query = f"INSERT INTO per_dag_open_opties_opgerold ({columns}) VALUES ({', '.join(['?'] * len(final_columns))})"

print('insert data into table: per_dag_open_opties_opgerold')
cursor.executemany(query, data_to_insert)
conn.commit()

if conn:
    conn.close()

print("✅ Data successfully processed and Numba-batched calculations applied.")
elapsed_time = time.time() - start_time
print(f"⏱️ Time taken: {elapsed_time:.2f} seconds (script 4 - asset berekening opties)")
