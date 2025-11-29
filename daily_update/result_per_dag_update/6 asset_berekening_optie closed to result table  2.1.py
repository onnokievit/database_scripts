import pyodbc
import pandas as pd
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
        exit(1)  # Exit the program if the date format is incorrect
else:
    parsed_date = pd.Timestamp('today').date() - pd.Timedelta(days=50)
    print(f"No date provided. Using default date: {parsed_date}")



# Path to your Access database file
db_path = args.db
# db_path = r'C:\Users\onno\OneDrive\Beleggen\2025 - portefeuille database 02.03.accdb'
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + db_path
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Fetching asset_rollup data
cursor.execute("SELECT asset_rollup FROM asset_rollup_data")
rows = cursor.fetchall()
asset_list = [row[0] for row in rows]
#asset_list = {'INFINEON'}

# Fetching transactions data
cursor.execute("SELECT * FROM transacties_bron_data WHERE asset_type='optie'")
rows = cursor.fetchall()
columns = [column[0] for column in cursor.description]
df_opties_alle_transacties_closed = pd.DataFrame.from_records(rows, columns=columns)

updates_closed_opties = pd.DataFrame()

# Set the start date for the date range
# start_date = pd.Timestamp('2024-01-01').date() # DATUM NIET WIJZIGEN, werkt snel genoeg zo, heeft fouten

start_date=parsed_date
first_date_asset = df_opties_alle_transacties_closed['datum'].min()

if not df_opties_alle_transacties_closed.empty and df_opties_alle_transacties_closed['datum'].notna().any():
    first_date_asset = df_opties_alle_transacties_closed['datum'].min().date()
    if pd.isna(first_date_asset):
        first_date_asset = start_date
else:
    first_date_asset = start_date

start_date = max(start_date, first_date_asset)
date_range = pd.date_range(start=start_date, end=pd.Timestamp('today'))

for date in date_range:
    date_only = date.date()

    df_optie_open_transacties_till_date = df_opties_alle_transacties_closed[(df_opties_alle_transacties_closed['datum'] <= date)]
    
    df_grouped = df_optie_open_transacties_till_date.groupby(
        'uniek_id', as_index=False
    ).agg({
        'broker': 'first',
        'asset_rollup': 'first',
        'optie_exp_date': 'first',
        'optie_strike': 'first',
        'optie_call_put': 'first',
        'transactie_euro_totaal': 'sum',
        'transactie_aantal': 'sum',
        'transactie_fee': 'sum'
    })

    # Filter for closed options
    df_grouped_open = df_grouped[df_grouped['transactie_aantal'] == 0].copy()

    # Check if df_grouped_open is not empty before proceeding with aggregation
    if not df_grouped_open.empty:
        # Aggregate on asset_rollup and datum
        df_grouped_open['datum'] = date_only  # Assign date_only to 'datum' column
        df_grouped_open = df_grouped_open.groupby(['asset_rollup', 'datum'], as_index=False).agg({
            'transactie_euro_totaal': 'sum',
            'transactie_fee': 'sum'
        })

        # Concatenate DataFrame
        updates_closed_opties = pd.concat([updates_closed_opties, df_grouped_open], ignore_index=True)

# Display the DataFrame of closed options
print('updates_closed_opties dataframe klaar')


# Step 1: Create a temporary table for updates
cursor.execute("""
    CREATE TABLE TempUpdates (
        datum DATE,
        asset_rollup TEXT(255),
        hist_premie DOUBLE,
        optie_closed_fee DOUBLE
    )
""")

print('# Step 2: Insert aggregated data into the temporary table')
for index, row in updates_closed_opties.iterrows():
    cursor.execute("""
        INSERT INTO TempUpdates (datum, asset_rollup, hist_premie, optie_closed_fee)
        VALUES (?, ?, ?, ?)
    """, (row['datum'], row['asset_rollup'], row['transactie_euro_totaal'], row['transactie_fee']))

print('# Step 3: Perform the bulk update')
update_query = """
    UPDATE per_dag_asset_result
    INNER JOIN TempUpdates ON per_dag_asset_result.datum = TempUpdates.datum AND per_dag_asset_result.asset_rollup = TempUpdates.asset_rollup
    SET 
        per_dag_asset_result.hist_premie = TempUpdates.hist_premie,
        per_dag_asset_result.optie_closed_fee = TempUpdates.optie_closed_fee
"""

cursor.execute(update_query)

# Step 4: Clean up by dropping the temporary table
cursor.execute("DROP TABLE TempUpdates")

# Commit the transaction
conn.commit()

# Close the connection
cursor.close()
conn.close()

print("Bulk update completed successfully.")


