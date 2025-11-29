import pyodbc
import pandas as pd
import argparse
from datetime import datetime


def main():
    # Argument parser voor datum input
    parser = argparse.ArgumentParser(description="Script dat een datum accepteert")
    parser.add_argument('--date', type=str, help="Datum in YYYY-MM-DD formaat")
    parser.add_argument("--db", type=str, required=True, help="Pad naar Access database")
    args = parser.parse_args()

    # Datum verwerken
    if args.date:
        try:
            parsed_date = datetime.strptime(args.date, '%Y-%m-%d').date()
            print(f"Parsed Date: {parsed_date}")
        except ValueError:
            print("Ongeldig datumformaat. Gebruik YYYY-MM-DD.")
            exit(1)
    else:
        parsed_date = pd.Timestamp('today').date() - pd.Timedelta(days=50)
        print(f"Geen datum opgegeven. Default: {parsed_date}")

    # Database connectie
    db_path = args.db
    
    # db_path = (
    #     r'C:\Users\onno\OneDrive\Beleggen\2025 - portefeuille database 02.03.accdb'
    # )
    
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={db_path}'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Datum range
    start_date = parsed_date
    date_range = pd.date_range(start=start_date, end=pd.Timestamp('today'))

    # Ophalen data: per_dag_open_opties_opgerold
    query = f"""
        SELECT * FROM per_dag_open_opties_opgerold
        WHERE per_dag_open_opties_opgerold.datum >= #{start_date}#
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df_open_opgerolde_opties = pd.DataFrame.from_records(rows, columns=columns)

    # Ophalen asset_rollup lijst
    cursor.execute("SELECT asset_rollup FROM asset_rollup_data")
    rows = cursor.fetchall()
    asset_list = [row[0] for row in rows]

    print(df_open_opgerolde_opties)

    # Aggregatie
    print("# Aggregatie uitvoeren...")
    aggregated_df = df_open_opgerolde_opties.groupby(
        ['datum', 'asset_rollup']
    ).agg({
        'optie_premie': 'sum',
        'open_optie_waarde_itm': 'sum',
        'optie_fee': 'sum',
        'winst_verlies': 'sum',
        'optie_aantal': lambda x: x[
            (df_open_opgerolde_opties['optie_call_put'] == 'put') &
            (df_open_opgerolde_opties['optie_aantal'] < 0)
        ].sum()
        if ((df_open_opgerolde_opties['optie_call_put'] == 'put') &
            (df_open_opgerolde_opties['optie_aantal'] < 0)).any()
        else 0
    }).reset_index()

    print("Aggregated DataFrame:")
    print(aggregated_df)

    # Stap 1: Tijdelijke tabel maken
    cursor.execute("""
        CREATE TABLE TempUpdates (
            datum DATE,
            asset_rollup TEXT(255),
            open_premie DOUBLE,
            asset_open_optie_waarde DOUBLE,
            optie_open_fee DOUBLE,
            optie_aantal_put_bezit DOUBLE
        )
    """)

    # Stap 2: Geaggregeerde data invoegen in TempUpdates
    for _, row in aggregated_df.iterrows():
        cursor.execute("""
            INSERT INTO TempUpdates (
                datum, asset_rollup, open_premie,
                asset_open_optie_waarde, optie_open_fee, optie_aantal_put_bezit
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            row['datum'], row['asset_rollup'], row['optie_premie'],
            row['open_optie_waarde_itm'], row['optie_fee'], row['optie_aantal']
        ))

    # Stap 3: Bulk update uitvoeren
    update_query = """
        UPDATE per_dag_asset_result
        INNER JOIN TempUpdates
        ON per_dag_asset_result.datum = TempUpdates.datum
        AND per_dag_asset_result.asset_rollup = TempUpdates.asset_rollup
        SET per_dag_asset_result.open_premie = TempUpdates.open_premie,
            per_dag_asset_result.asset_open_optie_waarde = TempUpdates.asset_open_optie_waarde,
            per_dag_asset_result.optie_open_fee = TempUpdates.optie_open_fee,
            per_dag_asset_result.optie_aantal_put_bezit = TempUpdates.optie_aantal_put_bezit
    """
    cursor.execute(update_query)

    # Stap 4: Tijdelijke tabel opruimen
    cursor.execute("DROP TABLE TempUpdates")

    # Commit & afsluiten
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
