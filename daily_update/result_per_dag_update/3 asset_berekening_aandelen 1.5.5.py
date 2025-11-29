import pyodbc
import pandas as pd
import numpy as np
import argparse
from datetime import datetime


def main():
    # -----------------------------
    # CLI-argument voor startdatum
    # -----------------------------
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
        parsed_date = (pd.Timestamp('today') - pd.Timedelta(days=50)).date()
        print(f"No date provided. Using default date: {parsed_date}")

    start_ts = pd.to_datetime(parsed_date)
    end_ts = pd.Timestamp('today').normalize()
    prev_ts = start_ts - pd.Timedelta(days=1)

    # terugkijkvenster voor prijzen
    lookback_days = 5

    # -----------------------------
    # DB connect
    # -----------------------------
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

    # -----------------------------
    # 1) Data in één keer laden
    # -----------------------------
    # Alleen asset ANET meenemen
    cursor.execute("SELECT asset_rollup FROM asset_rollup_data")
    asset_list = [row[0] for row in cursor.fetchall()]
    assets_series = pd.Series(asset_list, name='asset_rollup')

    # transacties
    cursor.execute("SELECT * FROM transacties_bron_data WHERE asset_type='aandeel'")
    rows = cursor.fetchall()
    tx_cols = [c[0] for c in cursor.description]
    df_tx = pd.DataFrame.from_records(rows, columns=tx_cols)

    # prijzen
    cursor.execute("SELECT * FROM hist_data_per_asset_symbol")
    rows = cursor.fetchall()
    price_cols = [c[0] for c in cursor.description]
    df_prices = pd.DataFrame.from_records(rows, columns=price_cols)

    # adjusted close berekenen
    df_prices['adj_close'] = (
        df_prices['close'].astype(float) *
        df_prices['multiplier_close_price'].astype(float)
    )

    # baseline cumulatieven ophalen
    cursor.execute("""
        SELECT asset_rollup, datum,
               cumulative_aantal, cumulative_aantal_aankopen,
               cumulative_aantal_verkopen, cumulative_aankoop_bedrag,
               cumulative_verkoop_bedrag, asset_fee
        FROM per_dag_asset_result
        WHERE [datum] = ?
    """, prev_ts)
    rows = cursor.fetchall()
    prev_cols = [c[0] for c in cursor.description]
    df_prev = pd.DataFrame.from_records(rows, columns=prev_cols)

    if df_prev.empty:
        df_prev = pd.DataFrame({
            'asset_rollup': asset_list,
            'datum': pd.to_datetime(prev_ts),
            'cumulative_aantal': 0.0,
            'cumulative_aantal_aankopen': 0.0,
            'cumulative_aantal_verkopen': 0.0,
            'cumulative_aankoop_bedrag': 0.0,
            'cumulative_verkoop_bedrag': 0.0,
            'asset_fee': 0.0
        })

    # -----------------------------
    # 2) Normaliseren & filteren
    # -----------------------------
    if 'datum' in df_tx.columns:
        df_tx['datum'] = pd.to_datetime(df_tx['datum']).dt.normalize()
    if 'datum' in df_prices.columns:
        df_prices['datum'] = pd.to_datetime(df_prices['datum']).dt.normalize()
    df_prev['datum'] = pd.to_datetime(df_prev['datum']).dt.normalize()

    df_tx = df_tx[df_tx['asset_rollup'].isin(asset_list)].copy()
    df_prices = df_prices[df_prices['asset_rollup'].isin(asset_list)].copy()

    df_tx = df_tx[(df_tx['datum'] >= start_ts) & (df_tx['datum'] <= end_ts)].copy()
    df_prices = df_prices[(df_prices['datum'] <= end_ts)].copy()

    # -----------------------------
    # 3) Dagelijkse transacties
    # -----------------------------
    df_tx['aantal_aankoop'] = np.where(
        df_tx['transactie_type'] == 'koop',
        df_tx['transactie_aantal'].astype(float), 0.0
    )
    df_tx['aantal_verkopen'] = np.where(
        df_tx['transactie_type'] == 'verkoop',
        df_tx['transactie_aantal'].astype(float), 0.0
    )
    df_tx['aankoop_bedrag'] = np.where(
        df_tx['transactie_type'] == 'koop',
        df_tx['transactie_euro_totaal'].astype(float), 0.0
    )
    df_tx['verkoop_bedrag'] = np.where(
        df_tx['transactie_type'] == 'verkoop',
        df_tx['transactie_euro_totaal'].astype(float), 0.0
    )
    df_tx['fee_dag'] = df_tx['transactie_fee'].astype(float)

    daily = df_tx.groupby(['asset_rollup', 'datum'], as_index=False).agg({
        'aantal_aankoop': 'sum',
        'aantal_verkopen': 'sum',
        'aankoop_bedrag': 'sum',
        'verkoop_bedrag': 'sum',
        'fee_dag': 'sum'
    })

    # skeleton (alle dagen × alle assets)
    all_days = pd.date_range(start_ts, end_ts, freq='D')
    skeleton = (
        assets_series.to_frame()
        .assign(key=1)
        .merge(pd.DataFrame({'datum': all_days, 'key': 1}), on='key')
        .drop(columns='key')
    )
    daily_full = skeleton.merge(daily, on=['asset_rollup', 'datum'], how='left')

    for col in ['aantal_aankoop', 'aantal_verkopen',
                'aankoop_bedrag', 'verkoop_bedrag', 'fee_dag']:
        daily_full[col] = daily_full[col].fillna(0.0)

    # -----------------------------
    # 4) Prijsreeksen
    # -----------------------------
    prices_adj = (
        df_prices.pivot_table(index='datum',
                              columns='asset_rollup',
                              values='adj_close',
                              aggfunc='last')
        .sort_index()
        .reindex(columns=asset_list)
    )
    prices_raw = (
        df_prices.pivot_table(index='datum',
                              columns='asset_rollup',
                              values='close',
                              aggfunc='last')
        .sort_index()
        .reindex(columns=asset_list)
    )

    all_days_buffer = pd.date_range(start_ts - pd.Timedelta(days=lookback_days),
                                    end_ts, freq='D')

    def prep_prices(pivot_df):
        pivot_df = pivot_df.reindex(all_days_buffer)
        pivot_df.index.name = 'datum'
        pivot_df = pivot_df.replace(0, np.nan).ffill(limit=lookback_days).fillna(0.0)
        return pivot_df.loc[start_ts:end_ts]

    prices_adj = prep_prices(prices_adj)
    prices_raw = prep_prices(prices_raw)

    prices_long = (
        prices_adj.stack().rename('close_price_adj').reset_index()
        .rename(columns={'level_1': 'asset_rollup'})
    )
    prices_raw_long = (
        prices_raw.stack().rename('close_price_raw').reset_index()
        .rename(columns={'level_1': 'asset_rollup'})
    )

    # -----------------------------
    # 5) Merge & cumulatieven
    # -----------------------------
    df_all = (
        daily_full.merge(prices_long, on=['asset_rollup', 'datum'], how='left')
        .merge(prices_raw_long, on=['asset_rollup', 'datum'], how='left')
        .sort_values(['asset_rollup', 'datum'])
        .reset_index(drop=True)
    )
    df_all['close_price_adj'] = df_all['close_price_adj'].astype(float).fillna(0.0)
    df_all['close_price_raw'] = df_all['close_price_raw'].astype(float).fillna(0.0)

    df_all['cum_aantal_aankopen'] = df_all.groupby('asset_rollup')['aantal_aankoop'].cumsum()
    df_all['cum_aantal_verkopen'] = df_all.groupby('asset_rollup')['aantal_verkopen'].cumsum()
    df_all['cum_aankoop_bedrag'] = df_all.groupby('asset_rollup')['aankoop_bedrag'].cumsum()
    df_all['cum_verkoop_bedrag'] = df_all.groupby('asset_rollup')['verkoop_bedrag'].cumsum()
    df_all['cum_fee'] = df_all.groupby('asset_rollup')['fee_dag'].cumsum()

    baseline = df_prev[[
        'asset_rollup', 'cumulative_aantal_aankopen',
        'cumulative_aantal_verkopen', 'cumulative_aankoop_bedrag',
        'cumulative_verkoop_bedrag', 'asset_fee'
    ]]
    df_all = df_all.merge(baseline, on='asset_rollup', how='left')

    for col_cum, col_base in [
        ('cum_aantal_aankopen', 'cumulative_aantal_aankopen'),
        ('cum_aantal_verkopen', 'cumulative_aantal_verkopen'),
        ('cum_aankoop_bedrag', 'cumulative_aankoop_bedrag'),
        ('cum_verkoop_bedrag', 'cumulative_verkoop_bedrag'),
        ('cum_fee', 'asset_fee')
    ]:
        df_all[col_base] = df_all[col_base].fillna(0.0)
        df_all[col_cum] = df_all[col_cum] + df_all[col_base]

    df_all['cumulative_aantal_aankopen'] = df_all['cum_aantal_aankopen']
    df_all['cumulative_aantal_verkopen'] = df_all['cum_aantal_verkopen']
    df_all['cumulative_aankoop_bedrag'] = df_all['cum_aankoop_bedrag']
    df_all['cumulative_verkoop_bedrag'] = df_all['cum_verkoop_bedrag']
    df_all['asset_fee'] = df_all['cum_fee']
    df_all['cumulative_aantal'] = (
        df_all['cumulative_aantal_aankopen'] + df_all['cumulative_aantal_verkopen']
    )

    # waarde en resultaat berekenen met adjusted close
    df_all['waarde_bezit'] = df_all['cumulative_aantal'] * df_all['close_price_adj']
    df_all['asset_result'] = (
        df_all['cumulative_aankoop_bedrag'] +
        df_all['cumulative_verkoop_bedrag'] +
        df_all['waarde_bezit']
    )

    # -----------------------------
    # 6) Output naar Access (met raw close)
    # -----------------------------
    out_cols = [
        'datum', 'asset_rollup', 'cumulative_aantal',
        'cumulative_aantal_aankopen', 'cumulative_aantal_verkopen',
        'cumulative_aankoop_bedrag', 'cumulative_verkoop_bedrag',
        'close_price', 'waarde_bezit', 'asset_result', 'asset_fee'
    ]
    df_out = df_all.rename(columns={'close_price_raw': 'close_price'})[out_cols].copy()
    df_out['datum'] = pd.to_datetime(df_out['datum']).dt.date

    try:
        cursor.execute("DROP TABLE Tempper_dag_asset_result")
        conn.commit()
    except:
        conn.rollback()

    cursor.execute("""
        CREATE TABLE Tempper_dag_asset_result (
            datum DATE,
            asset_rollup TEXT(255),
            cumulative_aantal DOUBLE,
            cumulative_aantal_aankopen DOUBLE,
            cumulative_aantal_verkopen DOUBLE,
            cumulative_aankoop_bedrag DOUBLE,
            cumulative_verkoop_bedrag DOUBLE,
            close_price DOUBLE,
            waarde_bezit DOUBLE,
            asset_result DOUBLE,
            asset_fee DOUBLE
        )
    """)
    conn.commit()

    records = [tuple(x) for x in df_out.to_numpy()]
    placeholders = ','.join(['?'] * len(out_cols))
    insert_temp_sql = f"""
        INSERT INTO Tempper_dag_asset_result ({', '.join(out_cols)})
        VALUES ({placeholders})
    """
    if records:
        cursor.executemany(insert_temp_sql, records)
        conn.commit()

    cursor.execute("""
        UPDATE per_dag_asset_result
        INNER JOIN Tempper_dag_asset_result
        ON per_dag_asset_result.asset_rollup = Tempper_dag_asset_result.asset_rollup
        AND per_dag_asset_result.datum = Tempper_dag_asset_result.datum
        SET per_dag_asset_result.cumulative_aantal = Tempper_dag_asset_result.cumulative_aantal,
            per_dag_asset_result.cumulative_aantal_aankopen = Tempper_dag_asset_result.cumulative_aantal_aankopen,
            per_dag_asset_result.cumulative_aantal_verkopen = Tempper_dag_asset_result.cumulative_aantal_verkopen,
            per_dag_asset_result.cumulative_aankoop_bedrag = Tempper_dag_asset_result.cumulative_aankoop_bedrag,
            per_dag_asset_result.cumulative_verkoop_bedrag = Tempper_dag_asset_result.cumulative_verkoop_bedrag,
            per_dag_asset_result.close_price = Tempper_dag_asset_result.close_price,
            per_dag_asset_result.waarde_bezit = Tempper_dag_asset_result.waarde_bezit,
            per_dag_asset_result.asset_result = Tempper_dag_asset_result.asset_result,
            per_dag_asset_result.asset_fee = Tempper_dag_asset_result.asset_fee
    """)
    conn.commit()

    cursor.execute(f"""
        INSERT INTO per_dag_asset_result ({', '.join(out_cols)})
        SELECT {', '.join(['Tempper_dag_asset_result.' + c for c in out_cols])}
        FROM Tempper_dag_asset_result
        LEFT JOIN per_dag_asset_result
        ON Tempper_dag_asset_result.asset_rollup = per_dag_asset_result.asset_rollup
        AND Tempper_dag_asset_result.datum = per_dag_asset_result.datum
        WHERE per_dag_asset_result.datum IS NULL
    """)
    conn.commit()

    cursor.execute("DROP TABLE Tempper_dag_asset_result")
    conn.commit()

    if conn:
        conn.close()

    print(f"Klaar. Verwerkt van {start_ts.date()} t/m {end_ts.date()} voor {len(asset_list)} assets.")


if __name__ == "__main__":
    main()
