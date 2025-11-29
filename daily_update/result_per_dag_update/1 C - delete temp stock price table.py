import pyodbc

conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\onno\OneDrive\Beleggen\2025 - portefeuille database 02.03 - STOCKDATA.accdb'
TEMP_TABLE = "temp_stock_prices_temp"

def delete_all_records():
    try:
        # Establish a connection to the database
        with pyodbc.connect(conn_str) as conn:
            conn.autocommit = True  # No need for explicit commit
            cur = conn.cursor()

            # SQL query to delete all records from the table
            delete_query = f"DELETE FROM {TEMP_TABLE}"
            cur.execute(delete_query)
            print(f"All records deleted from {TEMP_TABLE}")
    except pyodbc.Error as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    delete_all_records()
