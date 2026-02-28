import psycopg2
import csv
from datetime import datetime

try:
    # Connect to database
    conn = psycopg2.connect(
        host="localhost",
        database="investment_compliance",
        user="postgres",
        password="Sql123"
    )

    cursor = conn.cursor()
    print("Connected to database.")

    # Open CSV file
    with open("trade_data.csv", "r") as file:
        reader = csv.DictReader(file)

        for row in reader:
            insert_query = """
            INSERT INTO compliance.trades 
            (portfolio_id, trade_date, instrument, trade_amount)
            VALUES (%s, %s, %s, %s);
            """

            cursor.execute(insert_query, (
                int(row["portfolio_id"]),
                row["trade_date"],
                row["instrument"],
                float(row["trade_amount"])
            ))

            print(f"Inserted trade for instrument: {row['instrument']}")

    # Run compliance check
    cursor.execute("SELECT compliance.run_compliance_check();")
    print("Compliance procedure executed.")

    conn.commit()
    print("Transaction committed successfully.")

except Exception as e:
    print("Error occurred:", e)
    conn.rollback()
    print("Transaction rolled back.")

finally:
    cursor.close()
    conn.close()
    print("Connection closed.")