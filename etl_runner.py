# import psycopg2
# import csv
# from datetime import datetime

# try:
#     # Connect to database
#     conn = psycopg2.connect(
#         host="localhost",
#         database="investment_compliance",
#         user="postgres",
#         password="Sql123"
#     )

#     cursor = conn.cursor()
#     print("Connected to database.")

#     # Open CSV file
#     with open("trade_data.csv", "r") as file:
#         reader = csv.DictReader(file)

#         for row in reader:
#             insert_query = """
#             INSERT INTO compliance.trades 
#             (portfolio_id, trade_date, instrument, trade_amount)
#             VALUES (%s, %s, %s, %s);
#             """

#             cursor.execute(insert_query, (
#                 int(row["portfolio_id"]),
#                 row["trade_date"],
#                 row["instrument"],
#                 float(row["trade_amount"])
#             ))

#             print(f"Inserted trade for instrument: {row['instrument']}")

#     # Run compliance check
#     cursor.execute("SELECT compliance.run_compliance_check();")
#     print("Compliance procedure executed.")

#     conn.commit()
#     print("Transaction committed successfully.")

# except Exception as e:
#     print("Error occurred:", e)
#     conn.rollback()
#     print("Transaction rolled back.")

# finally:
#     cursor.close()
#     conn.close()
#     print("Connection closed.")



# import psycopg2
# import csv
# from datetime import datetime

# job_name = "trade_etl"
# start_time = datetime.now()

# try:
#     conn = psycopg2.connect(
#         host="localhost",
#         database="investment_compliance",
#         user="postgres",
#         password="Sql123"
#     )
#     cursor = conn.cursor()

#     print("Connected to database.")

#     # Read CSV
#     with open("trade_data.csv", "r") as file:
#         reader = csv.DictReader(file)

#         for row in reader:
#             insert_query = """
#             INSERT INTO compliance.trades 
#             (portfolio_id, trade_date, instrument, trade_amount)
#             VALUES (%s, %s, %s, %s);
#             """

#             cursor.execute(insert_query, (
#                 int(row["portfolio_id"]),
#                 row["trade_date"],
#                 row["instrument"],
#                 float(row["trade_amount"])
#             ))

#     # Run compliance procedure
#     cursor.execute("SELECT compliance.run_compliance_check();")

#     conn.commit()

#     end_time = datetime.now()

#     # Log SUCCESS
#     cursor.execute("""
#         INSERT INTO compliance.etl_job_log 
#         (job_name, start_time, end_time, status)
#         VALUES (%s, %s, %s, %s);
#     """, (job_name, start_time, end_time, "SUCCESS"))

#     conn.commit()

#     print("ETL job completed successfully.")

# except Exception as e:
#     end_time = datetime.now()

#     cursor.execute("""
#         INSERT INTO compliance.etl_job_log 
#         (job_name, start_time, end_time, status, error_message)
#         VALUES (%s, %s, %s, %s, %s);
#     """, (job_name, start_time, end_time, "FAILED", str(e)))

#     conn.rollback()

#     print("Error occurred:", e)

# finally:
#     cursor.close()
#     conn.close()




import psycopg2
import csv
from datetime import datetime

job_name = "trade_etl"
start_time = datetime.now()
rejected_rows = 0

try:
    conn = psycopg2.connect(
        host="localhost",
        database="investment_compliance",
        user="postgres",
        password="Sql123"
    )
    cursor = conn.cursor()

    print("Connected to database.")

    with open("trade_data.csv", "r") as file:
        reader = csv.DictReader(file)

        for row in reader:
            try:
                portfolio_id = int(row["portfolio_id"])
                trade_amount = float(row["trade_amount"])
                trade_date = row["trade_date"]
                instrument = row["instrument"]

                # Basic validation rules
                if portfolio_id <= 0:
                    raise ValueError("Invalid portfolio_id")

                if trade_amount <= 0:
                    raise ValueError("Trade amount must be positive")

                if not trade_date:
                    raise ValueError("Missing trade_date")

                insert_query = """
                INSERT INTO compliance.trades 
                (portfolio_id, trade_date, instrument, trade_amount)
                VALUES (%s, %s, %s, %s);
                """

                cursor.execute(insert_query, (
                    portfolio_id,
                    trade_date,
                    instrument,
                    trade_amount
                ))

            except Exception as row_error:
                rejected_rows += 1
                print(f"Rejected row: {row} | Reason: {row_error}")
                continue

    # Run compliance procedure
    cursor.execute("SELECT compliance.run_compliance_check();")
    conn.commit()

    end_time = datetime.now()

    # Log SUCCESS
    cursor.execute("""
        INSERT INTO compliance.etl_job_log 
        (job_name, start_time, end_time, status, error_message)
        VALUES (%s, %s, %s, %s, %s);
    """, (
        job_name,
        start_time,
        end_time,
        "SUCCESS",
        f"Rejected rows: {rejected_rows}"
    ))

    conn.commit()

    print(f"ETL job completed. Rejected rows: {rejected_rows}")

except Exception as e:
    end_time = datetime.now()

    cursor.execute("""
        INSERT INTO compliance.etl_job_log 
        (job_name, start_time, end_time, status, error_message)
        VALUES (%s, %s, %s, %s, %s);
    """, (
        job_name,
        start_time,
        end_time,
        "FAILED",
        str(e)
    ))

    conn.rollback()
    print("Job failed:", e)

finally:
    cursor.close()
    conn.close()