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





# import psycopg2
# from datetime import datetime

# try:
#     conn = psycopg2.connect(
#         host="localhost",
#         database="investment_compliance",
#         user="postgres",
#         password="Sql123"
#     )

#     cursor = conn.cursor()

#     print("Connected to database successfully!")

#     # Start transaction
#     job_start = datetime.now()

#     # Insert sample trade
#     insert_query = """
#     INSERT INTO compliance.trades 
#     (portfolio_id, trade_date, instrument, trade_amount)
#     VALUES (1, CURRENT_DATE, 'ERROR_TEST', 500000);
#     """

#     cursor.execute(insert_query)
#     print("Trade inserted.")

#     # Run compliance check
#     cursor.execute("SELECT compliance.run_compliance_check();")
#     print("Compliance check executed.")

#     # Commit only if everything succeeds
#     conn.commit()

#     print("Transaction committed successfully!")

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
# import os
# from datetime import datetime

# S3_FOLDER = "s3_bucket/incoming"

# try:
#     conn = psycopg2.connect(
#         host="localhost",
#         database="investment_compliance",
#         user="postgres",
#         password="Sql123"
#     )

#     cursor = conn.cursor()
#     print("Connected to database!")

#     job_start = datetime.now()

#     for file in os.listdir(S3_FOLDER):
#         if file.endswith(".csv"):
#             file_path = os.path.join(S3_FOLDER, file)
#             print(f"Processing file: {file}")

#             with open(file_path, 'r') as f:
#                 reader = csv.DictReader(f)

#                 for row in reader:
#                     print("Row read:", row)
#                     try:
#                         trade_amount = float(row["trade_amount"])

#                         # Validation rule
#                         if trade_amount <= 0:
#                             print(f"Invalid trade amount found: {trade_amount}. Skipping row.")
#                             continue

#                         insert_query = """
#                         INSERT INTO compliance.trades 
#                         (portfolio_id, trade_date, instrument, trade_amount)
#                         VALUES (%s, %s, %s, %s);
#                         """

#                         cursor.execute(insert_query, (
#                             row["portfolio_id"],
#                             row["trade_date"],
#                             row["instrument"],
#                             trade_amount
#                         ))

#                     except Exception as row_error:
#                         print("Row error:", row_error)
#                         continue

#             print(f"{file} loaded successfully.")

#     cursor.execute("SELECT compliance.run_compliance_check();")

#     conn.commit()
#     print("Compliance check completed.")
#     print("Transaction committed.")

# except Exception as e:
#     print("Error:", e)
#     conn.rollback()
#     print("Transaction rolled back.")

# finally:
#     cursor.close()
#     conn.close()
#     print("Connection closed.")

import shutil
import psycopg2
import csv
import os
from datetime import datetime

# ============================================
# ETL CONFIGURATION
# ============================================
S3_FOLDER = "s3_bucket/incoming"
JOB_NAME = "trade_etl"

try:
    conn = psycopg2.connect(
        host="localhost",
        database="investment_compliance",
        user="postgres",
        password="Sql123"
    )

    cursor = conn.cursor()
    print("Connected to database!")

    start_time = datetime.now()
    total_rows = 0
    failed_rows = 0

    # ============================================
    # FILE INGESTION
    # ============================================
    for file in os.listdir(S3_FOLDER):
        if file.endswith(".csv"):
            file_path = os.path.join(S3_FOLDER, file)
            print(f"Processing file: {file}")

            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)

                for row in reader:
                    try:
                        trade_amount = float(row["trade_amount"])

                        if trade_amount <= 0:
                            print(f"Invalid trade amount: {trade_amount}. Skipping row.")
                            failed_rows += 1
                            continue

                        insert_query = """
                        INSERT INTO compliance.trades 
                        (portfolio_id, trade_date, instrument, trade_amount)
                        VALUES (%s, %s, %s, %s);
                        """

                        cursor.execute(insert_query, (
                            row["portfolio_id"],
                            row["trade_date"],
                            row["instrument"],
                            trade_amount
                        ))

                        total_rows += 1

                    except Exception as row_error:
                        print("Row error:", row_error)
                        failed_rows += 1
                        continue

            print(f"{file} loaded successfully.")
    # ============================================
        # MOVE FILE TO ARCHIVE AFTER SUCCESS
        # ============================================
            archive_path = os.path.join("s3_bucket", "archive", file)
            shutil.move(file_path, archive_path)
            print(f"{file} moved to archive.")
            

    # ============================================
    # RUN COMPLIANCE PROCEDURE
    # ============================================
    cursor.execute("SELECT compliance.run_compliance_check();")

    end_time = datetime.now()

    # ============================================
    # INSERT AUDIT RECORD
    # ============================================
    audit_query = """
    INSERT INTO compliance.etl_job_audit
    (job_name, start_time, end_time, status, total_rows, failed_rows, error_message)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    cursor.execute(audit_query, (
        JOB_NAME,
        start_time,
        end_time,
        "SUCCESS",
        total_rows,
        failed_rows,
        None
    ))

    conn.commit()
    print("ETL completed successfully and logged.")

except Exception as e:
    conn.rollback()
    print("Error:", e)

finally:
    cursor.close()
    conn.close()
    print("Connection closed.")


