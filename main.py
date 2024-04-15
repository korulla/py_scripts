import psycopg2
import pandas as pd

conn = psycopg2.connect(host="localhost", dbname="postgres",
                        user="postgres", password="26062001", port=5432)

excel_file = "jan24_tollfreeusage.xlsx"

# Read data from Excel file, specifying dtype=str for phone numbers to avoid .0 suffix
df_company_name = pd.read_excel(excel_file, sheet_name="Company phone numbers (2)", dtype=str)
df_bulkvs = pd.read_excel(excel_file, sheet_name="Bulkvs", dtype=str)
df_vt = pd.read_excel(excel_file, sheet_name="VT", dtype=str)

# Create table for company names
try:
    cur = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS company_phones (
        company VARCHAR(255),
        phone_number VARCHAR(255)
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    print("Table 'company_phones' created or already exists.")
except (Exception, psycopg2.Error) as error:
    print("Error creating table company_phones:", error)
    exit()

# Add data into company_phones database from the Excel sheet
try:
    for index, row in df_company_name.iterrows():
        company = row["Company"]
        phone_number = row["Phone Number"]
        cur.execute("""
                    INSERT INTO company_phones (company, phone_number)
                    VALUES (%s, %s);
                    """,
                    (company, phone_number))
        conn.commit()  # Commit after each insertion
    print("Data inserted successfully into company_phones.")
except (Exception, psycopg2.Error) as error:
    print("Error inserting data into company_phones:", error)
    conn.rollback()  # Rollback changes in case of error



# raise SystemExit


# Create a new table named 'bulk_vs' in PostgreSQL
try:
    create_bulkvs_query = """
    CREATE TABLE IF NOT EXISTS bulk_vs (
        company_name VARCHAR(255),
        duration_secs INTEGER,
        call_destination VARCHAR(255)
    );
    """
    cur.execute(create_bulkvs_query)
    conn.commit()
    print("Table 'bulk_vs' created or already exists.")
except (Exception, psycopg2.Error) as error:
    print("Error creating table bulk_vs:", error)
    exit()

# Iterate through the rows of the Bulkvs sheet and insert data into the bulk_vs table
for index, row in df_bulkvs.iterrows():
    phone_number = row["Call Destination"]
    cur.execute("SELECT company FROM company_phones WHERE phone_number = %s;", (phone_number,))
    result = cur.fetchone()
    if result:
        company_name = result[0]
        duration_secs = row["Duration Secs"]
        call_destination = phone_number  # Use the phone_number variable directly
        cur.execute("""
                    INSERT INTO bulk_vs (company_name, duration_secs, call_destination)
                    VALUES (%s, %s, %s);
                    """,
                    (company_name, duration_secs, call_destination))
        conn.commit()  # Commit after each insertion

# Create a new table named 'vitality' in PostgreSQL
try:
    create_vitality_query = """
    CREATE TABLE IF NOT EXISTS vitality (
        company_name VARCHAR(255),
        duration_secs INT,
        call_destination VARCHAR(255)
    );
    """
    cur.execute(create_vitality_query)
    conn.commit()
    print("Table 'vitality' created or already exists.")
except (Exception, psycopg2.Error) as error:
    print("Error creating table vitality:", error)
    exit()

# Iterate through the rows of the VT sheet and insert data into the vitality table
for index, row in df_vt.iterrows():
    phone_number = row["Destination"]
    cur.execute("SELECT company FROM company_phones WHERE phone_number = %s;", (phone_number,))
    result = cur.fetchone()
    if result:
        company_name = result[0]
        duration_secs = row["Seconds"]
        call_destination = phone_number
        cur.execute("""
                    INSERT INTO vitality (company_name, duration_secs, call_destination)
                    VALUES (%s, %s, %s);
                    """,
                    (company_name, duration_secs, call_destination))
        conn.commit()  # Commit after each insertion
    else:
        print("No company found for phone number:", phone_number)
        

try:
    create_total_bulkvs_query = """
    CREATE TABLE IF NOT EXISTS total_bulkvs (
        company_name VARCHAR(255),
        duration_mins INT,
        duration_secs INT,
        price INT
    );
    """
    cur.execute(create_total_bulkvs_query)
    conn.commit()
    print("Table 'total_bulkv' created or already exists.")
except (Exception, psycopg2.Error) as error:
    print("Error creating table total_bulkvs:", error)
    exit()

cur.execute("""
    SELECT company_name, SUM(duration_secs) AS total_duration_secs
    FROM bulk_vs
    GROUP BY company_name
""")

rows = cur.fetchall()

for row in rows:
    company_name = row[0]
    total_duration_sec = row[1]
    total_duration_min = total_duration_sec / 60
    price = total_duration_min * 0.030
    cur.execute("""
                INSERT INTO total_bulkvs (company_name, duration_mins, duration_secs, price)
                VALUES (%s, %s, %s, %s)
                """,
                (company_name, total_duration_min, total_duration_sec, price)
                )    
    conn.commit()
cur.close()
conn.close()
print("Database connection closed.")