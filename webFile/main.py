# from http.server import HTTPServer, SimpleHTTPRequestHandler
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os
import cgi
import pandas as pd
import psycopg2
from openpyxl import Workbook


def process_excel_file(file_name):
    # Database connection
    conn = psycopg2.connect(host="localhost", dbname="postgres",
                            user="postgres", password="26062001", port=5432)
    cur = conn.cursor()
    # Read data from Excel file
    df_company_name = pd.read_excel(
        file_name, sheet_name="Company phone numbers (2)", dtype=str)
    df_bulkvs = pd.read_excel(file_name, sheet_name="Bulkvs", dtype=str)
    df_vt = pd.read_excel(file_name, sheet_name="VT", dtype=str)
    # Create table for company names
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS company_phones (
            company VARCHAR(255),
            phone_number VARCHAR(255)
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        print("table for company names created")
    except (Exception, psycopg2.Error) as error:
        print("Error creating table company_phones:", error)
        conn.rollback()
    # Insert data into company_phones database from the Excel sheet
    try:
        for index, row in df_company_name.iterrows():
            company = row["Company"]
            phone_number = row["Phone Number"]
            cur.execute("""
                        INSERT INTO company_phones (company, phone_number)
                        VALUES (%s, %s);
                        """, (company, phone_number))
            conn.commit()
            print("data successfully inserted into company_phones")
    except (Exception, psycopg2.Error) as error:
        print("Error inserting data into company_phones:", error)
        conn.rollback()
    # Create table bulk_vs and insert data
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
        print("created table for bulk vs")
        for index, row in df_bulkvs.iterrows():
            phone_number = row["Call Destination"]
            cur.execute(
                "SELECT company FROM company_phones WHERE phone_number = %s;", (phone_number,))
            result = cur.fetchone()
            if result:
                company_name = result[0]
                duration_secs = row["Duration Secs"]
                call_destination = phone_number
                cur.execute("""
                            INSERT INTO bulk_vs (company_name, duration_secs, call_destination)
                            VALUES (%s, %s, %s);
                            """, (company_name, duration_secs, call_destination))
                conn.commit()
                print("data inserted into bulkvs successfully")
    except (Exception, psycopg2.Error) as error:
        print("Error with table bulk_vs:", error)
        conn.rollback()
    # Create table vitality and insert data
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
        print("created table named vitality")
        for index, row in df_vt.iterrows():
            phone_number = row["Destination"]
            cur.execute(
                "SELECT company FROM company_phones WHERE phone_number = %s;", (phone_number,))
            result = cur.fetchone()
            if result:
                company_name = result[0]
                duration_secs = row["Seconds"]
                call_destination = phone_number
                cur.execute("""
                            INSERT INTO vitality (company_name, duration_secs, call_destination)
                            VALUES (%s, %s, %s);
                            """, (company_name, duration_secs, call_destination))
                conn.commit()
                print("data inserted into vitality successfully")
    except (Exception, psycopg2.Error) as error:
        print("Error with table vitality:", error)
        conn.rollback()
    # Create and insert data into total_bulkvs
    try:
        create_total_bulkvs_query = """
        CREATE TABLE IF NOT EXISTS total_bulkvs (
            company_name VARCHAR(255),
            duration_mins REAL,
            duration_secs REAL,
            price REAL
        );
        """
        cur.execute(create_total_bulkvs_query)
        conn.commit()
        print("table created for total bulkvs")
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
            price = total_duration_min * 0.035
            cur.execute("""
                        INSERT INTO total_bulkvs (company_name, duration_mins, duration_secs, price)
                        VALUES (%s, %s, %s, %s)
                        """, (company_name, total_duration_min, total_duration_sec, price))
            conn.commit()
            print("data added to total bulk vs successfully")
    except (Exception, psycopg2.Error) as error:
        print("Error with table total_bulkvs:", error)
        conn.rollback()

    # creating and adding data into total vitality
    try:
        create_total_vitality_query = """
        CREATE TABLE IF NOT EXISTS total_vitality (
            company_name VARCHAR(255),
            duration_mins REAL,
            duration_secs REAL,
            price REAL
        );
        """
        cur.execute(create_total_vitality_query)
        conn.commit()
        print("table created for total vitality")
        cur.execute("""
            SELECT company_name, SUM(duration_secs) AS total_duration_secs
            FROM vitality
            GROUP BY company_name
        """)
        rows = cur.fetchall()
        for row in rows:
            company_name = row[0]
            total_duration_sec = row[1]
            total_duration_min = total_duration_sec / 60
            price = total_duration_min * 0.030
            cur.execute("""
                        INSERT INTO total_vitality (company_name, duration_mins, duration_secs, price)
                        VALUES (%s, %s, %s, %s)
                        """, (company_name, total_duration_min, total_duration_sec, price))
            conn.commit()
            print("data added into total vitality successfully")
    except (Exception, psycopg2.Error) as error:
        print("Error with table total_bulkvs:", error)
        conn.rollback()

    cur.close()
    conn.close()


class CustomHTTPRequestHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/download':
            # Set the file path to the location of your downloaded file
            file_path = 'downloaded_file.xlsx'
            if os.path.exists(file_path):
                self.send_response(200)
                self.send_header('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                self.send_header('Content-Disposition', 'attachment; filename="downloaded_file.xlsx"')
                self.end_headers()
                with open(file_path, 'rb') as f:
                    self.wfile.write(f.read())
            else:
                self.send_error(404, message="File not found")
        else:
            super().do_GET()
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', 'http://localhost:5173')
        super().end_headers()
    def do_POST(self):
        if self.path == '/upload':
            ctype, pdict = cgi.parse_header(self.headers.get('content-type'))
            if ctype == 'multipart/form-data':
                pdict['boundary'] = bytes(pdict['boundary'], "utf-8")
                pdict['CONTENT-LENGTH'] = int(
                    self.headers.get('content-length'))
                fields = cgi.parse_multipart(self.rfile, pdict)
                file_item = fields['file'][0]
                if file_item:
                    file_name = 'uploaded_file.xlsx'
                    with open(file_name, 'wb') as f:
                        f.write(file_item)
                    print(f"File '{file_name}' uploaded successfully.")
                    # Process the uploaded file
                    process_excel_file(file_name)
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"File processed successfully.")
                else:
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(b"No file was uploaded.")
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Invalid request.")
        elif self.path == '/download':
            try:
                wb = Workbook()
                conn = psycopg2.connect(host="localhost", dbname="postgres",
                                        user="postgres", password="26062001", port=5432)
                cur = conn.cursor()
                tables = ["company_phones", "bulk_vs", "vitality", "total_bulkvs", "total_vitality"]
                for table in tables:
                    cur.execute(f"SELECT * FROM {table}")
                    rows = cur.fetchall()
                    df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
                    df.to_excel(wb, sheet_name=table, index=False)
                file_name = 'downloaded_file.xlsx'
                wb.save(file_name)
                cur.close()
                conn.close()
                with open(file_name, 'rb') as f:
                    content = f.read()
                self.send_response(200)
                self.send_header('Content-type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                self.send_header('Content-Disposition', f'attachment; filename="{file_name}"')
                self.end_headers()
                self.wfile.write(content)
            except (Exception, psycopg2.Error) as error:
                print("Error downloading Excel file:", error)
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b"Error downloading Excel file.")

        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"URL not found.")


def run(server_class=HTTPServer, handler_class=CustomHTTPRequestHandler):
    server_address = ('', 8000)
    httpd = server_class(server_address, handler_class)
    print("Starting httpd server on port 8000...")
    httpd.serve_forever()


if __name__ == "__main__":
    run()
