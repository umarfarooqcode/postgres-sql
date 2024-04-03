
import psycopg2
import psycopg2.extras
import csv
from datetime import datetime
conn = psycopg2.connect(
    host='localhost',
    dbname='postgres',
    user='postgres',
    password='umer.farooq1',
    port=5432  # Corrected parameter name here
)
cur = conn.cursor()
# Open your CSV file
with open(r'C:\Users\user\sample_dataset\Store_Complaints.csv', 'r') as f:
    # Your code here
    cur.execute("""
    CREATE TABLE IF NOT EXISTS store_complaints (
        Complaint_ID SERIAL PRIMARY KEY,
        Customer_Name TEXT,
        Complaint_Type TEXT,
        Staff_Name TEXT,
        Department TEXT,
        Product_Details TEXT,
        Date_of_Complaint DATE,
        Store_Location TEXT
    );
""")
    cur.execute("TRUNCATE TABLE store_complaints;")

    reader = csv.reader(f)
    next(reader)  # Skip the header row
    for row in reader:
        # Assuming the date is in the 7th column (index 6)
        date_of_complaint = datetime.strptime(row[6], '%m/%d/%Y').date()
        row[6] = date_of_complaint.strftime('%Y-%m-%d')
        
        # Now insert the row into the database
        cur.execute(
            "INSERT INTO store_complaints VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", row
        )
conn.commit()
cur.close()
conn.close()


