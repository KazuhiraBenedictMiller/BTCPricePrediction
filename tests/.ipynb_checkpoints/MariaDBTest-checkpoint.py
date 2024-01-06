# Module Imports
import mariadb
import sys
from datetime import datetime
import pandas as pd

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user="Zero",
        password="Zero.31",
        host="127.0.0.1",
        port=3306,
        database="Test"

    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)
    
# Get Cursor
cur = conn.cursor()
'''
for i in range(50):
    cur.execute(
    "INSERT Trial VALUES (?, ?, ?)", 
    (50 + i, datetime.now(), "miao"))
    
conn.commit() 

'''
cur.execute("SELECT * FROM Trial")

df = pd.DataFrame(data = [x for x in cur], columns = ["id", "date", "miao"])

print(df)