import sqlite3
from datetime import datetime
from prettytable import PrettyTable
import pandas as pd


def open_database():    
    db = 'pydb.db' 

    print('Connecting to SQLite...')
    conn = sqlite3.connect(db)
    print('connected')  

    return conn


conn = open_database()
df = pd.read_sql_query("select * from product", conn)
print(df)

conn.close()