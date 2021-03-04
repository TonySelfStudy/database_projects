import sqlite3

db = 'pydb.db' 

conn = sqlite3.connect(db)
print('connected')                              
print(conn)

conn.close()