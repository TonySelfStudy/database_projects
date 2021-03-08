import sqlite3
from datetime import datetime
from prettytable import PrettyTable


def open_database():    
    db = 'pydb.db' 

    print('Connecting to SQLite...')
    conn = sqlite3.connect(db)
    print('connected')  

    return conn

def read_data(conn):
    print('reading data')
    cursor = conn.cursor()

    cursor.execute("select id, name, stock, price, created from product") 
    t = PrettyTable(['ID','Name', 'Stock', 'Price', 'Created'])   
    for (id, name, stock, price, created) in cursor:   
        t.add_row([id, name, stock, format(price,'.2f'), created])
        
    print(t)
    cursor.close()
    print('done')


# creating data demo
print('transaction demo')

conn = open_database()
print('Original data.....')
read_data(conn)

# set manual transaction
conn.isolation_level = None


try:
    cursor = conn.cursor()
    cursor.execute("BEGIN")
    for index in range(1,5):
        product_name = 'product ' + str(index)
        price = 1.2 * index
        stock = 10 + 2*index        
        
        insert_sql = ("INSERT INTO product "
                "(name, stock, price, created) "
                "VALUES(?, ?, ?, ?)")

        # demo error
        if index == 3:
            insert_sql = insert_sql.replace('INSERT','INSERT1') # wrong statement

        params = (product_name, stock, price, datetime.now())
        conn.execute(insert_sql, params)
        product_id = cursor.lastrowid
        print('inserted with id=', product_id)       
    
    conn.commit()
    cursor.close()
    
except Exception as e:
    cursor.execute("ROLLBACK")
    conn.rollback()
    print('error in inserting data')
    print(e)


print('Update data.....')
read_data(conn)

conn.close()
print('done')

