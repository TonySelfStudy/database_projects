import sqlite3
from datetime import datetime
from prettytable import PrettyTable


def open_database():    
    db = 'pydb.db' 

    print('Connecting to SQLite...')
    conn = sqlite3.connect(db)
    print('connected')  

    return conn

def create_table(conn):
    cursor = conn.cursor()
    sql = ''' create table if not exists product(
        id integer primary key autoincrement,
        name char(30) not null,
        stock integer,
        price float,
        created datetime
    )'''

    cursor.execute(sql)
    conn.commit() 
    print('created a table')


def create_data(conn):    
    cursor = conn.cursor()
    print('inserting data...')
    for i in range(1,6):
        insert_sql = ("INSERT INTO product "
                   "(name, stock, price, created) "
                   "VALUES(?, ?, ?, ?)")

        params = ("product " + str(i), 3+i*4,
                        0.4+i*8, datetime.now())
        cursor.execute(insert_sql, params)
        product_id = cursor.lastrowid
        print('inserted with id=', product_id)

    conn.commit()
    cursor.close()
    print('done')

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


def update_data(conn,id,product_name,stock,price):
    print('updating data for product id=' + str(id))
    update_sql = ("UPDATE product SET name=?, stock=?, price=? "                   
                   "WHERE id=?")
    cursor = conn.cursor()

    params = (product_name,stock,price,id,)
    cursor.execute(update_sql, params)
    print(cursor.rowcount, ' products updated')
    
    conn.commit()
    cursor.close()
    print('done')


def delete_data(conn,id):
    print('deleting data with id=' + str(id))
    cursor = conn.cursor()

    params = (id,)
    cursor.execute("delete from product where id=?", params)
    print(cursor.rowcount, ' product deleted')
    
    conn.commit()
    cursor.close()
    print('done')


# open data
conn = open_database()

# creating table demo
create_table(conn)

# creating data demo
create_data(conn)

# reading data demo
read_data(conn)

# # updating data demo
# print('updating data demo')
# id = 3
# product_name = 'updated name'
# stock = 10
# price = 0.9
# update_data(conn,id, product_name, stock, price)
# read_data(conn)

# # deleting data demo
# print('deleting data demo')
# read_data(conn)
# id = 3
# delete_data(conn,id)
# read_data(conn)
# print('done')

# close data
conn.close()