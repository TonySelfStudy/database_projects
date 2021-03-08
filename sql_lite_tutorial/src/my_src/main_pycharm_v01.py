"""My SQLite walk-through and scratch codes on SQLite databasing
# based on the book 'Python and SQLite Development"""

import sqlite3
import datetime
from prettytable import PrettyTable
import pandas as pd


def db_connect():
    db = 'pydb.db'
    conn = sqlite3.connect(db)
    print('connected')
    print(conn)
    conn.close()


def open_db(file_name):
    print(f'Connecting to dbase: {file_name}')
    conn = sqlite3.connect(file_name)
    print(f'Connected')

    print(f'{conn=}')
    return conn


def create_product_table(conn):
    cursor = conn.cursor()

    txt = '''
    create table if not exists product(
    id integer primary key autoincrement,
    name char(30) not null,
    stock integer,
    price float,
    created datetime)
    '''

    cursor.execute(txt)
    conn.commit()
    cursor.close()

    print('Created/Verified the product table')


def create_data(conn):

    print('Creating Data')
    cursor = conn.cursor()

    insert_sql = ('insert into product (name, stock, price, created) '
                        'values (?, ?, ?, ?)')

    for i in range(1, 6):
        params = ("product" + str(i),
                  (100 + i),
                  (3.14 * i),
                  datetime.datetime.now())
        cursor.execute(insert_sql, params)
        product_id = cursor.lastrowid
        print(f'Created product_id: {product_id}')

    conn.commit()
    cursor.close()
    print('Data added to product table')

def read_data(conn):
    print('Reading Data')

    cursor = conn.cursor()
    sql = 'select * from product'
    cursor.execute(sql)

    # print('Bulk Read')
    # for i, row in enumerate(cursor):
    #     print(f'Row {i}: {row}')

    print('Specified Read and Formatted output')
    t = PrettyTable(['ID', 'Name', 'Stock', 'Price', 'Created'])
    cursor.execute(sql)
    for (id, name, stock, price, created) in cursor:
        t.add_row([id, name, stock, format(price, '.2f'), created])

    print(t)

    print('Data read to product table')


def update_data(conn, id, name, stock, price):
    print('Updating Data')
    cursor = conn.cursor()

    sql = ('update product set name=?, stock=?, price=? '
           'where id = ?')
    params = (name, stock, price, id)
    cursor.execute(sql, params)

    conn.commit()
    cursor.close
    print('Done updating')

def del_data(conn, id):
    print(f'Deleting Data for {id=}')
    cursor = conn.cursor()

    sql = ('delete from product where id = ?')
    params = (id,)
    cursor.execute(sql, params)

    print(f'{cursor.rowcount} row(s) deleted from product table')

    conn.commit()
    cursor.close
    print('Done updating')

def crud_demo():
    """Driver for crud_demo examples"""
    db_file_name = 'pydb.db'
    conn = open_db(db_file_name)
    create_product_table(conn)
    # create_data(conn)
    read_data(conn)

    id_ = 15
    update_data(conn, id_, 'new prod id', id_+100, id_*0.1)
    del_data(conn, 18)
    read_data(conn)

    panda_party = pd.read_sql_query('select * from product', conn)
    print(panda_party)

    conn.close()


if __name__ == '__main__':
    db_connect()
    crud_demo()
