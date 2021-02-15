import sqlite3


def open_db(file_name):
    conn = sqlite3.connect(file_name)
    print(f'Connected to dbase: {file_name}')
    print(conn)
    return conn


def create_product_table(conn):
    conn.cursor()

    txt = '''
    create table if not exists product(
    id integer primary key autoincrement,
    name char(30) not null,
    stock integer,
    price float,
    created datetime)
    '''

    conn.execute(txt)
    conn.commit()
    print('Created a table')


if __name__ == '__main__':
    db_file_name = 'pydb.db'
    conn = open_db(db_file_name)
    create_product_table(conn)
    conn.close()
