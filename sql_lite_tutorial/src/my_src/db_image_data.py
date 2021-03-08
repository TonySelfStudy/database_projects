import sqlite3
import datetime
from prettytable import PrettyTable

def open_db(file_name):
    print(f'Connecting to dbase: {file_name}')
    conn = sqlite3.connect(file_name)
    print(f'Connected')

    print(f'{conn=}')
    return conn

def create_table(conn):
    cursor = conn.cursor()

    sql = ('create table if not exists imagefiles('
           'id integer primary key autoincrement,'
           'filename char(30) not null,'
           'filetype char(15) not null,'
           'imgfile blob,'
           'created datetime)')
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    print('Created table if needed')

def insert_image(conn, fn):
    with open(fn, 'rb') as fid:
        image = fid.read()

    sql = ('insert into imagefiles (filename, filetype, imgfile, created)'
           'values (?,?,?,?)')
    params = (fn, fn.split('.')[1], image, datetime.datetime.now())


    cursor = conn.cursor()
    cursor.execute(sql, params)
    img_id = cursor.lastrowid
    print(f'Inserted file: {fn} as id: {img_id}')

    conn.commit()
    cursor.close()

def extract_image(conn, id):
    sql = ('select filename, imgfile from imagefiles where id=?')
    params = (id,)

    cursor = conn.cursor()

    cursor.execute(sql, params)

    for (filename, imgfile) in cursor:
        print(f"Saving filename: {'copy' + filename}")

        with open('copy' + filename, 'wb') as fid:
            fid.write(imgfile)

def main():
    db_name = 'image_blob.db'
    conn1 = open_db(db_name)

    create_table(conn1)

    fn_image = 'image1.png'
    insert_image(conn1, fn_image)

    print('Extracting file from database to new file')
    extract_image(conn1, 5)

    print('Closing connection')
    conn1.close()
    print('Done!')


if __name__ == '__main__':
    main()
