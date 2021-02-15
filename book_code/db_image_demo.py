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
    sql = ''' create table if not exists imagefiles(
        id integer primary key autoincrement,
        filename char(30) not null,
        imagetype char(30) not null,
        imgfile blob,
        created datetime
    )'''

    cursor.execute(sql)
    conn.commit() 
    print('created a table')


def insert_image_data(conn,full_file_path,file_name,file_type):
    print('inserting image data')
    cursor = conn.cursor()

    with open(full_file_path, 'rb') as f:
        imagedata = f.read()
    
    params = (file_name,file_type,imagedata,datetime.now())
    query = ("insert into imagefiles(filename,imagetype,imgfile,created) "
            "values(?,?,?,?)")
    
    cursor.execute(query, params)
    img_id = cursor.lastrowid
    print('inserted with id=',img_id)    
    
    conn.commit()
    cursor.close()

def read_image_data(conn, id,save_as_file):
    print('reading data id=',id)
    cursor = conn.cursor()
    try:

        params = (id,)
        query = ("select filename,imagetype,imgfile,created "
                 "from imagefiles where id=?")                
        
        cursor.execute(query,params)
        t = PrettyTable(['ID','File Name', 'Image Type','Created'])   
        for (filename, imagetype, imgfile, created) in cursor:   
            t.add_row([id, filename, imagetype, created])

            with open(save_as_file, 'wb') as f:
                f.write(imgfile)      
            f.close()
            print('Save image data as ',save_as_file)    
        
        print(t)
    except Exception as e:
        print(e)
 
    finally:
        cursor.close()
        pass

# open database
conn = open_database()

# # creating data demo
# create_table(conn)

# # inserting image data demo
# print('inserting image data demo')
# full_file_path = './image1.png'
# file_name = 'image1.png'
# file_type = 'image/png'
# insert_image_data(conn,full_file_path,file_name,file_type)
# print('done')


# reading image data demo
print('reading image data demo')
save_as_file = './image1-read.png'
id = 1
read_image_data(conn,id,save_as_file)
print('done')

# close database
#conn.close()