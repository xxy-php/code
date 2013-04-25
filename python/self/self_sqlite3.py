import sqlite3
from constant import sql_location

def conn(db):
    conn=sqlite3.connect(sql_location+db)
    cur=conn.cursor()
    return conn,cur

def commit(conn,cur):
    conn.commit()
    cur.close()