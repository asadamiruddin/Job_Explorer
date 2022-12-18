import os 
import sqlite3
from sqlite3 import Error


def check_db_exist(db):
    if os.path.exists(db):
        raise FileExistsError('Database already exists')

def create_connection(db):
    try:
        conn = sqlite3.connect(db)
        return conn
    except Error as e:
        raise(e)    

def close_connection(conn):
    try:
        conn.close()
    except Error as e:
        raise(e)    

def get_query(filename):
    try:
        path = os.path.dirname(os.path.abspath(__file__))

        file = open(f"{path}\queries\{filename}", "r")
        query = file.read()
        file.close()
        return query

    except:
        raise FileNotFoundError('Query file not found')

def execute_query(conn, query):
    cur = conn.cursor()
    try:
        cur.execute(query)
    except Error as e:
        raise(e)
    
def executemany_query(conn, query, values):
    cur = conn.cursor()
    try:
        cur.executemany(query, values)
    except Error as e:
        raise(e)

def drop_table(conn, table_name):
    query_from_file = get_query("drop_table.sql")
    query_to_execute = query_from_file.format(table_name)
    execute_query(conn, query_to_execute)
