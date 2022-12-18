from configparser import ConfigParser
import sql.utils
import get_jobs

config = ConfigParser()
config.read('config.ini')

# setting db parameters
jobs_db = config.get('SQL', 'JOBS_DB')
all_jobs_table = config.get('SQL', 'ALL_JOBS_TABLE')

def create_all_jobs_table(conn, all_jobs_table = all_jobs_table):
    query_from_file = sql.utils.get_query("create_all_jobs_table.sql")
    query_to_execute = query_from_file.format(all_jobs_table)
    sql.utils.execute_query(conn = conn, query = query_to_execute)

def insert_all_jobs_table(conn, values_to_insert = [], all_jobs_table = all_jobs_table):
    query_from_file = sql.utils.get_query("insert_all_jobs_table.sql")
    query_to_execute = query_from_file.format(all_jobs_table)
    sql.utils.executemany_query(conn = conn, query = query_to_execute, 
                            values = values_to_insert)

def remove_old_entries(conn, all_jobs_table = all_jobs_table):
    query_from_file = sql.utils.get_query("remove_old_entries.sql")
    query_to_execute = query_from_file.format(all_jobs_table)
    sql.utils.execute_query(conn = conn, query = query_to_execute)

def populate_all_jobs(conn, values_to_insert = []):
    create_all_jobs_table(conn = conn)
    insert_all_jobs_table(conn = conn, values_to_insert = values_to_insert)
    remove_old_entries(conn = conn)
    conn.commit()

if __name__ == "__main__":
    conn = sql.utils.create_connection(jobs_db)
    all_jobs = get_jobs.get_jobs()
    populate_all_jobs(conn = conn, values_to_insert = all_jobs)
    sql.utils.close_connection(conn)