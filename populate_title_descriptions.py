from configparser import ConfigParser
import sql.utils

config = ConfigParser()
config.read('config.ini')

# setting db parameters
jobs_db = config.get('SQL', 'JOBS_DB')
all_jobs_table = config.get('SQL', 'ALL_JOBS_TABLE')
job_titles_table = config.get('SQL', 'JOB_TITLES_TABLE')
title_descriptions_table = config.get('SQL', 'TITLE_DESCRIPTIONS_TABLE')
    
def create_job_titles_table(conn, job_titles_table = job_titles_table):
    query_from_file = sql.utils.get_query("create_job_titles_table.sql")
    query_to_execute = query_from_file.format(job_titles_table)
    sql.utils.execute_query(conn = conn, query = query_to_execute)

def insert_job_titles_table(conn, values_to_insert = [], job_titles_table = job_titles_table):
    query_from_file = sql.utils.get_query("insert_job_titles_table.sql")
    query_to_execute = query_from_file.format(job_titles_table)
    sql.utils.executemany_query(conn = conn, query = query_to_execute, 
                            values = values_to_insert)

def populate_title_descriptions(conn):
    sql.utils.drop_table(conn, title_descriptions_table)
    sql.utils.drop_table(conn, job_titles_table)

    create_job_titles_table(conn = conn)

    with open("job_titles.txt") as f:
        titles = zip([line.rstrip('\n') for line in f])
    insert_job_titles_table(conn = conn, values_to_insert = titles)

    query_from_file = sql.utils.get_query("populate_title_descriptions.sql")
    query_to_execute = query_from_file.format(title_descriptions_table, all_jobs_table, job_titles_table)
    sql.utils.execute_query(conn = conn, query = query_to_execute)

    conn.commit()

if __name__ == "__main__":
    conn = sql.utils.create_connection(jobs_db)
    populate_title_descriptions(conn = conn)
    sql.utils.close_connection(conn)