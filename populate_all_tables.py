from configparser import ConfigParser
import sql.utils
import get_jobs
import populate_all_jobs
import populate_title_descriptions

config = ConfigParser()
config.read('config.ini')

# setting db parameters
jobs_db = config.get('SQL', 'JOBS_DB')

if __name__ == "__main__":
    conn = sql.utils.create_connection(jobs_db)
    all_jobs = get_jobs.get_jobs()
    populate_all_jobs.populate_all_jobs(conn = conn, values_to_insert = all_jobs)
    populate_title_descriptions.populate_title_descriptions(conn = conn)
    sql.utils.close_connection(conn)