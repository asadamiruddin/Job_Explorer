from flask import Flask, render_template
from configparser import ConfigParser
import sql.utils
import pandas as pd

config = ConfigParser()
config.read('config.ini')

# setting db parameters
jobs_db = config.get('SQL', 'JOBS_DB')

app = Flask(__name__)

@app.route('/')
def index():
    conn = sql.utils.create_connection(jobs_db)
    cursor = conn.cursor()
    cursor.execute("select * from top_skills") 
    data = cursor.fetchall() 
    return render_template('index.html', value = data)

if __name__ == "__main__":
    app.run(debug=True)