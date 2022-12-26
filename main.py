from flask import Flask, render_template
import pandas as pd

app = Flask(__name__)

@app.route('/')
def index():
    df = pd.read_csv('top_skills.csv')
    df = df.iloc[: , 1:]
    df.confidence_level = df.confidence_level.round(2)
    data = list(df.itertuples(index=False, name=None))
    return render_template('index.html', value = data)

if __name__ == "__main__":
    app.run(debug=True)