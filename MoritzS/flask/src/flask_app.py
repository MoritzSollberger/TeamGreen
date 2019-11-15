from flask import Flask, render_template
from sqlalchemy import create_engine
import pandas as pd

app = Flask(__name__)

@app.route('/templates')
def render_template():
    return render_template('page.html')

# Extract the Data from the DB
db = 'twitter'
table = 'tweets'
user = 'postgres'
host = 'postgres'
port = '5432'

def extract_data():
    engine = create_engine(f'postgres://{user}:{user}@{host}:{port}/{db}')
    df = pd.read_sql(f'SELECT text FROM {table} ORDER BY RANDOM() LIMIT 2', engine)
    return df
    #print(df.shape)

string = extract_data()
#string = 'hello you'
@app.route('/')
def hello_world():
    return f'<h1 style="color:red; font-size:25px;">{string}</h1>'



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
