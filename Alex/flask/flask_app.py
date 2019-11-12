# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 16:50:47 2019

@author: alexl
"""

from flask import Flask
from flask import render_template
# import get_tweets_aj as twt

app = Flask(__name__)


@app.route('/')
def home():
    # return '<h1>Hello from Flask</h1>'
    title = "Red Hot Chili Pythons"
    return render_template('test.html',title=title)


@app.route('/data')
def data():
    # tweet = twt.get_tweets(1, twt.show_text)
    # print(tweet)
    return "this is a string" #f'{tweet}'

app.run(host='0.0.0.0', port=80)
