from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello_world():
    return '<h1 style="color:red; font-size:30px;">Hello, World!</h1>'

if __name__ == '__main__':
    app.run()
