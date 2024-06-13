from flask import Flask
from flask import request
from numpy import require
from edfs import Firebase, MySQL, MongoDB
from flask_cors import CORS
import json
from task2 import Firebase as Fb, MySQL as Ms, MongoDB as Mg

app = Flask(__name__)
CORS(app)

@app.route('/edfs', methods = ['POST'])
def edfs(): 
    if request.method == 'POST':
        
        # data = request.form
        # print(data, request.json)
        
        
        data = request.data.decode('utf8').replace("'", '"')
        # print(json.loads(data)["server"])
        # return '<h1>Hello, World!</h1>'
        input = json.loads(data)
        command = input["command"]

        ans = ""
        if input["server"]=="1":
            response = app.response_class(
                response=json.dumps(Firebase(command)),
                status=200,
                mimetype='application/json'
            )
            return response
        elif input["server"]=="2":
            response = app.response_class(
                response=json.dumps(MongoDB(command)),
                status=200,
                mimetype='application/json'
            )
            return response
        elif input["server"]=="3":
            response = app.response_class(
                response=json.dumps(MySQL(command)),
                status=200,
                mimetype='application/json'
            )
            return response
        
@app.route('/navigate', methods = ['POST'])
def navigate():
    if request.method == 'POST':
       
        data = request.data.decode('utf8').replace("'", '"')
        
        input = json.loads(data)
        command = input["command"]
        
        if input["server"]=='1':
            response = app.response_class(
                response=json.dumps(Firebase(command)),
                status=200,
                mimetype='application/json'
            )
        elif input["server"]=='2':
            response = app.response_class(
                response=json.dumps(MongoDB(command)),
                status=200,
                mimetype='application/json'
            )
        else:
            response = app.response_class(
                response=json.dumps(MySQL(command)),
                status=200,
                mimetype='application/json'
            )
        return response
        
@app.route('/task2', methods = ['POST'])
def task2(): 
    if request.method == 'POST':
        
        # data = request.form
        # print(data, request.json)
        
        
        data = request.data.decode('utf8').replace("'", '"')
        # print(json.loads(data)["server"])
        # return '<h1>Hello, World!</h1>'
        input = json.loads(data)
        query = input["query"]
        options = input["options"]
        

        if input["server"]==1:
            response = app.response_class(
                response=json.dumps(Fb(query, options)),
                status=200,
                mimetype='application/json'
            )
            return response
        elif input["server"]==2:
            response = app.response_class(
                response=json.dumps(Mg(query, options)),
                status=200,
                mimetype='application/json'
            )
            return response
        elif input["server"]==3:
            response = app.response_class(
                response=json.dumps(Ms(query, options)),
                status=200,
                mimetype='application/json'
            )
            return response
            
if __name__ == "__main__":
  app.run()