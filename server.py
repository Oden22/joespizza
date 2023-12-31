from flask import Flask, render_template, jsonify, request, send_from_directory
from utils import *
from flask_cors import CORS
import time
# Create a Flask web application
app = Flask(__name__, static_url_path='/', static_folder='build')

CORS(app)

global mainServ
global clientServ
global cosmosServ
global orderManager

def create_new_connectors():
    # This creates new objects for the several connectors
    global mainServ
    global clientServ
    global cosmosServ
    global orderManager

    mainServ = SQLDataAccess(
        server = 'ict320-task3d.database.windows.net',
        database = 'joe-pizzeria',
        username = 'student320 ',
        password = 'ICT320_student'
    )

    clientServ = SQLDataAccess(
        server = 'ict320-ob-pizaa-sqlserv.database.windows.net',
        database = 'ICT320-PIZZA-SQL',
        username = 'obadmin ',
        password = 'Adminadmin$!'
    )

    cosmosServ = CosmosDataAccess(
        db_name = "Pizza",
        uri = "mongodb://ict320-obmongo:UkVH3PR4G5AwII87B9A9NVvklCnYzIbXJ9jV7B4xWccrtO0XCB5uK4rsBUSZghFbW6Btvaln2wvlACDbpWVkzw==@ict320-obmongo.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@ict320-obmongo@"
    )

    orderManager = OrderManager(clientServ, mainServ, cosmosServ)

@app.route('/api/orders/process/<date>')
def process_day_orders(date):
    #This route creates dockets for a specific date
    attempt = 0
    error = ""
    
    try:
        dockets = orderManager.process_orders(date)
        print("Successfully Processed Dockets for date: " + date)
        print(dockets)
        return jsonify(dockets)
    except Exception as e:
        print("Creating New Connectors")
        print(e)
        error = e
        create_new_connectors()
        attempt += 1
    
    return jsonify({'error': str(error)}), 500

@app.route('/api/orders/endofday/<date>')
def end_of_day(date):
    #This route preforms the end of day operations
    attempt = 0
    error = ""
    try:
        summary = orderManager.end_of_day_operations(date)
        print("Successfully Processed End Of Day for date: " + date)
        return jsonify(summary)
    except Exception as e:
        print("Creating New Connectors")
        print(e)
        error = e
        create_new_connectors()

    
    return jsonify({'error': str(error)}), 500

@app.route('/api/orders/new', methods=["POST"])
def new_order():
    #This route creates a new order
    attempt = 0
    error = ""
    print("attempt: " + str(attempt))
    try:
        data = request.get_json()
        order = orderManager.create_new_order(data)

        print("Successfully Created Order")
        return jsonify(order)
    except Exception as e:
        print("Creating New Connectors")
        print(e)
        error = e
        create_new_connectors()
        attempt += 1
    
    return jsonify({'error': str(error)}), 500

@app.route("/", defaults={'path': ''})
def serve(path):
    #This route serves the frontend
    return app.send_static_file('index.html')

create_new_connectors()

