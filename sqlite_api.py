
import pandas as pd

import sqlite3
from flask import Flask, request, jsonify
from sqlalchemy import create_engine
import db_operations

app = Flask(__name__)

@app.route('/help', methods=['GET'])
def run_help():
    helpfile = {"endpoints":
                    {'/query':
                         {'methods':'POST',
                          'payload':"{'sql':'sql_string','engine':'sqlite:///{db_name}.db",
                          'return':'returns json object of sql query result'},
                     '/update':
                         {'methods':'POST',
                          'payload':"{'table_name':'string', 'engine':'sqlite:///{db_name}.db, 'data':json of df",
                          'return':'NONE - updates existing table with new data'},
                     '/new_table':
                         {'methods': 'POST',
                          'payload': "{'table_name':'string', 'engine':'sqlite:///{db_name}.db, 'data':json of df",
                          'return': 'NONE - creates new table with df data'},
                     '/delete_table':
                         {'methods':'POST',
                          'payload': "{'table_name':'string', 'conn':'{db_name}.db",
                          'return': 'NONE - deletes table'}
                     }
                }
    return helpfile


@app.route('/query', methods=['POST'])
def run_query():

    if request.is_json:
        data = request.get_json()
        sql = data['sql']
        engine = create_engine(data['engine'])

        try:
            df = db_operations.read_sql(engine, sql)
        except:
            return jsonify({"status": "error", "data": "An error occurred processing your request ..."}), 500

        return jsonify({"status": "success", "data": df.to_json()}), 200
    else:
        return jsonify({"status": "error", "data": "Request must be JSON"}), 400


@app.route('/update', methods=['POST'])
def run_update():

    if request.is_json:
        data = request.get_json()

        table_name = data['table_name']
        engine = create_engine(data['engine'])
        df = pd.read_json(data['data'])

        db_operations.insert_records(engine, table_name, df)

        return jsonify({"status": "success", "message": f"table {table_name} updated"}), 200
    else:
        return jsonify({"status": "error", "message": "Request must be JSON"}), 400


@app.route('/new_table', methods=['POST'])
def run_new_table():

    if request.is_json:
        data = request.get_json()

        table_name = data['table_name']
        engine = create_engine(data['engine'])
        df = pd.read_json(data['data'])

        db_operations.make_table(engine, table_name, df)

        return jsonify({"status": "success", "message": f"table {table_name} created"}), 200
    else:
        return jsonify({"status": "error", "message": "Request must be JSON"}), 400


@app.route('/delete_table', methods=['POST'])
def run_delete_table():

    if request.is_json:
        data = request.get_json()

        table_name = data['table_name']
        conn = sqlite3.connect(data['conn'])

        db_operations.delete_table(conn, table_name)
        conn.close()
        return jsonify({"status": "success", "message": f"table {table_name} deleted or didn't exist"}), 200
    else:
        return jsonify({"status": "error", "message": "Request must be JSON"}), 400


@app.route('/sql', methods=['POST'])
def run_sql():

    if request.is_json:
        data = request.get_json()
        sql = data['sql']
        conn = sqlite3.connect(data['conn'])

        try:
            df = db_operations.execute_sql(conn, sql)
        except:
            return jsonify({"status": "error", "data": "An error occurred processing your request ..."}), 500
        if df is not None:
            return jsonify({"status": "success", "data": df.to_json()}), 200
        else:
            return jsonify({"status": "success", "data": "OK"}), 200
    else:
        return jsonify({"status": "error", "data": "Request must be JSON"}), 400


if __name__ == '__main__':

    app.run(host='0.0.0.0', port=5000)