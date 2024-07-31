from flask import Flask, jsonify, request, send_file
from src.scraper.logger import logging
from src.scraper.database import create_database, create_tables


app = Flask(__name__)


## this api is responsible for creating a database
@app.route('/create_database', methods=['POST'])
def create_database_api():
    try:
        data = request.get_json()
        host = data.get('host')
        user = data.get('user')
        password = data.get('password')

        result = create_database(host, user, password)
        return jsonify(result, {"status": "success", "message": "DB created successfully"})
    except Exception as e:
        logging.error(f"Error in processing request: {e}")
        return jsonify({"status": "error", "message": "Internal Server Error", "error": str(e)}), 500


## the below API is responsible for create tables
@app.route('/create_tables', methods=['POST'])
def create_tables_api():
    try:
        data = request.get_json()
        host = data.get('host')
        user = data.get('user')
        password = data.get('password')
        database = data.get('database')

        result = create_tables(host, user, password, database)
        return jsonify(result, {"status": "success", "message": "Tables created successfully"})
    except Exception as e:
        logging.error(f"Error in processing request: {e}")
        return jsonify({"status": "error", "message": "Internal Server Error", "error": str(e)}), 500
    

if __name__ == "__main__":
    app.run(debug=True)
