import json
import os
import shutil
import time
import traceback
import requests

from flask import Flask, render_template, request, Markup
from flask import Flask, jsonify, request
from flask_cors import CORS
from dag_trigger import trigger_dag

# Initialize Flask app
app = Flask(__name__)
CORS(app)


@app.route('/', methods=['GET'])
def success():

    title = 'Global Terrorism Analytics'
    return render_template('index.html', title=title)




@app.route('/trigger_dag', methods=['POST'])
def trigger_dag_run():
    # Fetching the variables from the post request
    payload = {
    "client_id" :"72714827302-hc202kf7r5osg4bp24v2e0mhc3enntdd.apps.googleusercontent.com",
    "webserver_id": "u167ea46d2daf2d8bp-tp",
    "dag_name":"master_deploy_dag",
    }

    try:
        print(f'Payload fetched as: {payload}')
        response, resp_status_code = trigger_dag(payload)

        return jsonify({
            "dag id": payload.get('dag_name'),
            "dag_trigger_response": response,
            "dag_trigger_status": resp_status_code
        })

    except Exception as err:
        print(f"Exception occured as {err}")
        print(traceback.format_exc())

        return jsonify({"error_msg", err})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=True)
