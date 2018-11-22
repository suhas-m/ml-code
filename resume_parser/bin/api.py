#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 15 14:50:13 2018

@author: suhasm
"""
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import auth

from flask import Flask, jsonify, request
#from flask_restful import Resource, Api
import resume_parsing
import logging
import sys
import asyncio

import threading
sys.path.append('../')
from dao.db_connection import db_connection
from utils.common_utils import common_utils
utils = common_utils()
config = utils.loadConfig()
connection = db_connection(config)
from dao.batch import batch
app = Flask(__name__)
#api = Api(app)
SCOPES = 'https://www.googleapis.com/auth/drive.readonly'
CREDENTIALS_FILE = 'credentials.json'
@app.route('/batch', methods=['GET'])
def getBatches():
    start = request.args.get('start')
    limit = request.args.get('limit')
    batchObj = batch(connection)
    batches = batchObj.find(start, limit)
    return jsonify(batches)   

@app.route('/batch/<string:batch_id>', methods=['GET'])
def getBatchDetails(batch_id) :
    batchObj = batch(connection)
    users = batchObj.getDetails(batch_id)
    return jsonify(users)

@app.route('/batch', methods = ['POST'])
def createBatch():
    req_data = request.get_json()
    drive_folder = req_data['folder_name']
    logging.info("Folder name:",drive_folder)
    t = threading.Thread(target = resume_parsing.processResumes, args = (connection, drive_folder))
    t.start()
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(resume_parsing.processResumes(connection, drive_folder))
    #asyncio.set_event_loop(loop)
    loop.run_until_complete
    loop.run_until_complete(asyncio.gather(task))
    """
    return jsonify({'status': 'success', 'data':'Resume parsing is in process'})

@app.route('/upload-credentials', methods=['POST'])
def storeCredentials() :
    
    logging.info(request.files)
    input_file = request.files['cred_file']
    if input_file :
        input_file.save("credentials.json")
        return jsonify({'status': 'success'})
    else :
        return jsonify({'status':'failure'})
    


@app.route('/auth-token', methods = ['GET'])
def storeAuthToken() :
    authInst = auth.auth(SCOPES, CREDENTIALS_FILE)
    creds = authInst.getCredentials()
    return jsonify({'status': 'success'})

if __name__ == '__main__':
    logging.basicConfig(filename='/var/log/others/lafarge-error.log',level=logging.INFO)
    app.run(debug=True, use_reloader = True)