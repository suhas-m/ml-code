#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 15 14:50:13 2018

@author: suhasm
"""

from flask import Flask, jsonify, request
#from flask_restful import Resource, Api
import resume_parsing
import logging
import sys
sys.path.append('../')
from dao.db_connection import db_connection
from utils.common_utils import common_utils
utils = common_utils()
config = utils.loadConfig()
connection = db_connection(config)
from dao.batch import batch
app = Flask(__name__)
#api = Api(app)

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
    result = resume_parsing.processResumes(connection)
    return jsonify(result)

 

if __name__ == '__main__':
    logging.basicConfig(filename='/var/log/others/lafarge-error.log',level=logging.INFO)
    app.run(debug=True, use_reloader = True)