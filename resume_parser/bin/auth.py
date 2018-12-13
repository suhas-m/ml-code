#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 13 10:48:11 2018

@author: suhasm
"""
from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import requests 
# If modifying these scopes, delete the file token.json.

class auth:
    def __init__(self, config):
        self.SCOPES = config["google-drive"]["scope"]
        self.CREDENTIALS_FILE = config["google-drive"]["credential_file"]
        self.TOKEN_FILE = config["google-drive"]["token_file"]
        
    def storeAuthToken(self) :
        store = file.Storage(self.TOKEN_FILE)
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets(self.CREDENTIALS_FILE, self.SCOPES)
            creds = tools.run_flow(flow, store)
        
    def getCredentials(self):
        requests.post('https://accounts.google.com/o/oauth2/revoke',
                      params={'token': file.Storage(self.TOKEN_FILE)},
                      headers = {'content-type': 'application/x-www-form-urlencoded'})
        store = file.Storage(self.TOKEN_FILE)
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets(self.CREDENTIALS_FILE, self.SCOPES)
            creds = tools.run_flow(flow, store)
        return creds
    