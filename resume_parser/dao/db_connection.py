#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 13 17:15:23 2018

@author: suhasm
"""

import mysql.connector
import logging
import re

class db_connection :
    def __init__(self, config) :
        self.config = config
        self.mydb = None
    
    def getConnection(self) :
        if self.mydb is not None  :
            #print("Return connection 1")    
            return self.mydb
        else :
            try :
                #print("Config:", self.config['mysql'])
                mydb = mysql.connector.connect(
                    host = self.config['mysql']['host'],
                    user = self.config['mysql']['user'],
                    passwd = self.config['mysql']['password'],
                    database = self.config['mysql']['db']
                ) 
                #print("Inside connection 1")
                self.mydb = mydb            
            except Exception as e:
                #print("Inside connection 2")
                print(e)
        #print("Return connection 2")
        return self.mydb
    
    def executeQuery(self, sql, data = None) :
        try:
            self.getConnection()
            logging.info("Inside DB Connection======>"+sql)
            mycursor = self.mydb.cursor(buffered=True, dictionary=True)
            
            mycursor.execute(sql, data)
            
            logging.info("Query======>"+mycursor.statement)
            arr = ['INSERT', 'UPDATE', 'DELETE']
            #r = re.compile(r'\bINSERT \b | \bUPDATE\b | \bDELETE\b', flags = re.I)
            if (any(re.findall('|'.join(arr), sql))) :
                self.mydb.commit()
            
            return mycursor
        except Exception as error:
            logging.error("Error executing query"+str(error))
            return False
            
            