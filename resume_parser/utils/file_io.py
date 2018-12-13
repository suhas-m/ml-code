#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 13 10:54:38 2018

@author: suhasm
"""

from __future__ import print_function
import os, io
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from apiclient.http import MediaIoBaseDownload
import boto
import boto.s3
from boto.s3.key import Key
from boto.s3.connection import S3Connection
import subprocess


import logging

class file_io:
    def __init__(self, drive_service) :
        self.drive_service = drive_service
    #Function to list files in folder
    def listFiles(self, folderId, size) :
        
        results = self.drive_service.files().list(q = "'"+folderId+"' in parents ",
        pageSize=size, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])
    
        if not items:
            print('No files found.')
        else:
            print('Files:')
            for item in items:
                print(u'{0} ({1})'.format(item['name'], item['id']))
    
    #function to create folder
    def createFolder(self, name):
        file_metadata = {
            'name' : name,
            'mimeType' : 'application/vnd.google-apps.folder'            
        }
        response = self.drive_service.files().create(body = file_metadata, fields = 'id').execute()
        folderId = response.get('id')
        
        return folderId;        
    
    #Function to get folderId from foldername
    def getFolderId(self, name):
        try :
            response = self.drive_service.files().list(q= "name = '"+name+"' AND mimeType = 'application/vnd.google-apps.folder'", 
                                               fields = "nextPageToken, files(id, name)",
                                               pageSize= 1).execute()
            folderId = None
            for file in response.get('files', []):
                folderId = file.get('id')
                print ('Found file: %s (%s)' % (file.get('name'), file.get('id')))
        except Exception as e:
                logging.error("Error in getting file list from folder:"+str(e))         
        #print('Folder Id: %s' % files[0].get('id'))
        return folderId;
    
    
    #function to download single file from id    
    def downloadSingleFile(self, fileId, destinationPath) :
            print("Destination Path:", destinationPath)
            try :
                request = self.drive_service.files().get_media(fileId=fileId)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    print ("Download %d%%." % int(status.progress() * 100))
                with io.open(destinationPath, 'wb') as f :
                    fh.seek(0)
                    f.write(fh.read())
            except Exception as e:
                logging.error("Error in downloading file:"+str(e))         

    #function to get files from folder with given folderId
    def downloadFiles(self, folderId, destinationPath, pageSize) :
        page_token = None
        remoteIds = {}
        while True :
            results = self.drive_service.files().list(q = "'"+folderId+"' in parents ", pageSize=pageSize, fields="nextPageToken, files(id, name)").execute()
            print(results)
            for file in results.get('files', []):
                self.downloadSingleFile(file.get('id'), destinationPath+file.get('name'))  
                remoteIds[file.get('id')] = file.get('name')
                
            page_token = results.get('nextPageToken', None)
            if page_token is None:
                break
        
        return remoteIds
    
    #function to get files from folder with given folderId
    def getAllFiles(self, folderId, pageSize = 10) :
        page_token = None
        remoteIds = {}
        while True :
            results = self.drive_service.files().list(q = "'"+folderId+"' in parents ", pageSize=pageSize, fields="nextPageToken, files(id, name)").execute()
            print(results)
            for file in results.get('files', []):
                #self.downloadSingleFile(file.get('id'), destinationPath+file.get('name'))  
                remoteIds[file.get('id')] = file.get('name')
                
            page_token = results.get('nextPageToken', None)
            if page_token is None:
                break
        
        return remoteIds  

    def renameFile(self, sourcePath, destinationPath) :
        os.rename(sourcePath, destinationPath);
    
    def uploadToS3(self, sourcePath, filename, config) :
        
        
       
        logging.info("File path=======>"+str(sourcePath))
        #response = subprocess.call("aws s3 cp '"+sourcePath+"' s3://"+config['aws']['resume_bucket_name']+"/")
        
        out = subprocess.Popen(['aws', 's3', 'cp', sourcePath, 's3://'+config['aws']['resume_bucket_name']+'/'], 
           stdout=subprocess.PIPE, 
           stderr=subprocess.STDOUT)
        stdout,stderr = out.communicate()
        
        logging.info("S3 upload response:"+str(stdout))
        """
        #Commented as we are executing with shell command
        
        #conn = boto.connect_s3(config['aws']['access_key'], config['aws']['secret_key'], )
        conn = S3Connection(config['aws']['access_key'], config['aws']['secret_key'], host = config['aws']['region_host'])
        logging.info("S3 Connection:"+str(conn))
        #bucket = conn.create_bucket(bucket_name, location=boto.s3.connection.Location.DEFAULT)
        try :
            bucket = conn.get_bucket(config['aws']['resume_bucket_name']);
            logging.info("S3 Bucket=======>"+str(bucket))
        except Exception as e:
            logging.error("S3 Bucket Error:"+str(e))
            error = e.split(':')
            #if (error[1] == "404 Not Found"):
            try :
                bucket = conn.create_bucket(config['aws']['resume_bucket_name'], location=boto.s3.connection.Location.DEFAULT) 
                logging.info("S3 Bucket 2=======>"+str(bucket))
            c         
        
        #key = Key(bucket, sourcePath)
        key = Key(bucket)
        with open(sourcePath, 'r', encoding = "ISO-8859-1") as f:
            key.send_file(f.read())
        #End commit
        """    
        return "s3://"+config['aws']['resume_bucket_name']+"/"+filename;    
            
    #function to clean input files directory
    def cleanInputDir(self, dirPath) :
        for file in os.listdir(dirPath):
            file_path = os.path.join(dirPath, file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                
            except Exception as e:
                print(e)
                
                
                    