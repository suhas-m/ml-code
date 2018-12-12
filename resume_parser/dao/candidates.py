# -*- coding: utf-8 -*-
import logging
class candidates :
    def __init__(self, connection):
        self.connection = connection
    
    #Create User
    def create(self, row, fileId):
        query = "INSERT INTO candidates(id, file_id, name, email, mobile, resume_path, creationdate) VALUES (null, %s, %s, %s, %s, %s, now())"
        data = (fileId, str(row['candidate_name']), str(row['email']), str(row['phone']), str(row['resume_path']))
        cursor = self.connection.executeQuery(query, data)
        if (cursor != False) :
            return cursor.lastrowid
        else :
            return False
    
    #Update User
    def update(self, row, fileId, userId):
        query = "UPDATE candidates SET file_id = %s, name = %s ,  mobile = %s, resume_path= %s WHERE id = %s"
        data = (fileId, str(row['candidate_name']), str(row['phone']), str(row['resume_path']), userId)
        cursor = self.connection.executeQuery(query, data)
        if (cursor != False) :
            return cursor.lastrowid
        else :
            return False
        
        
    #Get User   
    def find(self, params = None) :
        where = ""
        data = []
        if (params is not None) :
            for param in params:
                where += " "+param+" = %s" 
                data.append(str(params[param]))
        if (where != "") :
            where = "WHERE "+where
        query = "SELECT * FROM candidates "+where
        data = tuple(data)
        cursor = self.connection.executeQuery(query, data)
        if (cursor != False) :
            return cursor
        else :
            return False
            # -*- coding: utf-8 -*-

