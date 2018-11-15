# -*- coding: utf-8 -*-
import logging
class users :
    def __init__(self, connection):
        self.connection = connection
    
    #Create User
    def create(self, row, fileId):
        query = "INSERT INTO users(id, file_id, name, email, mobile, creationdate) VALUES (null, %s, %s, %s, %s, now())"
        data = (fileId, str(row['candidate_name']), str(row['email']), str(row['phone']))
        cursor = self.connection.executeQuery(query, data)
        if (cursor != False) :
            return cursor.lastrowid
        else :
            return False
    
    #Update User
    def update(self, row, fileId, userId):
        query = "UPDATE users SET file_id = %s, name = %s ,  mobile = %s WHERE id = %s"
        data = (fileId, str(row['candidate_name']), str(row['phone']), userId)
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
        query = "SELECT * FROM users "+where
        data = tuple(data)
        cursor = self.connection.executeQuery(query, data)
        if (cursor != False) :
            return cursor
        else :
            return False
            # -*- coding: utf-8 -*-

