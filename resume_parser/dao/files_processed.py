class files_processed :
    def __init__(self, connection):
        self.connection = connection
        
    
    def create(self, remote_id, batch_id, folder_id):
        query = "INSERT INTO files_processed(id, remote_id, batch_id, folder_id, creationdate) VALUES (null, %s, %s,  %s,  now())"
        data = (str(remote_id), batch_id, folder_id)
        cursor = self.connection.executeQuery(query, data)
        if (cursor.lastrowid) :
            return cursor.lastrowid
        else :
            return False# -*- coding: utf-8 -*-

    def find(self, remote_id):
        query = "SELECT * FROM files_processed WHERE remote_id = '"+remote_id+"'";
        cursor = self.connection.executeQuery(query)
        if cursor != False :
            return cursor
        else :
            return False# -*- coding: utf-8 -*-
        
    def getDetails(self, folder_id, start, limit):
        query = "SELECT u.email, u.mobile, u.name, s.skills, s.experience FROM users u LEFT JOIN skills s ON u.id = s.user_id INNER JOIN files_processed f ON f.id = u.file_id  WHERE f.folder_id = '"+folder_id+"' LIMIT "+start+","+limit
        data = (folder_id)
        cursor = self.connection.executeQuery(query, data)
        if (cursor != False) :
            return cursor.fetchall()
        else :
            return False    