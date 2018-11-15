class files_processed :
    def __init__(self, connection):
        self.connection = connection
        
    
    def create(self, remote_id, batch_id):
        query = "INSERT INTO files_processed(id, remote_id, batch_id, creationdate) VALUES (null, %s, %s,  now())"
        data = (str(remote_id), batch_id)
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