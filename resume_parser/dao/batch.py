# -*- coding: utf-8 -*-
import logging


class batch :
    def __init__(self, connection):
        self.connection = connection
    
    def create(self, files_to_process, already_processed):
        query = "INSERT INTO batch(id, files_to_process, already_processed, status, creationdate) VALUES (null, %s, %s, 'pending', now())"
        data = (files_to_process, already_processed)
        cursor = self.connection.executeQuery(query, data)
        if (cursor != False) :
            return cursor.lastrowid
        else :
            return False
            
    def getDetails(self, batch_id):
        query = "SELECT u.email, u.mobile, u.name, s.skills, s.experience FROM users u LEFT JOIN skills s ON u.id = s.user_id INNER JOIN files_processed f ON f.id = u.file_id INNER JOIN batch b ON b.id = f.batch_id WHERE b.id = "+batch_id
        data = (batch_id)
        cursor = self.connection.executeQuery(query, data)
        if (cursor != False) :
            return cursor.fetchall()
        else :
            return False
        
    def update(self, batch_id, status):
        query = "UPDATE batch SET status = %s WHERE id = %s"
        data = (status, batch_id)
        cursor = self.connection.executeQuery(query, data)
        if (cursor != False) :
            return cursor.lastrowid
        else :
            return False
        
    def find(self, start, limit):
        query = "SELECT * FROM batch LIMIT "+start+", "+limit
        data = None
        cursor = self.connection.executeQuery(query, data)
        if (cursor != False) :
            return cursor.fetchall()
        else :
            return False