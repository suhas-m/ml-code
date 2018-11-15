# -*- coding: utf-8 -*-
import logging
import mysql.connector
class skills :
    def __init__(self, connection):
        self.connection = connection
    
    def create(self, row, userId):
        sql = "INSERT INTO skills(id, user_id, skills, experience, creationdate) VALUES (null, %s, %s, %s, now()) ON DUPLICATE KEY UPDATE skills = %s,  experience = %s"
        data = (userId, str(row['skills']), str(row['experience']), str(row['skills']), str(row['experience']))
        cursor = self.connection.executeQuery(sql, data)
        if (cursor != False) :
            return cursor.lastrowid
        else :
            return False
            
            # -*- coding: utf-8 -*-

