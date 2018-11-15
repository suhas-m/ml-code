#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 13 17:23:36 2018

@author: suhasm
"""
import json

class common_utils :
    def __init__(self) :
        pass
    
    def loadConfig(self) :
        with open('../confs/config.json') as json_data_file:
            data = json.load(json_data_file)
        return data
        