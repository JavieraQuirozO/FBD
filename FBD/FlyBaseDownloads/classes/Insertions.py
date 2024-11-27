#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 17:34:00 2023

@author: usuario
"""


from FlyBaseDownloads.downloads.Downloads import Downloads 
from FlyBaseDownloads.utilities.internet import Check_internet

class Insertions():
    
    def __init__(self):
        self.in_url = 'insertions/'
        self.header = 0
        
    def GAL4_drivers(self):
        self.un_url = 'fu_gal4_table_fb_*.json.gz'
        df = self.get()
        
        return df
        
        
    
    def Map_insertions(self):
        self.un_url = 'insertion_mapping_fb_*.tsv.gz'
        self.header = 3
        return self.get()
        
    def get(self):
        url = self.in_url + self.un_url
        connection_ =  Check_internet.check_internet_connection(msg=False)
        downloads = Downloads(url, connection_)
        
        return downloads.get(self.header)
