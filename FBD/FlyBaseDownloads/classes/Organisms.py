#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 17:33:17 2023

@author: usuario
"""


from FlyBaseDownloads.downloads.Downloads import Downloads 
from FlyBaseDownloads.utilities.internet import Check_internet

class Organisms():
    
    def __init__(self):
        self.org_url = 'species/organism_list_fb*.tsv.gz'
        self.header = 4
    
    def Species_list(self):
        
        connection_ =  Check_internet.check_internet_connection(msg=False)
        downloads = Downloads(self.org_url, connection_)
        
        return downloads.get(self.header)
