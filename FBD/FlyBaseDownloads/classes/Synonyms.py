#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 17:04:57 2023

@author: usuario
"""

from FlyBaseDownloads.downloads.Downloads import Downloads 
from FlyBaseDownloads.utilities.internet import Check_internet

class Synonyms():
    
    def __init__(self):
        self.syn_url = 'synonyms/fb_synonym_*.tsv.gz'
        self.header = 3
        
    
    def get(self):
        
        url = self.syn_url
        internet = Check_internet.check_internet_connection(msg=False)
        downloads = Downloads(url, internet)
        return downloads.get(self.header)
