#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 17:32:48 2023

@author: usuario
"""

from FlyBaseDownloads.downloads.Downloads import Downloads 
from FlyBaseDownloads.utilities.internet import Check_internet

class Ontology_Terms():
    
    def __init__(self):
        self.go_url = 'ontologies/'
        self.header = None
        
    def get(self):
        
        url = self.go_url + self.un_url
        connection_ =  Check_internet.check_internet_connection(msg=False)
        downloads = Downloads(url, connection_)
        
        return downloads.get(self.header)
        
    def FBbt(self):
        self.un_url = 'fly_anatomy.obo.gz'
        return self.get()
    
    def FBdv(self):
        self.un_url = 'fly_development.obo.gz'
        return self.get()
    
    def FBcv(self):
        self.un_url = 'flybase_controlled_vocabulary.obo.gz'
        return self.get()
    
    def FBsv(self):
        self.un_url = 'flybase_stock_vocabulary.obo.gz'
        return self.get()
    
    def GO(self):
        self.un_url = 'go-basic.obo.gz'
        return self.get()
    
    def FBbi(self):
        self.un_url = 'image.obo.gz'
        return self.get()
    
    def DO(self):
        self.un_url = 'so-simple.obo.gz'
        return self.get()
