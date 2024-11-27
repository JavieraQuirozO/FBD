#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 17:28:43 2023

@author: usuario
"""


from FlyBaseDownloads.downloads.Downloads import Downloads 
from FlyBaseDownloads.utilities.internet import Check_internet


class Homologs():
    
    def __init__(self):
        self.gen_url = 'orthologs/'
        
    def get(self):
        
        url = self.gen_url + self.un_url
        connection_ =  Check_internet.check_internet_connection(msg=False)
        downloads = Downloads(url, connection_)
        
        return downloads.get(self.header)
        
    def Drosophila_Paralogs(self):
        self.un_url = 'dmel_paralogs_fb_*.tsv.gz'
        self.header = 4
        return self.get()
    
    def Human_Orthologs(self):
        self.un_url = 'dmel_human_orthologs_disease_fb_*.tsv.gz'
        self.header = 3
        return self.get()
