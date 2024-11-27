#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 17:27:27 2023

@author: usuario
"""

from FlyBaseDownloads.downloads.Downloads import Downloads 
from FlyBaseDownloads.utilities.internet import Check_internet

class Gene_groups():
    
    def __init__(self):
        self.internet = Check_internet.check_internet_connection(msg=False)
        self.gen_url = 'genes/'
        
    def get(self):
        
        url = self.gen_url + self.un_url
        downloads = Downloads(url, self.internet)
        df = None
        df = downloads.get(self.header)
        
        try:
            df.columns = df.iloc[0]
            return df[1:].reset_index(drop=True)
        except:
            pass
        
    def Gene_group(self):
        self.un_url = 'gene_group_data_fb_*.tsv.gz'
        self.header = 6
        return self.get()
    
    def Gene_groups_HGNC(self):
        self.un_url = 'gene_groups_HGNC_fb_*.tsv.gz'
        self.header = 6
        return self.get()
        
    def Pathway_group(self):
        self.un_url = 'pathway_group_data_fb_*.tsv.gz'
        self.header = 6
        return self.get()
