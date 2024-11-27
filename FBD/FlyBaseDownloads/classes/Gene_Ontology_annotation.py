#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 17:25:55 2023

@author: usuario
"""


from FlyBaseDownloads.downloads.Downloads import Downloads 
from FlyBaseDownloads.utilities.internet import Check_internet

class Gene_Ontology_annotation():
    
    def __init__(self):
        self.internet = Check_internet.check_internet_connection(msg = False)
        self.go_url = 'go/'

        
    def GAF(self):
        self.un_url = 'gene_association.fb.gz'
        self.start = 5
        self.df_columns = ['DB', 'DB Object ID', 'DB Object Symbol',
                           'Qualifier', 'GO ID', 'DB:Reference',
                           'Evidence', 'With (or) From', 'Aspect',
                           'DB Object Name', 'DB Object Synonym', 'DB Object Type',
                           'taxon', 'Date', 'Assigned by']
        return self.get()
    
    def GPI(self):
        self.un_url = 'gp_information.fb.gz'
        self.start = 0
        self.df_columns = ['DB', 'DB Object ID', 'DB Object Symbol',
                           'DB Object Name', 'DB Object Synonym', 'DB Object Type',
                           'Taxon', 'Parent Object ID', 'DB Xref(s)',
                           'Properties']
        return self.get()
    
        
        
    def get(self):
        
        url = self.go_url + self.un_url
        downloads = Downloads(url, self.internet)
        file = None
        file = downloads.download_file()
        if file is not None:
            df = downloads.open_fb(file, self.start, self.df_columns)
            return df
