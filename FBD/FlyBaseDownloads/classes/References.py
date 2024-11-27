#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 17:35:38 2023

@author: usuario
"""

from FlyBaseDownloads.downloads.Downloads import Downloads 
from FlyBaseDownloads.utilities.internet import Check_internet

class References():
    
    def __init__(self):
        self.org_url = 'references/fbrf_pmid_pmcid_doi_fb*.tsv.gz'
        self.header = 2
        
    
    def FBrf_PMid_PMCid_doi(self):
        
        url = self.org_url
        connection_ =  Check_internet.check_internet_connection(msg=False)
        downloads = Downloads(url, connection_)
        df = downloads.get(self.header)
        if df is not None:
            return df.iloc[1:, :]
