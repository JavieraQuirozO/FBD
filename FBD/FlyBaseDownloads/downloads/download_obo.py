#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  2 15:58:58 2024

@author: javicolors
"""


import gzip
import re
import obonet

class Download_obo():
    
                    
    def open_obo(self, file_path):
    
        a = []
        
        try:
            if re.search(r'gz', file_path):
                with gzip.open(file_path, 'rt') as file:
                   if re.search(r'obo', file_path):
                       
                      graph = obonet.read_obo(file)
                      
                      a.append(graph)

            return a[0]
        
        except:
            print('Failed to download the file') 
