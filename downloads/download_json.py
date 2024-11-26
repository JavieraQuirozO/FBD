#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  2 15:51:01 2024

@author: javicolors
"""

import pandas as pd
import gzip
import re
import json

class Download_json():
                
    def open_file_json(self, file_path):
        
        a = []

        if re.search(r'gz', file_path):
            
            try: 
                with gzip.open(file_path, 'rt') as file:
                    d = json.load(file)
                    d_ = d['data']
                    data = pd.DataFrame(d_)
                    a.append(data)
                   
            except:
                try:
                    with gzip.open(file_path, 'rt') as file:
                        return json.load(file)
                except:
                    print('Failed to download the file') 
            
        
        return a[0]
