#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  2 15:39:24 2024

@author: javicolors
"""
import pandas as pd
import gzip
import re
import csv

class Download_tsv():
    
    def open_file_tsv(self, file_path, header):
        a = []
    
        if re.search(r'gz', file_path):
            
            try: 
                with gzip.open(file_path, 'rt') as file:
                   return pd.read_csv(file, sep='\t', header=header, low_memory=False)
            
            except Exception:                 
                with gzip.open(file_path, 'rt') as file:
                    df = csv.reader(file, delimiter='\t')
                    a.append(pd.DataFrame(df))
            
                df = a[0]
                columns = df.iloc[header, :].tolist()
    
                df = df.iloc[header + 1:, :]
                
                df.columns = columns
                df = df.dropna(axis='columns')
              

                return (df)
                
            except:
                print('Failed to download the file') 
    
    def get_affy(self, file_path, to_dict = False):
        if re.search(r'gz', file_path):
            
            try: 
                a = []
                with gzip.open(file_path, 'rt') as file:
                    df = pd.read_csv(file, sep='\t', engine='python-fwf')
                    a.append(pd.DataFrame(df))
            
                df = a[0]
                
        
                #%%
        
                if to_dict:
                    dict_affy = {}
                    for i in df.index:
                       dict_affy[df.iloc[i, 0]] = list(df.iloc[i, 1:].dropna())
                    return dict_affy
                return df
            except:
                print('Failed to download the file')
        
        
                    
