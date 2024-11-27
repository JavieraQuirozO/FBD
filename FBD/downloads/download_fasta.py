#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Javiera Quiroz Olave
"""
import pandas as pd
import gzip
import re

class Download_fasta():
    
                
    def open_fasta(self, file_path):
        def parse_header(header):
            fields = re.findall(r'(\w+)=(\S+)', header)
            return dict(fields)

        if re.search(r'gz', file_path):
            
            try: 
                sequences = []
                with gzip.open(file_path, 'rt') as file:
                    lines = file.readlines()
                    current_entry = {}
                
                    for line in lines:
                        line = line.strip()
                        if line.startswith(">"):
                            if current_entry:
                                sequences.append(current_entry)
                            current_entry = {"header": line[1:]}
                        else:
                            current_entry.setdefault("sequence", []).append(line)
                
                    # Añadir la última entrada
                    if current_entry:
                        sequences.append(current_entry)
                
                # Crear un DataFrame a partir de la lista de entradas procesadas
                df_data = []
                
                for entry in sequences:
                    header_info = parse_header(entry["header"])
                    sequence = "".join(entry["sequence"])
                    cleaned_header_info = {key: value.replace(";", "") for key, value in header_info.items()}
                    cleaned_sequence = sequence.replace(";", "")
                
                    entry_data = {**cleaned_header_info, "sequence": cleaned_sequence}
                    df_data.append(entry_data)
                
                # Crear DataFrame
                return  pd.DataFrame(df_data)

                   
            except:
                print('Failed to download the file') 
                
        else:
            print('Failed to download the file') 
            return None
    
    
