#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  3 12:30:45 2024

@author: javicolors
"""


import gzip
import csv
import pandas as pd
import re

class Download_fb:
    def open_fb(self, file_path, start_line, columns):
        try:
            # Leer el archivo (soporte para gzip y texto plano)
            if re.search(r'gz$', file_path):
                with gzip.open(file_path, 'rt') as file:
                    reader = csv.reader(file, delimiter='\t')
                    data = list(reader)
            else:
                with open(file_path, 'r') as file:
                    reader = csv.reader(file, delimiter='\t')
                    data = list(reader)

            # Filtrar y procesar las filas
            cleaned_data = [
                row[:len(columns)] if len(row) > len(columns)  # Truncar filas largas
                else row + [""] * (len(columns) - len(row))    # Rellenar filas cortas
                for row in data[start_line:]
            ]

            # Crear el DataFrame
            df = pd.DataFrame(cleaned_data, columns=columns)
            return df

        except Exception as e:
            raise ValueError(f"Error al procesar el archivo: {e}")
