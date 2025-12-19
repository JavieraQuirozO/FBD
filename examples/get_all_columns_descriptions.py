#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from FBD.fbd import FBD
import pandas as pd


# =========================
# Init 
# =========================
fbd = FBD()

categories = fbd.get_categories()

# =========================
# Iterate datasets & collect columns
# =========================

rows = []

for category in categories:
    datasets = fbd.get_files_by_category(category)

    for dataset in datasets:
        try:
            print(f"\n================ {dataset.upper()} ================")

            # set + download
            fbd.set_dataset(dataset)
            data = fbd.download_file()

            print(data.head())

            # metadata
            try:
                column_descriptions = fbd.get_column_descriptions()
                for col, desc in column_descriptions.items():
                    print(f"{col}: {desc}")

                cols = list(data.columns)

            except Exception as e:
                print(f"Error leyendo columnas en {dataset}: {e}")
                cols = None

            rows.append({
                "dataset": dataset,
                "columns": cols
            })

        except Exception as e:
            print(f"{dataset}: Error: {e}")
            continue


# =========================
# Final DataFrame
# =========================

df_columns = pd.DataFrame(rows)

for columns in df_columns["columns"]:
    print(columns)
