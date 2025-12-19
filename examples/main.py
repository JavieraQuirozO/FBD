#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from FBD.fbd import FBD

fbd = FBD()

categories = fbd.get_categories()
print(categories)


#%%

files_by_cat = fbd.get_files_by_category("Genes")
print(files_by_cat)

#%%

fbd = FBD()
dataset = fbd.search_file("gene_genetic_interactions")
print("Dataset seleccionado:", dataset)


#%%

matches = fbd.search_file("gene")
print("Posibles datasets:")
for m in matches:
    print("-", m)


#%%

fbd = FBD()

fbd.set_dataset("antibody_information")
df1 = fbd.download_file()

fbd.set_dataset("gene_genetic_interactions")
df2 = fbd.download_file()

#%%

from FBD.fbd import FBD

fbd = FBD("gene_genetic_interactions")

df = fbd.download_file()

column_info = fbd.get_column_descriptions( columns= ["Starting_gene(s)_symbol", "Starting_gene(s)_symbol",
                                                     "Interaction_type"]
)


X = df[["Starting_gene(s)_symbol", "Starting_gene(s)_symbol"]]
y = df["Interaction_type"]


