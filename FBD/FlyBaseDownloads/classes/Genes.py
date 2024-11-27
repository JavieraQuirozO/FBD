#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 17:23:24 2023

@author: usuario
"""

from FlyBaseDownloads.downloads.Downloads import Downloads 
from FlyBaseDownloads.utilities.internet import Check_internet

class Genes():
    
    def __init__(self):
        self.gen_url = 'genes/'
        self.header = None
        
        self.internet =  Check_internet.check_internet_connection(msg=False)
        
    def Genetic_interaction_table(self):
        self.un_url = 'gene_genetic_interactions_fb_*.tsv.gz'
        self.header = 3
        return self.get()
        
    def RNASeq_values(self):
        self.un_url = 'gene_rpkm_report_fb_*.tsv.gz'
        self.header = 5
        return self.get()
        
    def RNASeq_values_matrix(self):
        self.un_url = 'gene_rpkm_matrix_fb_*.tsv.gz'
        self.header = 4
        return self.get()       
        
    def Single_Cell_RNA_Gene_Expression(self):
        self.un_url = 'scRNA-Seq_gene_expression_fb_*.tsv.gz'
        self.header = 7
        return self.get()
        
    def Physical_interaction_MITAB(self):
        self.un_url = 'physical_interactions_mitab_fb_*.tsv.gz'
        self.header = 0
        return self.get()
                
    def Functional_complementation(self):
        self.un_url = 'gene_functional_complementation_*.tsv.gz'
        self.header = 4
        return self.get()
    
    def FBgn_toDB_Accession_IDs(self):
        self.un_url = 'fbgn_NAseq_Uniprot_*.tsv.gz'
        self.header = 4
        return self.get()
    
    def FBgn_toAnnotation_ID(self):
        self.un_url = 'fbgn_annotation_ID_*.tsv.gz'
        self.header = 3
        return self.get()
    
    def FBgn_toGLEANR_IDs(self):
        self.un_url = 'fbgn_gleanr_*.tsv.gz'
        self.header = 3
        return self.get()
    
    def FBgn_to_FBtr_to_FBpp(self):
        self.un_url = 'fbgn_fbtr_fbpp_*.tsv.gz'
        self.header = 4
        return self.get()
    
    def FBgn_to_FBtr_to_FBpp_expanded(self):
        self.un_url = 'fbgn_fbtr_fbpp_expanded_*.tsv.gz'
        self.header = 4
        return self.get()
    
    def FBgn_exons2affy1(self, to_dict = False):
        self.un_url = 'fbgn_exons2affy1_overlaps.tsv.gz'
        return self.get_affy(to_dict=to_dict)
    
    def FBgn_exons2affy2(self, to_dict = False):
        self.un_url = 'fbgn_exons2affy2_overlaps.tsv.gz'
        return self.get_affy(to_dict=to_dict)
        
    def Genes_Sequence_Ontology(self):
        self.un_url = 'dmel_gene_sequence_ontology_annotations_fb_*.tsv.gz'
        self.header = 4
        return self.get()
    
    def Genes_map(self):
        self.un_url = 'gene_map_table_*.tsv.gz'
        self.header = 3
        return self.get()
        
    
    def Best_gene_summaries(self):
        self.un_url = 'best_gene_summary*.tsv.gz'
        self.header = 8
        return self.get()
                
    def Automated_gene_summaries(self):
        self.un_url = 'automated_gene_summaries.tsv.gz'
        self.header = 1
        return self.get()
    
    def Gene_Snapshots(self):
        self.un_url = 'gene_snapshots_*.tsv.gz'
        self.header = 4
        return self.get()

        
    def Unique_protein_isoforms(self):
        self.un_url = 'dmel_unique_protein_isoforms_fb_*.tsv.gz'
        self.header = 3
        return self.get()
    
    def Noncoding_RNAs(self):
        self.un_url = 'ncRNA_genes_fb_*.json.gz'
        return self.get()
    
    def Enzyme(self):
        self.un_url = 'Dmel_enzyme_data_fb_*.tsv.gz'
        self.header = 4
        return self.get()

    
    def get(self):
        
        url = self.gen_url + self.un_url
        downloads = Downloads(url, self.internet)
        
        return downloads.get(self.header)
    
    def get_affy(self, to_dict):
        url = self.gen_url + self.un_url
        downloads = Downloads(url, self.internet)
        file = None
        
        file = downloads.download_file()
        
        if file is not None:
            return downloads.get_affy(file, to_dict)
        
  
