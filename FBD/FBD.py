#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  2 14:19:10 2024

@author: javicolors
"""

"""

Unofficial FlyBase Database wrapper

"""

from .classes import *
from .utilities import Authentication, Check_internet
from dotenv import load_dotenv, set_key

class FBD():
    
    def __name__(self):
        self.__name__ = 'flybase downloads'
    
    def __init__(self, username, password, email = None, msg = False):
        
        self.username = username
        self.email = email
        self.password = password
        
        load_dotenv()
        
        internet = Check_internet.check_internet_connection(msg=msg)
        
        if internet:    
            auth = Authentication()
            if email is None:
                auth.verify_user(username, password)
            else:
                auth.get_user(email=self.email, username=self.username, password=self.password)
        else:
            Check_internet.check_internet_connection()
            set_key(".env", "USER_ID", "00")
        
        self.Synonyms = Synonyms()
        self.Genes = Genes()
        self.GOAnn = Gene_Ontology_annotation() 
        self.Gene_groups = Gene_groups()
        self.Homologs = Homologs()
        self.Ontology_Terms = Ontology_Terms()
        self.Organisms = Organisms()
        self.Insertions = Insertions()
        self.Clones = Clones()
        self.References = References()
        self.Alleles_Stocks = Alleles_Stocks()
        self.Human_disease = Human_disease()
        self.AnnSeq = AnnSeq()
        self.Map_conversion = Map_conversion()    

