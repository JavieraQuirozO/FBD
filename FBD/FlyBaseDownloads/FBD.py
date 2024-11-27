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
from .utilities import Authentication, Check_internet, ConfigManager

class FBD():
    
    def __name__(self):
        self.__name__ = 'flybase downloads'
    
    def __init__(self, username, password, email=None, msg=False):
        self.username = username
        self.email = email
        self.password = password
        
        # Cargar las variables de entorno desde .env usando ConfigManager
        self.config_manager = ConfigManager()
        
        # Verificar si tenemos conexión a internet
        internet = Check_internet.check_internet_connection(msg=msg)
        
        if internet:    
            auth = Authentication()
            if email is None:
                # Verificar usuario sin email
                auth.verify_user(username, password)
            else:
                # Crear o autenticar usuario con email
                auth.get_user(email=self.email, username=self.username, password=self.password)
        else:
            # Si no hay conexión a internet, sobrescribir el archivo .env con USER_ID = "00"
            Check_internet.check_internet_connection()
            self.config_manager.set("USER_ID", "00")  # Usamos el método de ConfigManager para actualizar el .env

        # Inicializar las clases
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


