#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Adaptado para manejar autenticación mediante el backend.
"""

import requests
import os
from dotenv import load_dotenv, set_key

# Cargar las variables de entorno desde el archivo `.env`
load_dotenv()  # Comenta esta línea si no estás usando un archivo .env

# Obtener la URL base desde las variables de entorno
BASE_URL = os.getenv("BASE_URL")

if not BASE_URL:
    raise ValueError("La variable de entorno BASE_URL no está configurada.")



class Authentication:
    def __init__(self):
        """
        Inicializa la clase con la URL base del backend.
        """
        self.base_url = BASE_URL
        

    def get_user(self, username, password, email = None):
        """
        Crea un usuario nuevo o recupera uno existente mediante el backend.
        """
        endpoint = f"{self.base_url}/users/"
        data = {"email": email, "username": username, "password": password}

        response = requests.post(endpoint, json=data)

        if response.status_code == 200:
            user_id = response.json().get("id")
            print("Usuario creado exitosamente")
            set_key(".env", "USER_ID", str(user_id))
            
        elif response.status_code == 400:  # Usuario ya existe
            print("El usuario ya existe. Intentando autenticar...")
            return self.verify_user(username, password)
        else:
            print(f"Error al registrar o autenticar el usuario: {response.status_code} - {response.text}")
            set_key(".env", "USER_ID", "00")
            return None

    def verify_user(self, username, password):
        """
        Verifica las credenciales del usuario mediante el backend.
        """
        endpoint = f"{self.base_url}/login/"
        data = {"username": username, "password": password}

        response = requests.post(endpoint, json=data)

        if response.status_code == 200:
            user_id = response.json().get("user_id")
            print("Autenticación exitosa")
            set_key(".env", "USER_ID", str(user_id))
        else:
            print(f"Error de autenticación: {response.status_code} - {response.text}")
            set_key(".env", "USER_ID", "00")
            return None

