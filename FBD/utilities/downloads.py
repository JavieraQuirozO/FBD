#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Adaptado para manejar descargas mediante el backend.
"""

import requests
import os
from dotenv import load_dotenv, dotenv_values


class RTD:
    def __init__(self):
        """
        Inicializa la clase con el ID del usuario y la URL base del backend.
        """
        config = dotenv_values(".env")  # Carga las variables como un diccionario
        self.user_id = config.get("USER_ID")
        load_dotenv()  # Comenta esta línea si no estás usando un archivo .env
        self.base_url = os.getenv("BASE_URL")

    def save_reg(self, file_path):
        """
        Guarda un registro de descarga para el usuario en el backend.
        """
        endpoint = f"{self.base_url}/downloads/"
        
        data = {"user_id": self.user_id, "filename": file_path}

        response = requests.post(endpoint, json=data)
        if response.status_code != 200:
            print(f"Error al registrar la descarga: {response.status_code} - {response.text}")

    def def_reg(self):
        """
        Elimina todos los registros de descargas asociados al usuario.
        """
        
        endpoint = f"{self.base_url}/downloads/{self.user_id}"
        response = requests.delete(endpoint)

        if response.status_code != 200:
            print(f"Error al eliminar los registros: {response.status_code} - {response.text}")


