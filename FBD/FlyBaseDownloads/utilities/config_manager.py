import os
from dotenv import dotenv_values, set_key
import pkg_resources

class ConfigManager:
    def __init__(self):
        """
        Inicializa la clase y carga las variables de entorno.
        """
        self._cargar_variables_entorno()

    def _cargar_variables_entorno(self):
        """
        Carga las variables de entorno desde el archivo .env incluido en el paquete.
        """
        # Obtener la ruta del archivo .env dentro del paquete
        env_path = pkg_resources.resource_filename(
            "FlyBaseDownloads", '.env'  
        )
        
        # Verificar si estamos en Google Colab y copiar el archivo .env temporalmente
        try:
            import google.colab  # Intentamos importar google.colab
            in_colab = True  # Si la importación tiene éxito, estamos en Colab
        except ImportError:
            in_colab = False  # Si no, no estamos en Colab

        if in_colab:
            from shutil import copy
            temp_env_path = '/content/.env'
            copy(env_path, temp_env_path)  # Copiar el archivo .env a Colab
            env_path = temp_env_path

        # Cargar las variables de entorno desde el archivo .env
        config = dotenv_values(env_path)  # Carga las variables de entorno como un diccionario

        # Establecer las variables de entorno para que estén disponibles globalmente
        for key, value in config.items():
            os.environ[key] = value  # Establece cada variable de entorno

    def get(self, key, default=None):
        """
        Obtiene el valor de una variable de entorno de forma segura.
        """
        return os.getenv(key, default)
    
    def set(self, key, value):
        """
        Establece el valor de una variable de entorno y la guarda en el archivo .env.
        """
        set_key(".env", key, value)  # Establece la variable en el archivo .env
        os.environ[key] = value  # También establece la variable en el entorno actual

