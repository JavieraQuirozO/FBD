import requests
from .config_manager import ConfigManager

class RTD:
    def __init__(self):
        """
        Inicializa la clase con el ID del usuario y la URL base del backend.
        """
        config_manager = ConfigManager()  # Crea la instancia de ConfigManager
        self.user_id = config_manager.get("USER_ID")  # Obtiene el valor de USER_ID
        self.base_url = config_manager.get("BASE_URL")  # Obtiene el valor de BASE_URL

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
