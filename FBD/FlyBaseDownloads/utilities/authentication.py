import requests
from .config_manager import ConfigManager

class Authentication:
    def __init__(self):
        """
        Inicializa la clase con la URL base del backend.
        """
        config_manager = ConfigManager()  # Crear la instancia de ConfigManager
        self.base_url = config_manager.get("BASE_URL")  # Obtiene el valor de BASE_URL

        if not self.base_url:
            raise ValueError("La variable de entorno BASE_URL no está configurada.")
        

    def get_user(self, username, password, email=None):
        """
        Crea un usuario nuevo o recupera uno existente mediante el backend.
        """
        endpoint = f"{self.base_url}/users/"
        data = {"email": email, "username": username, "password": password}

        response = requests.post(endpoint, json=data)

        if response.status_code == 200:
            user_id = response.json().get("id")
            print("Usuario creado exitosamente")
            self._guardar_user_id(user_id)
            
        elif response.status_code == 400:  # Usuario ya existe
            print("El usuario ya existe. Intentando autenticar...")
            return self.verify_user(username, password)
        else:
            print(f"Error al registrar o autenticar el usuario: {response.status_code} - {response.text}")
            self._guardar_user_id("00")
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
            self._guardar_user_id(user_id)
        else:
            print(f"Error de autenticación: {response.status_code} - {response.text}")
            self._guardar_user_id("00")
            return None

    def _guardar_user_id(self, user_id):
        """
        Guarda el user_id en el archivo .env.
        """
        config_manager = ConfigManager()  # Instancia de ConfigManager
        config_manager.set("USER_ID", str(user_id))
