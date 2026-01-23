# -*- coding: utf-8 -*-

from supabase import create_client, Client
from .config import Config
import requests, json

class SupabaseConnection:
    """
    Manages the Supabase client instance and user session state.
    Provides methods for initializing the client
    and retrieving database tables through the PostgREST API.
    """

    _client: Client | None = None
    _session: dict | None = None   

    @staticmethod
    def init():
        """
        Initializes the Supabase client using environment variables loaded through Config.
        Must be called before attempting to connect or authenticate.
        """
        url = Config.SUPABASE_URL
        key = Config.SUPABASE_KEY
        
        SupabaseConnection._client = create_client(url, key)
        

    # ------------------------------------------------------------------
    # Connect with Supabase
    # ------------------------------------------------------------------

    @staticmethod
    def connect() -> Client:
        """
        Returns the Supabase client instance.
        
        If a user session exists, applies the user's access token so that
        all requests are authenticated. If not, the client remains anonymous.
        
        Raises:
            RuntimeError: If init() has not been called.
        """
        if SupabaseConnection._client is None:
            raise RuntimeError("init() must be called before connect().")

        client = SupabaseConnection._client
        if SupabaseConnection._session:
            client.postgrest.auth(SupabaseConnection._session["access_token"])

        return client
    

    @staticmethod
    def is_logged_in() -> bool:
        """
        Returns True if a user session is active, otherwise False.
        """
        return SupabaseConnection._session is not None

    @staticmethod
    def logout():
        """
        Clears the stored user session.
        
        The Supabase client remains initialized but becomes anonymous again.
        """
        if SupabaseConnection._client:
            SupabaseConnection._client.auth.sign_out()

        SupabaseConnection._session = None

    @staticmethod
    def login(email: str, password: str):
        """
        Attempts to authenticate a user using the Supabase Auth REST API.
        
        Args:
            email (str): User email for login.
            password (str): User password.
        
        Returns:
            dict: A dictionary containing the status and additional information.
                  On success, includes user info and access token.
                  On failure, includes an error message.
        """
        url = f"{Config.SUPABASE_URL}/auth/v1/token?grant_type=password"

        payload = {
            "email": email,
            "password": password
        }

        headers = {
            "apikey": Config.SUPABASE_KEY,
            "Content-Type": "application/json"
        }

        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            data = response.json()

            if response.status_code != 200:
                return {
                    "status": "error",
                    "message": f"Login failed: {data.get('msg', data)}"
                }

            # Save session tokens for authenticated requests. Only to edit the db
            SupabaseConnection._session = {
                "access_token": data.get("access_token"),
                "refresh_token": data.get("refresh_token"),
            }

            return {
                "status": "ok",
                "user": data.get("user"),
                "access_token": data.get("access_token")
            }

        except Exception as e:
            return {"status": "error", "message": f"Error during login: {e}"}

    @staticmethod    
    def fetch_table(table_name):
        """
        Fetches all records from the specified Supabase table.
        
        Args:
            table_name (str): The name of the table to retrieve.
            
        Returns:
            list: A list of records retrieved from the table.
        """
        client = SupabaseConnection.connect()
        return client.table(table_name).select("*").execute().data
    
