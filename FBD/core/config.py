# -*- coding: utf-8 -*-
import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
from platformdirs import user_cache_dir, user_config_dir


class Config:
    """
    Handles global configuration for the application, including loading
    environment variables, resolving directory paths, and managing
    non-sensitive user configuration stored locally.

    The Config class is designed to be loaded once at application startup
    and provides class-level attributes for reading configuration values.
    """
    
    APP_NAME = "flybasedownloads"

    DEFAULT_CACHE_DIR = Path(user_cache_dir(APP_NAME))
    DEFAULT_DOWNLOAD_DIR = DEFAULT_CACHE_DIR / "downloads"

    CONFIG_FILE = Path(user_config_dir(APP_NAME)) / "config.json"

    SUPABASE_URL= "https://ipoleoimulkvsyhelkgx.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imlwb2xlb2ltdWxrdnN5aGVsa2d4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjE1Nzk4NTIsImV4cCI6MjA3NzE1NTg1Mn0.Ud_TPJIudNHZK54wC0VQbO-CkLl6mJr1h9I2ZTMvF0Q"

    DOWNLOAD_DIR = DEFAULT_DOWNLOAD_DIR
    CACHE_DIR = DEFAULT_CACHE_DIR

    LOG_LEVEL = "INFO"
    
    DOWNLOAD_RATE_LIMIT_ENABLED = True
    DOWNLOAD_MAX_CALLS = 15         
    DOWNLOAD_WINDOW_SECONDS = 3600  
    

    @classmethod
    def load_user_config(cls):
        """
        Loads values from the user's config.json. If the file does not exist
        or is corrupted, it is recreated with default values.
        """
        cfg_path = cls.CONFIG_FILE

        cfg_path.parent.mkdir(parents=True, exist_ok=True)

        if not cfg_path.exists():
            cls.save_user_config()
            return

        try:
            with open(cfg_path, "r") as f:
                cfg = json.load(f)

            cls.DOWNLOAD_DIR = Path(cfg.get("download_dir", cls.DEFAULT_DOWNLOAD_DIR))
            cls.CACHE_DIR = Path(cfg.get("cache_dir", cls.DEFAULT_CACHE_DIR))
            cls.LOG_LEVEL = cfg.get("log_level", "INFO")
            
            cls.DOWNLOAD_RATE_LIMIT_ENABLED = cfg.get(
                "download_rate_limit_enabled", True
            )
            cls.DOWNLOAD_MAX_CALLS = cfg.get(
                "download_max_calls", 15
            )
            cls.DOWNLOAD_WINDOW_SECONDS = cfg.get(
                "download_window_seconds", 3600
            )

        except Exception:
            cls.save_user_config()

    @classmethod
    def save_user_config(cls):
        """
        Saves the current non-sensitive configuration values to config.json.
        This includes directories and log level, but excludes environment
        variables and secrets.
        """
        cfg_data = {
            "download_dir": str(cls.DOWNLOAD_DIR),
            "cache_dir": str(cls.CACHE_DIR),
            "log_level": cls.LOG_LEVEL,

        # Rate limit
        "download_rate_limit_enabled": cls.DOWNLOAD_RATE_LIMIT_ENABLED,
        "download_max_calls": cls.DOWNLOAD_MAX_CALLS,
        "download_window_seconds": cls.DOWNLOAD_WINDOW_SECONDS,
        }

        cls.CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)

        with open(cls.CONFIG_FILE, "w") as f:
            json.dump(cfg_data, f, indent=4)
            
            
    @classmethod
    def set_rate_limit(
        cls,
        enabled: bool | None = None,
        max_calls: int | None = None,
        window_seconds: int | None = None,
        persist: bool = True
    ):
        if enabled is not None:
            cls.DOWNLOAD_RATE_LIMIT_ENABLED = enabled
        if max_calls is not None:
            cls.DOWNLOAD_MAX_CALLS = max_calls
        if window_seconds is not None:
            cls.DOWNLOAD_WINDOW_SECONDS = window_seconds

        if persist:
            cls.save_user_config()

    @classmethod
    def setup_dirs(cls):
        """
        Ensures that both cache and download directories exist.
        If they do not exist, they are created.
        """
        cls.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        cls.CACHE_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def detect_environment(cls):
        """
        Detects the environment in which the application is running.

        Returns:
            str: One of the following values:
                - "colab" for Google Colab
                - "spyder" for Spyder IDE
                - "jupyter" for Jupyter Notebook
                - "vscode" for VSCode interactive mode
                - "terminal" for a standard terminal
                - "local" if none of the above match
        """
        try:
            import google.colab  # type: ignore
            return "colab"
        except ImportError:
            pass

        try:
            get_ipython()  # type: ignore

            if "spyder_kernels" in sys.modules:
                return "spyder"

            if "ipykernel" in sys.modules:
                return "jupyter"

            if "vscode" in sys.modules:
                return "vscode"

            return "terminal"

        except Exception:
            return "local"
        
    @classmethod
    def summary(cls):
        """
        Prints a summary of the current configuration, including environment,
        Supabase URL, directories, and log level.
        """
        print("Config Information:")
        print(f"Environment   : {cls.detect_environment()}")
        print(f"Supabase URL  : {cls.SUPABASE_URL}")
        print(f"Download dir  : {cls.DOWNLOAD_DIR}")
        print(f"Cache dir     : {cls.CACHE_DIR}")
        print(f"Log level     : {cls.LOG_LEVEL}")
        
    
