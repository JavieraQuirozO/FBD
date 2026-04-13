# -*- coding: utf-8 -*-
import json
from pathlib import Path
from platformdirs import user_cache_dir, user_config_dir


class Config:
    """
    Configuración global de la aplicación.
    Gestiona directorios, URL de la edge function y parámetros de rate limiting.
    Todos los atributos y métodos son de clase; no se instancia.
    """

    APP_NAME = "flybasedownloads"

    DEFAULT_CACHE_DIR    = Path(user_cache_dir(APP_NAME))
    DEFAULT_DOWNLOAD_DIR = DEFAULT_CACHE_DIR / "downloads"
    CONFIG_FILE          = Path(user_config_dir(APP_NAME)) / "config.json"

    # URL pública de la edge function. No es un secreto.
    EDGE_FUNCTION_URL = "https://ipoleoimulkvsyhelkgx.supabase.co/functions/v1/fbd"

    DOWNLOAD_DIR = DEFAULT_DOWNLOAD_DIR
    CACHE_DIR    = DEFAULT_CACHE_DIR
    LOG_LEVEL    = "INFO"

    DOWNLOAD_RATE_LIMIT_ENABLED = True
    DOWNLOAD_MAX_CALLS          = 15
    DOWNLOAD_WINDOW_SECONDS     = 3600

    @classmethod
    def load_user_config(cls):
        """
        Carga config.json del usuario. Si no existe o está corrupto,
        lo regenera con los valores por defecto.
        """
        cfg_path = cls.CONFIG_FILE
        cfg_path.parent.mkdir(parents=True, exist_ok=True)

        if not cfg_path.exists():
            cls.save_user_config()
            return

        try:
            with open(cfg_path, "r") as f:
                cfg = json.load(f)

            cls.DOWNLOAD_DIR                = Path(cfg.get("download_dir", cls.DEFAULT_DOWNLOAD_DIR))
            cls.CACHE_DIR                   = Path(cfg.get("cache_dir", cls.DEFAULT_CACHE_DIR))
            cls.LOG_LEVEL                   = cfg.get("log_level", "INFO")
            cls.DOWNLOAD_RATE_LIMIT_ENABLED = cfg.get("download_rate_limit_enabled", True)
            cls.DOWNLOAD_MAX_CALLS          = cfg.get("download_max_calls", 15)
            cls.DOWNLOAD_WINDOW_SECONDS     = cfg.get("download_window_seconds", 3600)
            cls.EDGE_FUNCTION_URL           = cfg.get("edge_function_url", cls.EDGE_FUNCTION_URL)

        except Exception:
            cls.save_user_config()

    @classmethod
    def save_user_config(cls):
        """
        Persiste la configuración actual en config.json.
        No guarda secretos: EDGE_FUNCTION_URL es una URL pública.
        """
        cfg_data = {
            "download_dir":                str(cls.DOWNLOAD_DIR),
            "cache_dir":                   str(cls.CACHE_DIR),
            "log_level":                   cls.LOG_LEVEL,
            "download_rate_limit_enabled": cls.DOWNLOAD_RATE_LIMIT_ENABLED,
            "download_max_calls":          cls.DOWNLOAD_MAX_CALLS,
            "download_window_seconds":     cls.DOWNLOAD_WINDOW_SECONDS,
            "edge_function_url":           cls.EDGE_FUNCTION_URL,
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
        persist: bool = True,
    ):
        """
        Modifica los parámetros de rate limiting en tiempo de ejecución.
        Con persist=True (default) los cambios sobreviven entre sesiones.
        """
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
        """Crea los directorios de caché y descarga si no existen."""
        cls.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        cls.CACHE_DIR.mkdir(parents=True, exist_ok=True)
