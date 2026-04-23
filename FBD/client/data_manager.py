# -*- coding: utf-8 -*-
import requests
from FBD.core.config import Config


class DataManager:
    """
    Acceso a metadatos de datasets a través de la edge function.
    Todos los métodos son estáticos; la clase no tiene estado.
    Cada método realiza una o más llamadas HTTP a la edge function
    y devuelve los datos procesados.
    """

    @staticmethod
    def _get(path: str, params: dict | None = None) -> dict:
        """Realiza un GET a la edge function y retorna el JSON de respuesta."""
        url = f"{Config.EDGE_FUNCTION_URL}/{path}"
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def get_categories() -> list[str]:
        """Retorna la lista de nombres de todas las categorías disponibles."""
        data = DataManager._get("categories")
        return data.get("categories", [])

    @staticmethod
    def get_files_by_category(category_name: str | None = None) -> dict | list:
        """
        Sin parámetro: retorna {categoria: [datasets]} para todas las categorías.
        Con parámetro: retorna la lista de datasets de esa categoría.
        """
        if category_name:
            data = DataManager._get(f"categories/{requests.utils.quote(category_name)}")
            return data.get("datasets", [])

        categories = DataManager.get_categories()
        result = {}
        for cat in categories:
            datasets = DataManager.get_files_by_category(cat)
            if datasets:
                result[cat] = datasets
        return result

    @staticmethod
    def search_files(dataset: str) -> dict:
        """
        Búsqueda case-insensitive de datasets por subcadena.
        Retorna {categoria: [datasets]} para resultados parciales,
        o {"_exact": {dataset, link, filename, header, parser_type, parse_config}}
        para match exacto.
        """
        data = DataManager._get("search", params={"q": dataset})
        status = data.get("status")

        if status == "not_found":
            return {}

        if status == "ok":
            return {
                "_exact": {
                    "dataset":     data["dataset"],
                    "link":        data["link"],
                    "filename":    data["filename"],
                    "header":      data["header"],
                    "parser_type": data.get("parser_type"),
                    "parse_config": data.get("parse_config"),
                }
            }

        return data.get("matches", {})

    @staticmethod
    def get_dataset_metadata(dataset: str) -> dict:
        """
        Retorna los metadatos completos de un dataset.
        """
        return DataManager._get(f"datasets/{requests.utils.quote(dataset)}")

    @staticmethod
    def get_description(dataset: str) -> str | None:
        """
        Retorna la descripción del dataset, o None si no existe.
        """
        try:
            data = DataManager.get_dataset_metadata(dataset)
            return data.get("description")
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                print(f"Not found: '{dataset}'")
                return None
            raise

    @staticmethod
    def get_header_line(dataset: str) -> int | None:
        """
        Retorna el número de línea del header almacenado para el dataset.
        None indica que la detección debe ser automática.
        """
        data = DataManager.get_dataset_metadata(dataset)
        header = data.get("header")
        return int(header) if header is not None else None

    @staticmethod
    def set_header_line(dataset: str, header_line: int | None, admin_key: str) -> dict:
        """
        Actualiza el campo header del dataset. Requiere admin_key.
        """
        if not dataset or dataset.strip() == "":
            raise ValueError("dataset no puede estar vacío.")

        url = f"{Config.EDGE_FUNCTION_URL}/datasets/{requests.utils.quote(dataset)}/header"
        response = requests.patch(
            url,
            json={"header": header_line},
            headers={"X-Admin-Key": admin_key},
            timeout=10,
        )
        return response.json()

    @staticmethod
    def get_filename(dataset: str) -> str | None:
        """Retorna el nombre de archivo asociado al dataset."""
        data = DataManager.get_dataset_metadata(dataset)
        return data.get("filename")

    @staticmethod
    def get_column_descriptions(dataset: str, columns="all") -> dict:
        """
        Retorna las descripciones de columnas del dataset.
        columns puede ser "all", un string o una lista de nombres.
        """
        if isinstance(columns, list):
            cols_param = ",".join(columns)
        elif columns == "all" or columns is None:
            cols_param = "all"
        else:
            cols_param = columns

        try:
            return DataManager._get(
                f"datasets/{requests.utils.quote(dataset)}/columns",
                params={"cols": cols_param},
            )
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return {"status": "not_found", "message": f"Dataset '{dataset}' no encontrado."}
            raise
