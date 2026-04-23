# -*- coding: utf-8 -*-
import requests
from FBD.core.config import Config


class DataManager:
    """
    Dataset metadata access through the edge function.
    All methods are static; the class is stateless.
    Each method performs one or more HTTP requests to the edge function
    and returns processed results.
    """

    @staticmethod
    def _get(path: str, params: dict | None = None) -> dict:
        """Perform a GET request to the edge function and return the JSON payload."""
        url = f"{Config.EDGE_FUNCTION_URL}/{path}"
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def get_categories() -> list[str]:
        """Return the list of all available category names."""
        data = DataManager._get("categories")
        return data.get("categories", [])

    @staticmethod
    def get_files_by_category(category_name: str | None = None) -> dict | list:
        """
        Without a parameter, return {category: [datasets]} for all categories.
        With a parameter, return the dataset list for that category.
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
        Case-insensitive dataset search by substring.
        Returns {category: [datasets]} for partial matches,
        or {"_exact": {dataset, link, filename, header, parser_type, parse_config}}
        for an exact match.
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
        Return full metadata for a dataset.
        """
        return DataManager._get(f"datasets/{requests.utils.quote(dataset)}")

    @staticmethod
    def get_description(dataset: str) -> str | None:
        """
        Return the dataset description, or None if it does not exist.
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
        Return the stored header line number for the dataset.
        None means the header should be auto-detected.
        """
        data = DataManager.get_dataset_metadata(dataset)
        header = data.get("header")
        return int(header) if header is not None else None

    @staticmethod
    def set_header_line(dataset: str, header_line: int | None, admin_key: str) -> dict:
        """
        Update the dataset header field. Requires admin_key.
        """
        if not dataset or dataset.strip() == "":
            raise ValueError("dataset cannot be empty.")

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
        """Return the filename associated with the dataset."""
        data = DataManager.get_dataset_metadata(dataset)
        return data.get("filename")

    @staticmethod
    def get_column_descriptions(dataset: str, columns="all") -> dict:
        """
        Return dataset column descriptions.
        columns can be "all", a string, or a list of names.
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
                return {"status": "not_found", "message": f"Dataset '{dataset}' was not found."}
            raise
