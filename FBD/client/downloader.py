# -*- coding: utf-8 -*-
from FBD.core.supabase_client import SupabaseConnection
from FBD.client.data_manager import DataManager
from FBD.core.config import Config
from FBD.client.parse import Parse
import requests
from pathlib import Path
from FBD.core.rate_limiter import RateLimiter

_rate_limiter = RateLimiter()


class Downloader:
    """
    Provides high-level utilities for searching, downloading, and parsing files
    stored in Supabase. Integrates with DataManager for metadata lookup and
    with Parse for file-type-specific parsing.
    """
    
    SupabaseConnection.init()

    @classmethod
    def search_file(cls, dataset: str):
        """
        Search for a file by dataset. Returns metadata only if an exact match exists.
        
        Args:
            dataset (str): The exact dataset to search for.
        
        Returns:
            dict with:
                - status: "ok", "multiple", or "not_found"
                - dataset, link, filename (when exact match exists)
        """
        client = SupabaseConnection.connect()

        search_result = DataManager.search_files(dataset)

        matched_datasets = []
        for datasets in search_result.values():
            matched_datasets.extend(datasets)

        # Case: no results
        if not matched_datasets:
            return {
                "status": "not_found",
                "message": (
                    f"No file found matching dataset '{dataset}'. "
                    "Use DataManager.get_files_by_category() to explore available files."
                ),
            }

        # Case: exact match
        if dataset in matched_datasets:
            response = (
                client.table("links")
                .select("dataset, link", "filename")
                .eq("dataset", dataset)
                .limit(1)
                .execute()
            )

            row = response.data[0] if response.data else None

            if not row or not row.get("link"):
                return {
                    "status": "error",
                    "message": "The dataset exists but does not contain a valid download link."
                }

            return {
                "status": "ok",
                "dataset": dataset,
                "link": row["link"],
                "filename": row["filename"]
            }
        
        #Case: single partial match
        if len(matched_datasets) == 1:
            return {
                "status": "partial",
                "message": f"Found one partial match for '{dataset}'.",
                "match": matched_datasets[0]
            }


        # Case: multiple partial matches
        if len(matched_datasets) > 1:
            return {
                "status": "multiple",
                "message": f"Found {len(matched_datasets)} files matching '{dataset}'.",
                "match": matched_datasets,
            }

    @classmethod
    def download_file(cls, dataset: str):
        """
        Download and process a file associated with a dataset.
        
        Args:
            dataset (str): The exact dataset to search for.

            Only proceeds when:
                - The dataset has an exact match (status: "ok" or "exact").

        Supported formats:
            - TSV
            - JSON
            - OBO
            - TXT

        Returns:
            dict with:
                - status: "ok" or "error"
                - file: dataset
                - data: parsed content (if successful)
        """
        _rate_limiter.check()
        search_result = cls.search_file(dataset)
        status = search_result.get("status")

        # Only allow download for exact matches
        if status not in ["exact", "ok"]:
            return {
                "status": "error",
                "message": f"Cannot download because the search status is '{status}'."
            }

        download_dir = Path(Config.DOWNLOAD_DIR)
        download_dir.mkdir(parents=True, exist_ok=True)

        filename = search_result["filename"]
        file_url = search_result["link"]

        # Handle .gz files correctly
        _filename = filename[:-3] if filename.endswith(".gz") else filename
        destination = download_dir / filename
        decompress_path = download_dir / _filename

        # Download only if the decompressed file does not exist
        if not decompress_path.exists():
            response = requests.get(file_url, stream=True)
            response.raise_for_status()
    
            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
    
            if filename.endswith(".gz"):
                decompress_path = Parse.decompress_gz(destination)
            else:
                decompress_path = destination
                    

        extension = decompress_path.suffix.lstrip(".")

        if extension == "tsv":
            if "affy" in filename:
                data = Parse.affy_to_df(str(decompress_path))["data"]
            else:
                header = DataManager.get_header_line(dataset)
                data = Parse.tsv_to_df(str(decompress_path), header)["data"]

        elif extension == "json":
            data = Parse.json_to_df(str(decompress_path))

        elif extension == "obo":
            data = Parse.obo_to_graph(str(decompress_path))

        elif extension == "txt":
            data = Parse.txt_to_df(str(decompress_path))


        else:
            return {
                "status": "error",
                "file": dataset,
                "message": f"Unsupported file extension: .{extension}"
            }

        if data is not None:
            return {
                "status": "ok",
                "file": dataset,
                "data": data
            }
        else:
            return {
                "status": "error",
                "file": dataset
            }
