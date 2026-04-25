# -*- coding: utf-8 -*-
import json
import requests
from pathlib import Path
from FBD.core.config import Config
from FBD.core.rate_limiter import RateLimiter
from FBD.client.parse import Parse
from FBD.client.parser_dispatcher import ParserDispatcher

_rate_limiter = RateLimiter()


class Downloader:
    """
    File search and download helper. It queries the edge function to obtain
    dataset metadata (link, filename, header, parser metadata) and downloads
    the file from the registered URL. Parsing is delegated to the parse layer.
    """

    @classmethod
    def search_file(cls, dataset: str) -> dict:
        """
        Search for a dataset in the edge function (GET /search?q={dataset}).

        Returns:
            dict with status:
                "ok"        -> exact match, includes metadata needed to download
                "partial"   -> one partial match
                "multiple"  -> multiple partial matches
                "not_found" -> no matches
        """
        response = requests.get(
            f"{Config.EDGE_FUNCTION_URL}/search",
            params={"q": dataset},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        status = data.get("status")

        if status == "ok":
            return {
                "status":      "ok",
                "dataset":     data["dataset"],
                "link":        data["link"],
                "filename":    data["filename"],
                "header":      data["header"],
                "parser_type": data.get("parser_type"),
                "parse_config": data.get("parse_config"),
            }

        if status == "not_found":
            return {
                "status":  "not_found",
                "message": data.get("message", f"Dataset '{dataset}' was not found."),
            }

        # Flatten grouped matches into a single list for the public client API.
        return {
            "status":  status,
            "message": data.get("message", ""),
            "match":   [ds for datasets in data.get("matches", {}).values() for ds in datasets],
        }

    @classmethod
    def _metadata_cache_dir(cls) -> Path:
        cache_dir = Path(Config.CACHE_DIR) / "metadata"
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            return None
        return cache_dir

    @classmethod
    def _metadata_cache_path(cls, dataset: str) -> Path | None:
        cache_dir = cls._metadata_cache_dir()
        if cache_dir is None:
            return None
        safe_name = dataset.replace("/", "_")
        return cache_dir / f"{safe_name}.metadata.json"

    @classmethod
    def _save_metadata_cache(cls, dataset: str, metadata: dict) -> None:
        cache_path = cls._metadata_cache_path(dataset)
        if cache_path is None:
            return
        cache_payload = {
            "status": metadata.get("status"),
            "dataset": metadata.get("dataset"),
            "filename": metadata.get("filename"),
            "header": metadata.get("header"),
            "parser_type": metadata.get("parser_type"),
            "parse_config": metadata.get("parse_config"),
        }
        try:
            with open(cache_path, "w", encoding="utf-8") as fh:
                json.dump(cache_payload, fh, indent=2, ensure_ascii=False)
        except OSError:
            return

    @classmethod
    def _load_metadata_cache(cls, dataset: str) -> dict | None:
        cache_path = cls._metadata_cache_path(dataset)
        if cache_path is None or not cache_path.exists():
            return None

        try:
            with open(cache_path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (OSError, json.JSONDecodeError):
            return None

    @classmethod
    def _local_asset_path(cls, metadata: dict) -> Path | None:
        filename = metadata.get("filename")
        if not filename:
            return None

        local_name = filename[:-3] if filename.endswith(".gz") else filename
        return Path(Config.DOWNLOAD_DIR) / local_name

    @classmethod
    def _cached_asset_result(cls, dataset: str, metadata: dict) -> dict:
        local_path = cls._local_asset_path(metadata)
        if local_path is None or not local_path.exists():
            return {
                "status": "error",
                "message": (
                    f"Dataset '{dataset}' is unavailable online and no compatible "
                    "local cached file was found."
                ),
            }

        return {
            "status": "ok",
            "file": dataset,
            "local_path": local_path,
            "metadata": metadata,
        }

    @classmethod
    def download_file(cls, dataset: str) -> dict:
        """
        Download and parse a dataset file.

        Flow:
        1. Check the local rate limit
        2. Fetch metadata from the edge function
        3. Download the file from the registered URL
        4. Decompress it if it is a .gz file
        5. Delegate parsing to the parse dispatcher

        Uses a local cache: if the decompressed file already exists in
        DOWNLOAD_DIR, the download step is skipped.
        Only proceeds for exact matches (status "ok").
        """
        asset = cls.download_asset(dataset)
        if asset.get("status") != "ok":
            return asset

        try:
            data = ParserDispatcher.parse(
                dataset=dataset,
                local_path=asset["local_path"],
                metadata=asset["metadata"],
            )
        except (KeyError, ValueError) as exc:
            return {
                "status": "error",
                "file": dataset,
                "message": str(exc),
            }

        if data is not None:
            return {"status": "ok", "file": dataset, "data": data}
        else:
            return {"status": "error", "file": dataset}

    @classmethod
    def download_asset(cls, dataset: str) -> dict:
        """
        Transport-layer helper: resolve metadata, download, decompress, and
        return the local file path together with its metadata.
        """
        cached_metadata = cls._load_metadata_cache(dataset)

        _rate_limiter.check()

        try:
            search_result = cls.search_file(dataset)
        except requests.RequestException:
            if cached_metadata is not None:
                return cls._cached_asset_result(dataset, cached_metadata)
            return {
                "status": "error",
                "message": (
                    f"Cannot download dataset '{dataset}': metadata lookup failed "
                    "and no local metadata cache is available."
                ),
            }

        if search_result.get("status") not in ["exact", "ok"]:
            return {
                "status":  "error",
                "message": f"Cannot download dataset: status '{search_result.get('status')}'.",
            }

        cls._save_metadata_cache(dataset, search_result)

        download_dir = Path(Config.DOWNLOAD_DIR)
        download_dir.mkdir(parents=True, exist_ok=True)

        filename = search_result["filename"]
        file_url = search_result["link"]

        _filename       = filename[:-3] if filename.endswith(".gz") else filename
        destination     = download_dir / filename
        decompress_path = download_dir / _filename

        if not decompress_path.exists():
            response = requests.get(file_url, stream=True, timeout=60)
            response.raise_for_status()

            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            if filename.endswith(".gz"):
                decompress_path = Parse.decompress_gz(destination)
            else:
                decompress_path = destination

        return {
            "status": "ok",
            "file": dataset,
            "local_path": decompress_path,
            "metadata": search_result,
        }
