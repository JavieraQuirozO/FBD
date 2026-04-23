# -*- coding: utf-8 -*-
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
        _rate_limiter.check()

        search_result = cls.search_file(dataset)
        if search_result.get("status") not in ["exact", "ok"]:
            return {
                "status":  "error",
                "message": f"Cannot download dataset: status '{search_result.get('status')}'.",
            }

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
