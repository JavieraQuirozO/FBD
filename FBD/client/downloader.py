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
    Búsqueda y descarga de archivos. Consulta la edge function para obtener
    metadatos (link, filename, header) y descarga el archivo directamente
    desde la URL de almacenada. El parseo se delega en Parse.
    """

    @classmethod
    def search_file(cls, dataset: str) -> dict:
        """
        Busca un dataset en la edge function (GET /search?q={dataset}).

        Returns:
            dict con status:
                "ok"       → match exacto, incluye link, filename y header
                "partial"  → un resultado parcial
                "multiple" → varios resultados parciales
                "not_found"→ sin resultados
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
                "message": data.get("message", f"No se encontró '{dataset}'."),
            }

        # partial / multiple: aplana matches a lista
        return {
            "status":  status,
            "message": data.get("message", ""),
            "match":   [ds for datasets in data.get("matches", {}).values() for ds in datasets],
        }

    @classmethod
    def download_file(cls, dataset: str) -> dict:
        """
        Descarga y parsea el archivo del dataset.

        Flujo:
        1. Verifica rate limit local
        2. Obtiene metadata desde la edge function
        3. Descarga el archivo directamente desde la URL registrada
        4. Descomprime si es .gz
        5. Delega el parseo al dispatcher de la capa de parseo

        Usa caché local: si el archivo ya existe en DOWNLOAD_DIR, omite la descarga.
        Solo procede con match exacto (status "ok").
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
        Capa de transporte: resuelve metadata, descarga, descomprime y retorna
        la ubicación local del archivo junto con su metadata.
        """
        _rate_limiter.check()

        search_result = cls.search_file(dataset)
        if search_result.get("status") not in ["exact", "ok"]:
            return {
                "status":  "error",
                "message": f"No se puede descargar: status '{search_result.get('status')}'.",
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
