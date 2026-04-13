# -*- coding: utf-8 -*-
import requests
from pathlib import Path
from FBD.core.config import Config
from FBD.core.rate_limiter import RateLimiter
from FBD.client.data_manager import DataManager
from FBD.client.parse import Parse

_rate_limiter = RateLimiter()


class Downloader:
    """
    Búsqueda y descarga de archivos. Consulta la edge function para obtener
    metadatos (link, filename, header) y descarga el archivo directamente
    desde la URL de FlyBase. El parseo se delega en Parse.
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
                "status":   "ok",
                "dataset":  data["dataset"],
                "link":     data["link"],
                "filename": data["filename"],
                "header":   data["header"],
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
        2. Obtiene link, filename y header desde la edge function
        3. Descarga el archivo directamente desde FlyBase
        4. Descomprime si es .gz
        5. Parsea según extensión (.tsv, .json, .obo, .txt)

        Usa caché local: si el archivo ya existe en DOWNLOAD_DIR, omite la descarga.
        Solo procede con match exacto (status "ok").
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

        # Caché: si el archivo descomprimido ya existe, no descarga de nuevo
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

        extension = decompress_path.suffix.lstrip(".")

        if extension == "tsv":
            if "affy" in filename:
                data = Parse.affy_to_df(str(decompress_path))["data"]
            else:
                # El header viene en el resultado de búsqueda; solo consulta la
                # edge function como fallback si no está presente.
                header = search_result.get("header") or DataManager.get_header_line(dataset)
                data   = Parse.tsv_to_df(str(decompress_path), header)["data"]

        elif extension == "json":
            data = Parse.json_to_df(str(decompress_path))

        elif extension == "obo":
            data = Parse.obo_to_graph(str(decompress_path))

        elif extension == "txt":
            data = Parse.txt_to_df(str(decompress_path))

        else:
            return {
                "status":  "error",
                "file":    dataset,
                "message": f"Extensión no soportada: .{extension}",
            }

        if data is not None:
            return {"status": "ok", "file": dataset, "data": data}
        else:
            return {"status": "error", "file": dataset}
