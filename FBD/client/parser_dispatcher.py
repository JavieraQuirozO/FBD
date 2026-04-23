# -*- coding: utf-8 -*-
from pathlib import Path

from FBD.client.data_manager import DataManager
from FBD.client.parse import Parse


class ParserDispatcher:
    """
    Resuelve el parser apropiado a partir de la metadata del dataset y
    transforma el archivo local descargado en el objeto Python esperado.
    """

    @staticmethod
    def parse(dataset: str, local_path: str | Path, metadata: dict) -> object:
        local_path = Path(local_path)
        parser_type = metadata.get("parser_type") or local_path.suffix.lstrip(".")
        parse_config = metadata.get("parse_config") or {}
        header = metadata.get("header")

        if parser_type == "tsv":
            if header is None:
                header = DataManager.get_header_line(dataset)

        try:
            return Parse.parse(
                file_path=local_path,
                parser_type=parser_type,
                config=parse_config,
                header=header,
            )
        except ValueError as exc:
            raise ValueError(f"Extensión o parser no soportado para '{dataset}': {parser_type}") from exc
