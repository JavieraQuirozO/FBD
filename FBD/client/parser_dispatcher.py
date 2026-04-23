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

        if parser_type == "tsv":
            header = metadata.get("header")
            if header is None:
                header = DataManager.get_header_line(dataset)
            return Parse.tsv_to_df(str(local_path), header)["data"]

        if parser_type == "affy":
            return Parse.affy_to_df(str(local_path))["data"]

        if parser_type == "json":
            return Parse.json_to_df(str(local_path))

        if parser_type == "obo":
            return Parse.obo_to_graph(str(local_path))

        if parser_type == "txt":
            return Parse.txt_to_df(str(local_path))

        if parser_type == "fb":
            return Parse.fb_to_df(
                str(local_path),
                start_line=parse_config["start_line"],
                columns=parse_config["columns"],
            )

        raise ValueError(f"Extensión o parser no soportado para '{dataset}': {parser_type}")
