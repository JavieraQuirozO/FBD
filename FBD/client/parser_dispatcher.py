# -*- coding: utf-8 -*-
from pathlib import Path

from FBD.client.data_manager import DataManager
from FBD.client.parse import Parse


class ParserDispatcher:
    """
    Resolve the appropriate parser from dataset metadata and transform
    the downloaded local file into the expected Python object.
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
            raise ValueError(f"Unsupported extension or parser for '{dataset}': {parser_type}") from exc
