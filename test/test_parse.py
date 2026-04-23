import json
from pathlib import Path

import pandas as pd
import pytest

from FBD.client.parse import Parse


def test_parse_tsv(tmp_path):
    path = tmp_path / "sample.tsv"
    path.write_text("col1\tcol2\n1\t2\n", encoding="utf-8")

    data = Parse.parse(path, parser_type="tsv", header=0)

    assert list(data.columns) == ["col1", "col2"]
    assert data.iloc[0].to_list() == ["1", "2"]


def test_parse_txt_with_custom_separator(tmp_path):
    path = tmp_path / "sample.txt"
    path.write_text("a,b\nx,y\n", encoding="utf-8")

    data = Parse.parse(path, parser_type="txt", config={"sep": ","})

    assert list(data.columns) == ["a", "b"]
    assert data.iloc[0].to_list() == ["x", "y"]


def test_parse_json_to_dataframe(tmp_path):
    path = tmp_path / "sample.json"
    path.write_text(json.dumps({"data": [{"gene": "g1", "value": 3}]}), encoding="utf-8")

    data = Parse.parse(path, parser_type="json")

    assert isinstance(data, pd.DataFrame)
    assert data.iloc[0].to_dict() == {"gene": "g1", "value": 3}


def test_parse_fb(tmp_path):
    path = tmp_path / "gene_association.fb"
    lines = [
        "!gaf-version: 2.2",
        "!generated-by: test",
        "!date-generated: 2026-04-23",
        "!Header line 4",
        "!Header line 5",
        "FB\tFBgn0000001\tgeneA\tinvolved_in\tGO:0000001\tFBrf0000001\tIMP\t\tP\tGene A\t\tprotein\ttaxon:7227\t20260423\tFlyBase\t\t",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    data = Parse.parse(
        path,
        parser_type="fb",
        config={
            "start_line": 5,
            "columns": [
                "DB",
                "DB Object ID",
                "DB Object Symbol",
                "Qualifier",
                "GO ID",
                "DB:Reference",
                "Evidence Code",
                "With (or) From",
                "Aspect",
                "DB Object Name",
                "DB Object Synonym",
                "DB Object Type",
                "Taxon",
                "Date",
                "Assigned By",
                "Annotation Extension",
                "Gene Product Form ID",
            ],
        },
    )

    assert list(data.columns[:5]) == ["DB", "DB Object ID", "DB Object Symbol", "Qualifier", "GO ID"]
    assert data.iloc[0]["DB Object ID"] == "FBgn0000001"


def test_parse_unknown_parser_raises(tmp_path):
    path = tmp_path / "sample.unknown"
    path.write_text("data", encoding="utf-8")

    with pytest.raises(ValueError):
        Parse.parse(path, parser_type="unknown")
