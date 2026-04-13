import pytest
import requests as req
from unittest.mock import patch, MagicMock
from FBD.client.data_manager import DataManager


def make_mock_response(data: dict, status_code: int = 200):
    mock = MagicMock()
    mock.json.return_value = data
    mock.raise_for_status.return_value = None
    mock.status_code = status_code
    return mock


def make_error_response(status_code: int):
    error_response = MagicMock()
    error_response.status_code = status_code
    mock = MagicMock()
    mock.raise_for_status.side_effect = req.HTTPError(response=error_response)
    return mock


# ── Categorías ─────────────────────────────────────────────────────────────────

def test_get_categories():
    with patch("FBD.client.data_manager.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({"categories": ["Interactions", "Genes"]})
        result = DataManager.get_categories()
        assert result == ["Interactions", "Genes"]


def test_get_files_by_category_specific():
    with patch("FBD.client.data_manager.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({"datasets": ["ds1", "ds2"]})
        result = DataManager.get_files_by_category("CatA")
        assert result == ["ds1", "ds2"]


def test_get_files_by_category_all():
    # 1 llamada para get_categories + 1 por cada categoría
    responses = [
        make_mock_response({"categories": ["CatA", "CatB"]}),
        make_mock_response({"datasets": ["ds1", "ds2"]}),
        make_mock_response({"datasets": ["ds3"]}),
    ]
    with patch("FBD.client.data_manager.requests.get", side_effect=responses):
        result = DataManager.get_files_by_category()
        assert result == {"CatA": ["ds1", "ds2"], "CatB": ["ds3"]}


def test_get_files_by_category_not_found():
    with patch("FBD.client.data_manager.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({"datasets": []})
        result = DataManager.get_files_by_category("Unknown")
        assert result == []


# ── Búsqueda ───────────────────────────────────────────────────────────────────

def test_search_files_exact():
    with patch("FBD.client.data_manager.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({
            "status": "ok",
            "dataset": "gene_interactions",
            "link": "http://example.com/file.tsv",
            "filename": "file.tsv",
            "header": 0
        })
        result = DataManager.search_files("gene_interactions")
        assert "_exact" in result
        assert result["_exact"]["dataset"] == "gene_interactions"


def test_search_files_partial():
    with patch("FBD.client.data_manager.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({
            "status": "multiple",
            "matches": {"Genes": ["gene_interactions", "gene_snapshots"]}
        })
        result = DataManager.search_files("gene")
        assert "Genes" in result


def test_search_files_not_found():
    with patch("FBD.client.data_manager.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({"status": "not_found"})
        result = DataManager.search_files("xyz")
        assert result == {}


# ── Metadatos de dataset ────────────────────────────────────────────────────────

def test_get_description_success():
    with patch("FBD.client.data_manager.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({
            "status": "ok",
            "description": "Some description",
            "link": "http://example.com/file.tsv"
        })
        result = DataManager.get_description("valid_dataset")
        assert result == "Some description"


def test_get_description_not_found():
    with patch("FBD.client.data_manager.requests.get") as mock_get:
        mock_get.return_value = make_error_response(404)
        result = DataManager.get_description("missing")
        assert result is None


def test_get_header_line_success():
    with patch("FBD.client.data_manager.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({"status": "ok", "header": 3})
        result = DataManager.get_header_line("dataset")
        assert result == 3


def test_get_header_line_none():
    with patch("FBD.client.data_manager.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({"status": "ok", "header": None})
        result = DataManager.get_header_line("dataset")
        assert result is None


def test_get_filename_success():
    with patch("FBD.client.data_manager.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({"status": "ok", "filename": "file.tsv"})
        result = DataManager.get_filename("dataset")
        assert result == "file.tsv"


# ── Escritura ──────────────────────────────────────────────────────────────────

def test_set_header_line_success():
    with patch("FBD.client.data_manager.requests.patch") as mock_patch:
        mock_patch.return_value = make_mock_response({"status": "ok", "message": "Header updated"})
        result = DataManager.set_header_line("dataset", 5, admin_key="test_key")
        assert result["status"] == "ok"


def test_set_header_line_empty_dataset():
    with pytest.raises(ValueError):
        DataManager.set_header_line("", 3, admin_key="test_key")


# ── Descripciones de columnas ──────────────────────────────────────────────────

def test_get_column_descriptions_success():
    with patch("FBD.client.data_manager.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({
            "status": "ok",
            "data": {"col1": "desc1", "col2": "desc2"}
        })
        result = DataManager.get_column_descriptions("dataset")
        assert result["status"] == "ok"
        assert "col1" in result["data"]


def test_get_column_descriptions_not_found():
    with patch("FBD.client.data_manager.requests.get") as mock_get:
        mock_get.return_value = make_error_response(404)
        result = DataManager.get_column_descriptions("missing")
        assert result["status"] == "not_found"
