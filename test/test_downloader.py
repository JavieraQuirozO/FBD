import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from FBD.client.downloader import Downloader
from FBD.core.config import Config
from FBD.core.rate_limiter import RateLimiter


@pytest.fixture(autouse=True)
def disable_rate_limiter():
    Config.DOWNLOAD_RATE_LIMIT_ENABLED = False
    yield
    Config.DOWNLOAD_RATE_LIMIT_ENABLED = True


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(Config, "CACHE_DIR", tmp_path)
    yield


def make_mock_response(data: dict):
    mock = MagicMock()
    mock.json.return_value = data
    mock.raise_for_status.return_value = None
    return mock


def test_search_file_not_found():
    with patch("FBD.client.downloader.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({
            "status": "not_found",
            "message": "No dataset found matching 'unknown'."
        })
        result = Downloader.search_file("unknown_dataset")
        assert result["status"] == "not_found"
        assert "message" in result


def test_search_file_exact_match():
    with patch("FBD.client.downloader.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({
            "status": "ok",
            "dataset": "valid_dataset",
            "link": "http://example.com/file.tsv",
            "filename": "file.tsv",
            "header": 0,
            "parser_type": "tsv",
            "parse_config": {},
        })
        result = Downloader.search_file("valid_dataset")
        assert result["status"] == "ok"
        assert result["dataset"] == "valid_dataset"
        assert result["link"].startswith("http")
        assert result["filename"] == "file.tsv"
        assert result["parser_type"] == "tsv"


def test_search_file_single_partial_match():
    with patch("FBD.client.downloader.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({
            "status": "partial",
            "message": "Found 1",
            "matches": {"category": ["partial_dataset"]}
        })
        result = Downloader.search_file("part")
        assert result["status"] == "partial"
        assert "partial_dataset" in result["match"]


def test_search_file_multiple_partial_matches():
    with patch("FBD.client.downloader.requests.get") as mock_get:
        mock_get.return_value = make_mock_response({
            "status": "multiple",
            "message": "Found 2",
            "matches": {"cat1": ["dataset_a"], "cat2": ["dataset_b"]}
        })
        result = Downloader.search_file("data")
        assert result["status"] == "multiple"
        assert isinstance(result["match"], list)
        assert len(result["match"]) == 2


def test_download_file_rejects_non_exact_match():
    with patch("FBD.client.downloader.Downloader.search_file") as mock_search:
        mock_search.return_value = {"status": "multiple"}
        result = Downloader.download_file("ambiguous_dataset")
        assert result["status"] == "error"
        assert "No se puede descargar" in result["message"]


def test_download_file_success_tsv(tmp_path):
    fake_search_result = {
        "status": "ok",
        "dataset": "valid_dataset",
        "link": "http://example.com/file.tsv",
        "filename": "file.tsv",
        "header": 0,
        "parser_type": "tsv",
        "parse_config": {},
    }
    fake_df = MagicMock()

    mock_file_response = MagicMock()
    mock_file_response.iter_content.return_value = [b"col1\tcol2\n1\t2"]
    mock_file_response.raise_for_status.return_value = None

    with patch("FBD.client.downloader.Downloader.search_file", return_value=fake_search_result), \
         patch("FBD.client.downloader.Config.DOWNLOAD_DIR", tmp_path), \
         patch("FBD.client.downloader.requests.get", return_value=mock_file_response), \
         patch("FBD.client.parser_dispatcher.Parse.tsv_to_df", return_value={"data": fake_df}):

        result = Downloader.download_file("valid_dataset")

        assert result["status"] == "ok"
        assert result["file"] == "valid_dataset"
        assert result["data"] is fake_df


def test_download_file_success_fb(tmp_path):
    fake_search_result = {
        "status": "ok",
        "dataset": "gene_association",
        "link": "http://example.com/gene_association.fb.gz",
        "filename": "gene_association.fb.gz",
        "header": None,
        "parser_type": "fb",
        "parse_config": {
            "start_line": 5,
            "columns": ["DB", "DB Object ID"],
        },
    }
    fake_df = MagicMock()

    mock_file_response = MagicMock()
    mock_file_response.iter_content.return_value = [b"fake gz payload"]
    mock_file_response.raise_for_status.return_value = None

    with patch("FBD.client.downloader.Downloader.search_file", return_value=fake_search_result), \
         patch("FBD.client.downloader.Config.DOWNLOAD_DIR", tmp_path), \
         patch("FBD.client.downloader.requests.get", return_value=mock_file_response), \
         patch("FBD.client.downloader.Parse.decompress_gz", return_value=tmp_path / "gene_association.fb"), \
         patch("FBD.client.parser_dispatcher.Parse.fb_to_df", return_value=fake_df) as mock_fb:

        result = Downloader.download_file("gene_association")

        assert result["status"] == "ok"
        assert result["file"] == "gene_association"
        assert result["data"] is fake_df
        mock_fb.assert_called_once()


def test_download_file_unsupported_extension():
    fake_search_result = {
        "status": "ok",
        "dataset": "weird_dataset",
        "link": "http://example.com/file.xyz",
        "filename": "file.xyz",
        "header": None,
        "parser_type": "xyz",
        "parse_config": {},
    }

    with patch("FBD.client.downloader.Downloader.search_file", return_value=fake_search_result), \
         patch("FBD.client.downloader.Config.DOWNLOAD_DIR", Path("/tmp")), \
         patch("FBD.client.downloader.Path.exists", return_value=True):

        result = Downloader.download_file("weird_dataset")

        assert result["status"] == "error"
        assert "parser no soportado" in result["message"]


def test_download_asset_success(tmp_path):
    fake_search_result = {
        "status": "ok",
        "dataset": "valid_dataset",
        "link": "http://example.com/file.tsv",
        "filename": "file.tsv",
        "header": 0,
        "parser_type": "tsv",
        "parse_config": {},
    }

    mock_file_response = MagicMock()
    mock_file_response.iter_content.return_value = [b"col1\tcol2\n1\t2"]
    mock_file_response.raise_for_status.return_value = None

    with patch("FBD.client.downloader.Downloader.search_file", return_value=fake_search_result), \
         patch("FBD.client.downloader.Config.DOWNLOAD_DIR", tmp_path), \
         patch("FBD.client.downloader.requests.get", return_value=mock_file_response):

        result = Downloader.download_asset("valid_dataset")

        assert result["status"] == "ok"
        assert result["file"] == "valid_dataset"
        assert result["metadata"]["parser_type"] == "tsv"
        assert Path(result["local_path"]).name == "file.tsv"


def test_rate_limiter_blocks(isolated_cache):
    Config.DOWNLOAD_RATE_LIMIT_ENABLED = True
    Config.DOWNLOAD_MAX_CALLS = 1
    Config.DOWNLOAD_WINDOW_SECONDS = 60

    limiter = RateLimiter("download")
    limiter.check()

    with pytest.raises(RuntimeError):
        limiter.check()
