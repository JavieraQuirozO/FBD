from unittest.mock import patch, MagicMock
from pathlib import Path
import pytest

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


def test_search_file_not_found():
    with patch("FBD.client.downloader.DataManager.search_files") as mock_search, \
         patch("FBD.client.downloader.SupabaseConnection.connect"):

        mock_search.return_value = {}

        result = Downloader.search_file("unknown_dataset")

        assert result["status"] == "not_found"
        assert "No file found" in result["message"]


def test_search_file_exact_match():
    mock_client = MagicMock()
    mock_client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = [
        {"dataset": "valid_dataset", "link": "http://example.com/file.tsv", "filename": "file.tsv"}
    ]

    with patch("FBD.client.downloader.DataManager.search_files") as mock_search, \
         patch("FBD.client.downloader.SupabaseConnection.connect", return_value=mock_client):

        mock_search.return_value = {
            "category": ["valid_dataset"]
        }

        result = Downloader.search_file("valid_dataset")

        assert result["status"] == "ok"
        assert result["dataset"] == "valid_dataset"
        assert result["link"].startswith("http")
        assert result["filename"] == "file.tsv"


def test_search_file_single_partial_match():
    with patch("FBD.client.downloader.DataManager.search_files") as mock_search, \
         patch("FBD.client.downloader.SupabaseConnection.connect"):

        mock_search.return_value = {
            "category": ["partial_dataset"]
        }

        result = Downloader.search_file("part")

        assert result["status"] == "partial"
        assert result["match"] == "partial_dataset"


def test_search_file_multiple_partial_matches():
    with patch("FBD.client.downloader.DataManager.search_files") as mock_search, \
         patch("FBD.client.downloader.SupabaseConnection.connect"):

        mock_search.return_value = {
            "cat1": ["dataset_a"],
            "cat2": ["dataset_b"]
        }

        result = Downloader.search_file("data")

        assert result["status"] == "multiple"
        assert isinstance(result["match"], list)
        assert len(result["match"]) == 2


def test_download_file_rejects_non_exact_match():
    with patch("FBD.client.downloader.Downloader.search_file") as mock_search:
        mock_search.return_value = {"status": "multiple"}

        result = Downloader.download_file("ambiguous_dataset")

        assert result["status"] == "error"
        assert "Cannot download" in result["message"]


def test_download_file_success_tsv(tmp_path):
    fake_search_result = {
        "status": "ok",
        "dataset": "valid_dataset",
        "link": "http://example.com/file.tsv",
        "filename": "file.tsv"
    }

    fake_df = MagicMock()

    with patch("FBD.client.downloader.Downloader.search_file", return_value=fake_search_result), \
         patch("FBD.client.downloader.Config.DOWNLOAD_DIR", tmp_path), \
         patch("FBD.client.downloader.requests.get") as mock_get, \
         patch("FBD.client.downloader.Parse.tsv_to_df", return_value={"data": fake_df}), \
         patch("FBD.client.downloader.DataManager.get_header_line", return_value=0):

        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"col1\tcol2\n1\t2"]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = Downloader.download_file("valid_dataset")

        assert result["status"] == "ok"
        assert result["file"] == "valid_dataset"
        assert result["data"] is fake_df


def test_download_file_unsupported_extension():
    fake_search_result = {
        "status": "ok",
        "dataset": "weird_dataset",
        "link": "http://example.com/file.xyz",
        "filename": "file.xyz"
    }

    with patch("FBD.client.downloader.Downloader.search_file", return_value=fake_search_result), \
         patch("FBD.client.downloader.Config.DOWNLOAD_DIR", Path("/tmp")), \
         patch("FBD.client.downloader.Path.exists", return_value=True):

        result = Downloader.download_file("weird_dataset")

        assert result["status"] == "error"
        assert "Unsupported file extension" in result["message"]
        
def test_rate_limiter_blocks(isolated_cache):

    Config.DOWNLOAD_RATE_LIMIT_ENABLED = True
    Config.DOWNLOAD_MAX_CALLS = 1
    Config.DOWNLOAD_WINDOW_SECONDS = 60

    limiter = RateLimiter("download")

    limiter.check()

    with pytest.raises(RuntimeError):
        limiter.check()


