import pytest
from unittest.mock import patch
from FBD.fbd import FBD


def test_init_without_dataset():
    with patch("FBD.fbd.SupabaseConnection.init") as mock_init:
        fbd = FBD()
        mock_init.assert_called_once()
        assert fbd.dataset is None


def test_init_with_valid_dataset():
    with patch("FBD.fbd.SupabaseConnection.init"), \
         patch("FBD.fbd.Downloader.search_file") as mock_search:

        mock_search.return_value = {"status": "ok", "dataset": "dataset_1"}

        fbd = FBD("dataset_1")
        assert fbd.dataset == "dataset_1"

def test_download_file_invalid_dataset_raises():
    with patch("FBD.fbd.Downloader.download_file") as mock_dl:
        mock_dl.return_value = {
            "status": "error",
            "message": "Dataset not found"
        }

        fbd = FBD("bad_dataset")

        with pytest.raises(ValueError, match="Dataset not found"):
            fbd.download_file()


def test_method_sets_dataset():
    with patch("FBD.fbd.SupabaseConnection.init"), \
         patch("FBD.fbd.Downloader.search_file") as mock_search:

        mock_search.return_value = {"status": "ok", "dataset": "abc"}

        fbd = FBD()
        fbd.set_dataset("abc")

        assert fbd.dataset == "abc"


def test_method_reset_dataset():
    with patch("FBD.fbd.SupabaseConnection.init"):
        fbd = FBD("abc")
        fbd.reset_dataset()
        assert fbd.dataset is None



def test_search_file_uses_internal_dataset():
    with patch("FBD.fbd.SupabaseConnection.init"), \
         patch("FBD.fbd.Downloader.search_file") as mock_search:

        mock_search.return_value = {"status": "ok", "dataset": "internal_dataset"}

        fbd = FBD("internal_dataset")
        result = fbd.search_file()

        assert result == "internal_dataset"


def test_search_file_without_dataset_raises():
    with patch("FBD.fbd.SupabaseConnection.init"):
        fbd = FBD()

        with pytest.raises(ValueError):
            fbd.search_file()


def test_search_file_not_found_resets_dataset():
    with patch("FBD.fbd.SupabaseConnection.init"), \
         patch("FBD.fbd.Downloader.search_file") as mock_search:

        mock_search.return_value = {
            "status": "not_found",
            "message": "Not found"
        }

        fbd = FBD("old_dataset")

        with pytest.raises(ValueError):
            fbd.search_file("bad_dataset")

        assert fbd.dataset is None


def test_search_file_returns_matches():
    with patch("FBD.fbd.SupabaseConnection.init"), \
         patch("FBD.fbd.Downloader.search_file") as mock_search:

        mock_search.return_value = {
            "status": "multiple",
            "match": ["dataset1", "dataset2"]
        }

        fbd = FBD()
        result = fbd.search_file("multiple")

        assert result == ["dataset1", "dataset2"]



def test_download_file_success():
    with patch("FBD.fbd.SupabaseConnection.init"), \
         patch("FBD.fbd.Downloader.download_file") as mock_download:

        mock_download.return_value = {"data": {"a": 1}}

        fbd = FBD("valid_dataset")
        data = fbd.download_file()

        assert data == {"a": 1}


def test_download_file_invalid_structure():
    with patch("FBD.fbd.SupabaseConnection.init"), \
         patch("FBD.fbd.Downloader.download_file") as mock_download:

        mock_download.return_value = "invalid"

        fbd = FBD("valid_dataset")

        with pytest.raises(ValueError):
            fbd.download_file()


def test_download_file_without_dataset_raises():
    with patch("FBD.fbd.SupabaseConnection.init"):
        fbd = FBD()

        with pytest.raises(ValueError):
            fbd.download_file()



def test_get_column_descriptions_success():
    with patch("FBD.fbd.SupabaseConnection.init"), \
         patch("FBD.fbd.DataManager.get_column_descriptions") as mock_cols:

        mock_cols.return_value = {
            "status": "ok",
            "data": {"col1": "desc"}
        }

        fbd = FBD("valid_dataset")
        result = fbd.get_column_descriptions()

        assert result == {"col1": "desc"}


def test_get_column_descriptions_error():
    with patch("FBD.fbd.SupabaseConnection.init"), \
         patch("FBD.fbd.DataManager.get_column_descriptions") as mock_cols:

        mock_cols.return_value = {
            "status": "error",
            "message": "Something failed"
        }

        fbd = FBD("valid_dataset")

        with pytest.raises(ValueError):
            fbd.get_column_descriptions()
            
def test_get_description():
    with patch("FBD.fbd.DataManager.get_description") as mock:
        mock.return_value = "description"
        fbd = FBD("dataset")
        result = fbd.get_description()
        assert result == "description"



def test_get_categories():
    with patch("FBD.fbd.DataManager.get_categories") as mock:
        mock.return_value = ["cat1", "cat2"]

        result = FBD.get_categories()
        assert result == ["cat1", "cat2"]


def test_get_files_by_category():
    with patch("FBD.fbd.DataManager.get_files_by_category") as mock:
        mock.return_value = ["file1"]

        result = FBD.get_files_by_category("cat")
        assert result == ["file1"]

