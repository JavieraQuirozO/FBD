import pytest
from unittest.mock import patch, MagicMock
from FBD.client.data_manager import DataManager


def test_get_categories():
    mock_rows = [
        {"category": "Interactions"},
        {"category": "Genes"},
        {"other": "ignore"}
    ]

    with patch("FBD.client.data_manager.SupabaseConnection.fetch_table",
               return_value=mock_rows):

        categories = DataManager.get_categories()

        assert categories == ["Interactions", "Genes"]


def test_get_files_by_category_all():
    categories = [
        {"id": 1, "category": "CatA"},
        {"id": 2, "category": "CatB"}
    ]
    links = [
        {"dataset": "ds1", "category_id": 1, "link": "x"},
        {"dataset": "ds2", "category_id": 1, "link": "x"},
        {"dataset": "ds3", "category_id": 2, "link": "x"},
        {"dataset": "ds4", "category_id": 2, "link": None},  # ignored
    ]

    with patch("FBD.client.data_manager.SupabaseConnection.fetch_table",
               side_effect=[categories, links]):

        result = DataManager.get_files_by_category()

        assert result == {
            "CatA": ["ds1", "ds2"],
            "CatB": ["ds3"]
        }


def test_get_files_by_category_specific():
    categories = [{"id": 1, "category": "CatA"}]
    links = [
        {"dataset": "ds1", "category_id": 1, "link": "x"},
        {"dataset": "ds2", "category_id": 1, "link": "x"}
    ]

    with patch("FBD.client.data_manager.SupabaseConnection.fetch_table",
               side_effect=[categories, links]):

        result = DataManager.get_files_by_category("CatA")

        assert result == ["ds1", "ds2"]


def test_get_files_by_category_not_found():
    with patch("FBD.client.data_manager.SupabaseConnection.fetch_table",
               side_effect=[[], []]):

        result = DataManager.get_files_by_category("Unknown")

        assert result == []


def test_search_files():
    mock_client = MagicMock()
    mock_client.table.return_value.select.return_value.ilike.return_value.execute.return_value.data = [
        {
            "dataset": "gene_interactions",
            "link": "x",
            "categories": {"category": "Genes"}
        }
    ]

    with patch("FBD.client.data_manager.SupabaseConnection.connect",
               return_value=mock_client):

        result = DataManager.search_files("gene")

        assert result == {"Genes": ["gene_interactions"]}
        

def test_get_description_success():
    mock_client = MagicMock()
    mock_client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = [
        {"description": "Some description", "link": "x"}
    ]

    with patch("FBD.client.data_manager.SupabaseConnection.connect",
               return_value=mock_client):

        desc = DataManager.get_description("valid_dataset")

        assert desc == "Some description"


def test_get_description_not_found():
    mock_client = MagicMock()
    mock_client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = []

    with patch("FBD.client.data_manager.SupabaseConnection.connect",
               return_value=mock_client):

        desc = DataManager.get_description("missing")

        assert desc is None


def test_get_header_line_success():
    mock_client = MagicMock()
    mock_client.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value.data = {
        "header": 3
    }

    with patch("FBD.client.data_manager.SupabaseConnection.connect",
               return_value=mock_client):

        header = DataManager.get_header_line("dataset")

        assert header == 3


def test_get_header_line_not_found():
    mock_client = MagicMock()
    mock_client.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value.data = None

    with patch("FBD.client.data_manager.SupabaseConnection.connect",
               return_value=mock_client):

        with pytest.raises(ValueError):
            DataManager.get_header_line("dataset")


def test_set_header_line_success():
    mock_client = MagicMock()
    mock_client.table.return_value.update.return_value.eq.return_value.execute.return_value.data = [{"id": 1}]

    with patch("FBD.client.data_manager.SupabaseConnection.connect",
               return_value=mock_client):

        result = DataManager.set_header_line("dataset", 5)

        assert result["status"] == "success"


def test_set_header_line_empty_dataset():
    with pytest.raises(ValueError):
        DataManager.set_header_line("", 3)



def test_get_filename_success():
    mock_client = MagicMock()
    mock_client.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value.data = {
        "filename": "file.tsv"
    }

    with patch("FBD.client.data_manager.SupabaseConnection.connect",
               return_value=mock_client):

        filename = DataManager.get_filename("dataset")

        assert filename == "file.tsv"


def test_get_filename_not_found():
    mock_client = MagicMock()
    mock_client.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value.data = None

    with patch("FBD.client.data_manager.SupabaseConnection.connect",
               return_value=mock_client):

        with pytest.raises(ValueError):
            DataManager.get_filename("dataset")



def test_get_column_descriptions_success():
    mock_client = MagicMock()
    mock_client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = [
        {"id": 10}
    ]

    with patch("FBD.client.data_manager.SupabaseConnection.connect",
               return_value=mock_client), \
         patch("FBD.client.data_manager.DataManager.find_in_column_description",
               return_value={"status": "ok", "data": {"col": "desc"}}):

        result = DataManager.get_column_descriptions("dataset")

        assert result["status"] == "ok"
        assert "col" in result["data"]


def test_get_column_descriptions_not_found():
    mock_client = MagicMock()
    mock_client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = []

    with patch("FBD.client.data_manager.SupabaseConnection.connect",
               return_value=mock_client):

        result = DataManager.get_column_descriptions("missing")

        assert result["status"] == "not_found"

