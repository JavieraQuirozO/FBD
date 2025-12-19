from .client.downloader import Downloader
from .client.data_manager import DataManager
from .core.supabase_client import SupabaseConnection


class FBD:
    def __init__(self, dataset: str | None = None):
        SupabaseConnection.init()
        self.dataset = dataset or None
        

    def set_dataset(self, dataset: str):
        if dataset is not None:
            result = Downloader.search_file(dataset)
            if result.get("status") != "ok":
                raise ValueError(result.get("message", "Dataset not found"))
            self.dataset = dataset

    def reset_dataset(self):
        self.dataset = None


    def search_file(self, dataset: str | None = None):
        dataset_to_search = dataset or self.dataset

        if dataset_to_search is None:
            raise ValueError("No dataset provided")

        result = Downloader.search_file(dataset_to_search)

        status = result.get("status")

        if status == "ok":
            self.dataset = dataset_to_search
            return result["dataset"]

        if status in ("multiple", "partial"):
            return result.get("match")

        # not_found / error
        self.dataset = None
        raise ValueError(result.get("message", "Not found"))


    def download_file(self, dataset: str | None = None):
        dataset = dataset or self.dataset
        if dataset is None:
            raise ValueError("No dataset selected")
    
        result = Downloader.download_file(dataset)
    
        if not isinstance(result, dict):
            raise ValueError("Invalid download response")
            
        if "data" in result:
            return result["data"]
    
        if result.get("status") != "ok":
            raise ValueError(result.get("message", "Download failed"))


    def get_column_descriptions(self, dataset: str | None = None, columns: str | None = "all" ):
        
        dataset = dataset or self.dataset
        if dataset is None:
            raise ValueError("No dataset selected")

        result = DataManager.get_column_descriptions(dataset, columns)

        if result.get("status") != "ok":
            raise ValueError(result.get("message", "Unknown error"))

        return result["data"]
    
    
    def get_description(self, dataset: str | None = None):
        dataset = dataset or self.dataset
        return DataManager.get_description(dataset)

    @staticmethod
    def get_categories():
        return DataManager.get_categories()

    @staticmethod
    def get_files_by_category(category: str | None = None):
        return DataManager.get_files_by_category(category)

