# -*- coding: utf-8 -*-

from FBD.core.supabase_client import SupabaseConnection


class DataManager:
    """
    Provides high-level data access utilities for categories and file entries
    stored in Supabase, and return processed, application-friendly structures.

    This class is stateless and uses class/static methods exclusively.
    """

    SupabaseConnection.init()
    
    @staticmethod
    def get_categories():
        """
        Returns a list of category names from the `categories` table.

        Returns:
            list[str]: List of category names.
        """
        rows = SupabaseConnection.fetch_table("categories")
        return [row["category"] for row in rows if "category" in row]

    @staticmethod
    def get_files_by_category(category_name=None):
        """
        Returns datasets grouped by category, or from a specific category if provided.

        Args:
            category_name (str | None):
                If provided, returns only datasets belonging to this category.

        Returns:
            dict[str, list[str]] | list[str]:
                - If no category is given: a dict {category_name: [datasets]}.
                - If a category is given: a list of datasets from that category.
        """
        categories = SupabaseConnection.fetch_table("categories")
        links = SupabaseConnection.fetch_table("links")

        links = [link for link in links if link.get("link")]

        if category_name:
            category = next(
                (c for c in categories if c["category"] == category_name),
                None
            )
            if not category:
                return []

            return [
                link["dataset"]
                for link in links
                if link["category_id"] == category["id"]
            ]
        result = {}
        for c in categories:
            datasets = [
                link["dataset"]
                for link in links
                if link["category_id"] == c["id"]
            ]
            if datasets:
                result[c["category"]] = datasets

        return result

    @staticmethod
    def search_files(dataset):
        """
        Performs a case-insensitive search for datasets in the `links` table.

        Args:
            dataset (str): A partial dataset to search for.

        Returns:
            dict[str, list[str]]:
                Mapping of categories to matching datasets.
                Excludes records without a valid link.
        """
        client = SupabaseConnection.connect()

        response = (
            client.table("links")
            .select("dataset, link, categories(category)")
            .ilike("dataset", f"%{dataset}%")
            .execute()
        )

        rows = response.data or []

        rows = [r for r in rows if r.get("link")]

        result = {}
        for row in rows:
            category_name = row.get("categories", {}).get("category", "Uncategorized")
            dataset_value = row.get("dataset")

            if dataset_value:
                result.setdefault(category_name, []).append(dataset_value)

        return result

    @staticmethod
    def get_description(dataset: str):
        """
        Returns the description for a specific dataset.

        Args:
            dataset (str): The exact dataset to search for.

        Returns:
            str | None: The description, or None if not found or invalid.
        """
        client = SupabaseConnection.connect()

        response = (
            client.table("links")
            .select("description, link")
            .eq("dataset", dataset)
            .limit(1)
            .execute()
        )

        rows = response.data or []
        if not rows:
            print(f"Not found: '{dataset}'")
            return None

        row = rows[0]

        if not row.get("link"):
            print(f"Not found: '{dataset}' (no valid link)")
            return None

        return row.get("description")

    @staticmethod
    def get_header_line(dataset: str) -> int:
        """
        Returns the header line number associated with a specific dataset.

        Args:
            dataset (str): File dataset.

        Returns:
            int | None: The header line number, or None if not set.

        Raises:
            ValueError: If no matching record is found.
        """
        client = SupabaseConnection.connect()

        response = (
            client.table("links")
            .select("header")
            .eq("dataset", dataset)
            .single()
            .execute()
        )

        if not response.data:
            raise ValueError(f"No header information found for dataset '{dataset}'.")

        header_line = response.data.get("header")
        return int(header_line) if header_line is not None else None

    @staticmethod
    def set_header_line(dataset: str, header_line: int | None) -> dict:
        """
        Updates the `header` field for a given dataset.

        Args:
            dataset (str): File dataset to update.
            header_line (int | None): New header line value.

        Returns:
            dict: Operation status and response payload.
        """
        client = SupabaseConnection.connect()

        if not dataset or dataset.strip() == "":
            raise ValueError("dataset cannot be empty.")

        try:
            response = (
                client.table("links")
                .update({"header": header_line})
                .eq("dataset", dataset)
                .execute()
            )

            if not response.data:
                raise ValueError(
                    f"No record found with dataset '{dataset}' for update."
                )

            return {
                "status": "success",
                "message": f"Header updated for dataset '{dataset}'.",
                "data": response.data,
            }

        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to update header: {str(e)}",
            }

    @staticmethod
    def get_filename(dataset: str):
        """
        Returns the filename associated with a given dataset.

        Args:
            dataset (str): The dataset to look up.

        Returns:
            str | None: Filename string, or None if missing.

        Raises:
            ValueError: If the dataset does not exist.
        """
        client = SupabaseConnection.connect()

        response = (
            client.table("links")
            .select("filename")
            .eq("dataset", dataset)
            .single()
            .execute()
        )

        if not response.data:
            raise ValueError(f"No filename found for dataset '{dataset}'.")

        filename = response.data.get("filename")
        return str(filename) if filename is not None else None


    @staticmethod
    def find_in_column_description(file_id: int, columns=None) -> dict:
        """
        Search the column_description table by file_id.
        
        - If columns is None or "all": returns all rows for that file_id.
        - If columns is a string: returns only that column.
        - If columns is a list: returns only those matching names.
        """

        client = SupabaseConnection.connect()

        query = (
            client.table("column_description")
            .select("df_column_name, description")
            .eq("file_id", file_id)
        )

        # Filter by column(s)
        if columns and columns != "all":
            if isinstance(columns, str):
                query = query.eq("df_column_name", columns)
            elif isinstance(columns, list):
                query = query.in_("df_column_name", columns)

        res = query.execute()
        rows = res.data or []

        if not rows:
            return {
                "status": "empty",
                "message": "No description found for the requested dataset.",
                "data": {}
            }

        data = {r["df_column_name"]: r["description"] for r in rows}

        return {
            "status": "ok",
            "data": data
        }


    @classmethod
    def get_column_descriptions(cls, dataset: str, columns="all") -> dict:
        """
        Given a dataset, return column descriptions.

        - First find file_id from 'links'.
        - Then call find_in_column_description().
        """

        client = SupabaseConnection.connect()

        res = (
            client.table("links")
            .select("id")
            .eq("dataset", dataset)
            .limit(1)
            .execute()
        )

        if not res.data:
            return {
                "status": "not_found",
                "message": f"dataset '{dataset}' not found. Use the exact dataset."
            }

        file_id = res.data[0]["id"]

        return cls.find_in_column_description(file_id, columns)