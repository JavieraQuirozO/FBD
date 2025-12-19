from pathlib import Path
import gzip
import pandas as pd
import json
import obonet
import csv

class Parse:
    
    @staticmethod    
    def is_gzip(path):
        """
        Return True if the file is gzip-compressed, based on magic bytes.
        """
        with open(path, "rb") as f:
            return f.read(2) == b"\x1f\x8b"
        
    @staticmethod
    def clean_columns_name(df):
        df = df.copy()
        df.columns = df.columns.str.replace(r"^[#\s]+", "", regex=True)
        return df
        
    @staticmethod
    def clean_df(df):
        df = Parse.clean_columns_name(df)
        df = df.dropna(how="all")
        if df.columns[0].strip() == "":
            df = df.iloc[:, 1:]
        df = df[~df.iloc[:, 0].astype(str).str.startswith("## Finished")]
        return df   
          
    @staticmethod
    def detect_header_line(path: Path, sep="\t") -> int:
        """
        Automatically detect the header line of a TSV, even if the file contains
        leading metadata lines starting with '##'.

        A valid header is defined as:
        - A line containing two or more columns when split by the separator.
        - The next line must have the same number of columns.

        Returns
        -------
        int
            Index of the detected header line.
        """
        opener = gzip.open if Parse.is_gzip(path) else open
    
        with opener(path, "rt", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    
        for i, line in enumerate(lines):
            cols = line.rstrip("\n").split(sep)
    
            if len(cols) < 2:
                continue
    
            if i + 1 < len(lines):
                next_cols = lines[i + 1].rstrip("\n").split(sep)
    
                if len(next_cols) == len(cols):
                    return i
    
        raise RuntimeError("Unable to detect a valid header line in the TSV file.")
    
    @staticmethod
    def decompress_gz(src_path, delete_compressed=True):
        """
        Decompress a .gz file and optionally delete the original compressed file.

        Args:
        ----------
        src_path : str | Path
            Path to the .gz file.
        delete_compressed : bool
            If True, remove the .gz file after decompression.

        Returns
        -------
        Path
            Path to the decompressed file.
        """
        src_path = Path(src_path)

        if not src_path.exists():
            raise FileNotFoundError(f"Gzip file not found: {src_path}")

        if src_path.suffix != ".gz":
            raise ValueError(f"The file does not have a .gz extension: {src_path}")

        output_path = src_path.with_suffix("")

        with gzip.open(src_path, "rb") as f_in:
            with open(output_path, "wb") as f_out:
                f_out.write(f_in.read())

        if delete_compressed:
            try:
                src_path.unlink()
            except Exception as e:
                print(f"Could not delete compressed file: {e}")

        return output_path
    
    @staticmethod
    def tsv_to_df(file_path: str | Path, header: int | None = 0):
        """
        Load a TSV file into a pandas DataFrame, handling both normal and gzip-compressed files.

        Args:
        ----------
            file_path : str | Path
            Path to the TSV file.
            header : int | None
                Row index of the header. If None, header is auto-detected using detect_header_line().

        Returns
        -------
        dict
            Contains filename, header index, and the resulting DataFrame.
        """
        file_path = Path(file_path)
        
        if header is None:
            header = Parse.detect_header_line(file_path)
            
        
        skiprows = header - 1
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            if Parse.is_gzip(file_path):
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    df = pd.read_csv(
                        f,
                        sep="\t",
                        skiprows=skiprows,
                        header=0,
                        dtype=str,
                        engine="python",
                        on_bad_lines="skip",
                    )
            else:
                df = pd.read_csv(
                    file_path,
                    sep="\t",
                    skiprows=skiprows,
                    header=0,
                    dtype=str,
                    engine="python",
                    on_bad_lines="skip",
                )
        
        except Exception as e:
            raise RuntimeError(
                f"Error reading TSV '{file_path}': {e}"
            )
            
        data = Parse.clean_df(df)  
        
        return {
            "filename": file_path.name, 
            "header": header, 
            "data": data
        }
    
    @staticmethod
    def affy_to_df(file_path: str | Path, to_dict: bool = False):
        """
        Load a TSV file into a pandas DataFrame, handling both normal and gzip-compressed files.

        Args:
        ----------
            file_path : str | Path
                Path to the TSV file.
            header : int | None
                Row index of the header. If None, header is auto-detected using detect_header_line().

        Returns
        -------
        dict
            Contains filename, header index, and the resulting DataFrame.
        """
        file_path = Path(file_path)
        
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            if Parse.is_gzip(file_path):
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    df = pd.read_csv(
                        f,
                        sep="\t",
                        header=None,
                        dtype=str,
                        engine="python",
                        on_bad_lines="skip",
                    )
            else:
                df = pd.read_csv(
                    file_path,
                    sep="\t",
                    header=None,
                    dtype=str,
                    engine="python",
                    on_bad_lines="skip",
                )
        
        except Exception as e:
            raise RuntimeError(
                f"Error reading TSV '{file_path}': {e}"
            )
            
        df = df.dropna(how="all")
        df = df[~df.iloc[:, 0].astype(str).str.startswith("## Finished")]
        dict_affy = {}
        for i in df.index:
           dict_affy[df.iloc[i, 0]] = list(df.iloc[i, 1:].dropna())    
        return {
            "filename": file_path.name, 
            "data": dict_affy
        }
    
    @staticmethod
    def json_to_df(file_path: str | Path):
        """
        Load a JSON file and return either:
        - A raw Python dict, or
        - A DataFrame if the JSON contains a top-level 'data' key.

        Handles both plain JSON and gzip-compressed JSON files.
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"JSON file not found: {file_path}")

        try:
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                data = json.load(f)

        except gzip.BadGzipFile:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                raise RuntimeError(f"Error reading JSON '{file_path}': {e}")

        except Exception as e:
            raise RuntimeError(f"Error opening gzip JSON '{file_path}': {e}")

        if isinstance(data, dict) and "data" in data:
            try:
                return pd.DataFrame(data["data"])
            except Exception:
                return data

        return data
    
    @staticmethod
    def obo_to_graph(file_path: str | Path):
        """
        Load an OBO ontology file into a networkx graph object using obonet.

        Supports both gzip-compressed and plain OBO files.
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"OBO file not found: {file_path}")

        for open_fn, mode in [(gzip.open, "rt"), (open, "r")]:
            try:
                with open_fn(file_path, mode) as f:
                    graph = obonet.read_obo(f)
                    return graph
            except gzip.BadGzipFile:
                continue
            except Exception as e:
                raise RuntimeError(f"Error reading '{file_path}': {e}")

        raise RuntimeError(f"Unable to process OBO file: {file_path}")

    @staticmethod
    def txt_to_df(file_path: str | Path, sep: str = "\t"):
        """
        Load a plain text file into a pandas DataFrame.
        Default separator is a tab, but can be customized.
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"TXT file not found: {file_path}")

        try:
            df = pd.read_csv(file_path, sep=sep, encoding="utf-8", engine="python")
            return Parse.clean_df(df)  
        except Exception as e:
            raise RuntimeError(f"Error reading TXT '{file_path}': {e}")
    
    @staticmethod    
    def fb_to_df(file_path, start_line, columns): #unsupported file extension at the moment
        """
        Parse a FlyBase-style tab-delimited file (.fb), compressed or uncompressed.

        Args:
        ----------
        file_path : str | Path
        start_line : int
            Line index where the actual table starts.
        columns : list[str]
            Column names for the resulting DataFrame.

        Returns
        -------
        DataFrame
            Parsed and column-aligned DataFrame.
        """
        data = None
        file_path = Path(file_path)

        try:
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter="\t")
                data = list(reader)

        except gzip.BadGzipFile:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    reader = csv.reader(f, delimiter="\t")
                    data = list(reader)
            except Exception as e:
                raise RuntimeError(f"Error reading plain-text file '{file_path}': {e}")

        except Exception as e:
            raise RuntimeError(f"Error opening gzip file '{file_path}': {e}")

        if data is None:
            raise RuntimeError("Failed to read the file as gzip or plain text.")

        cleaned_data = [
            row[:len(columns)] if len(row) > len(columns)
            else row + [""] * (len(columns) - len(row))
            for row in data[start_line:]
        ]

        df = pd.DataFrame(cleaned_data, columns=columns)
        return Parse.clean_df(df)  
