# FlyBase Downloads (FBD)

**FlyBase Downloads (FBD)** is a lightweight Python library that provides programmatic access to selected datasets from **FlyBase**, allowing researchers to search, download, and load biological datasets directly into Python objects (e.g. pandas DataFrames) for analysis, exploration, and machine learning workflows.

**Important:** This is an **unofficial** library.  
All data is **retrieved directly from FlyBase**, and availability depends entirely on FlyBase’s public resources.

---

## Purpose

This library is designed for:

- Academic and educational use
- Bioinformatics analysis
- Data exploration in Jupyter notebooks
- Machine learning and data science pipelines

It is **not intended for high-frequency or automated bulk downloading**.

---

## Installation and Import

```bash
pip install flybasedownloads
```

``` python
from FBD.fbd import FBD
```
---

## Basic Usage

# List available categories

```python
fbd = FBD()
categories = fbd.get_categories()
print(categories)
```

# List datasets by category

```python
fbd = FBD()
files_by_cat = fbd.get_files_by_category("Genes")
print(files_by_cat)
```

# Search for a dataset (exact or partial match)

**Exact match**

```python
fbd = FBD()
dataset = fbd.search_file("gene_genetic_interactions")
print("Selected dataset:", dataset)
```

**Partial matches**

```python
fbd = FBD()
matches = fbd.search_file("gene")
print("Possible datasets:")
for m in matches:
    print("-", m)
```
---

## Download datasets

# Initialize with a dataset and download directly

```python
fbd = FBD("gene_genetic_interactions")
df = fbd.download_file()
```

# Download multiple datasets sequentially

```python
fbd = FBD()
fbd.set_dataset("antibody_information")
df1 = fbd.download_file()
fbd.set_dataset("gene_genetic_interactions")
df2 = fbd.download_file()
```

---

## Dataset metadata

# Retrieve column descriptions

``` python
column_info = fbd.get_column_descriptions(
    columns=[
        "Starting_gene(s)_symbol",
        "Ending_gene(s)_symbol",
        "Interaction_type"
    ]
)
print(column_info)
```
---

## Example: preparing data for ML

``` python
X = df[[
    "Starting_gene(s)_symbol",
    "Ending_gene(s)_symbol"
]]

y = df["Interaction_type"]
```

---

## Rate limiting & responsible use

To protect shared infrastructure and respect FlyBase resources, **downloads are rate-limited by default.**
-The download limit is enforced internally
-The library is intended for **academic use only**
-Bulk or automated scraping is strongly discouraged

Advanced users can modify the download limits via the Config class and config.json, but:
**We strongly ask users NOT to bypass rate limits**,
as this project does not have the infrastructure to support large-scale or abusive usage.
---

## Data source & disclaimer

-All datasets are retrieved directly from FlyBase
-This library is **not** affiliated with FlyBase
-Dataset availability may change over time
-Some datasets may become unavailable or updated without notice

If you need guaranteed access or bulk data, please use FlyBase’s official distribution channels.
 **Official FlyBase Downloads Overview:** [FlyBase:Downloads_Overview](https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview)


---

## Citation
If you use this library in academic work, please cite **FlyBase** as the original data source.


