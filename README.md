# CloudNativeCapstone
Cloud Native Capstone Project

## [Extract](extract.py)
Extract a subset of the Argo data from imos public data bucket.

## [Catalog](catalog.py)
Build a STAC catalog on the extracted data.

### Collection
Argo.

### Items and Assets
The netcdf files.

## Transform

### [Initial](transform.py)
Initially transform the netcdf to parquet files with a `1:1` mapping.

1179 files.

### [Re-Partitioning](partition.py)
Re partition the files per year. Use hive partitioning schema.

42 files.

## [Cloud](cloud.py)
Time some cloud readings of data.

### Hive Partitioning
Also utilise the hive partitioning for read.

## Results
| Dataset | Partition Scheme | # Files | File Size (gb) | Read whole dataset time (s) | Time to read 2020 year (s) | Hive Partitioning |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Argo (CSIRO subset) | netcdf File per netcdf file | 1179 | 7.60 | na | na | na |
| Argo (CSIRO subset) | Parquet file per netcdf file | 1179 | 2.49 | 107 | 69.35 | FALSE |
| Argo (CSIRO subset) | Parquet file/s per year | 42 | **1.37** | **32** | 35.05 | FALSE |
| Argo (CSIRO subset) | Parquet file/s per year | na | na | na | **4.54** | TRUE |