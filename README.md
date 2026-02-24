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

### Dataset
The parquet datasets look like this:

```bash
shape: (148_445_536, 49)
┌────────┬──────────┬────────────────────┬────────────────┬───┬───────────────┬──────────────────┬───────────────────┬──────┐
│ N_PROF ┆ N_LEVELS ┆ DATA_TYPE          ┆ FORMAT_VERSION ┆ … ┆ PSAL_ADJUSTED ┆ PSAL_ADJUSTED_QC ┆ PSAL_ADJUSTED_ERR ┆ year │
│ ---    ┆ ---      ┆ ---                ┆ ---            ┆   ┆ ---           ┆ ---              ┆ OR                ┆ ---  │
│ i64    ┆ i64      ┆ binary             ┆ binary         ┆   ┆ f32           ┆ binary           ┆ ---               ┆ i32  │
│        ┆          ┆                    ┆                ┆   ┆               ┆                  ┆ f32               ┆      │
╞════════╪══════════╪════════════════════╪════════════════╪═══╪═══════════════╪══════════════════╪═══════════════════╪══════╡
│ 0      ┆ 0        ┆ b"Argo\x20profile\ ┆ b"3.1\x20"     ┆ … ┆ 34.199169     ┆ b"1"             ┆ 0.01              ┆ 1999 │
│        ┆          ┆ x20\x20\x20\…      ┆                ┆   ┆               ┆                  ┆                   ┆      │
│ 0      ┆ 1        ┆ b"Argo\x20profile\ ┆ b"3.1\x20"     ┆ … ┆ 34.19614      ┆ b"1"             ┆ 0.01              ┆ 1999 │
│        ┆          ┆ x20\x20\x20\…      ┆                ┆   ┆               ┆                  ┆                   ┆      │
│ 0      ┆ 2        ┆ b"Argo\x20profile\ ┆ b"3.1\x20"     ┆ … ┆ 34.198139     ┆ b"1"             ┆ 0.01              ┆ 1999 │
│        ┆          ┆ x20\x20\x20\…      ┆                ┆   ┆               ┆                  ┆                   ┆      │
│ 0      ┆ 3        ┆ b"Argo\x20profile\ ┆ b"3.1\x20"     ┆ … ┆ 34.15593      ┆ b"1"             ┆ 0.01              ┆ 1999 │
│        ┆          ┆ x20\x20\x20\…      ┆                ┆   ┆               ┆                  ┆                   ┆      │
│ 0      ┆ 4        ┆ b"Argo\x20profile\ ┆ b"3.1\x20"     ┆ … ┆ 34.06649      ┆ b"1"             ┆ 0.01              ┆ 1999 │
│        ┆          ┆ x20\x20\x20\…      ┆                ┆   ┆               ┆                  ┆                   ┆      │
│ …      ┆ …        ┆ …                  ┆ …              ┆ … ┆ …             ┆ …                ┆ …                 ┆ …    │
│ 82     ┆ 722      ┆ b"Argo\x20profile\ ┆ b"3.1\x20"     ┆ … ┆ null          ┆ null             ┆ null              ┆ 2025 │
│        ┆          ┆ x20\x20\x20\…      ┆                ┆   ┆               ┆                  ┆                   ┆      │
│ 82     ┆ 723      ┆ b"Argo\x20profile\ ┆ b"3.1\x20"     ┆ … ┆ null          ┆ null             ┆ null              ┆ 2025 │
│        ┆          ┆ x20\x20\x20\…      ┆                ┆   ┆               ┆                  ┆                   ┆      │
│ 82     ┆ 724      ┆ b"Argo\x20profile\ ┆ b"3.1\x20"     ┆ … ┆ null          ┆ null             ┆ null              ┆ 2025 │
│        ┆          ┆ x20\x20\x20\…      ┆                ┆   ┆               ┆                  ┆                   ┆      │
│ 82     ┆ 725      ┆ b"Argo\x20profile\ ┆ b"3.1\x20"     ┆ … ┆ null          ┆ null             ┆ null              ┆ 2025 │
│        ┆          ┆ x20\x20\x20\…      ┆                ┆   ┆               ┆                  ┆                   ┆      │
│ 82     ┆ 726      ┆ b"Argo\x20profile\ ┆ b"3.1\x20"     ┆ … ┆ null          ┆ null             ┆ null              ┆ 2025 │
│        ┆          ┆ x20\x20\x20\…      ┆                ┆   ┆               ┆                  ┆                   ┆      │
└────────┴──────────┴────────────────────┴────────────────┴───┴───────────────┴──────────────────┴───────────────────┴──────┘
```

## [Cloud](cloud.py)
Time some cloud readings of data.

### Hive Partitioning
Also utilise the hive partitioning for read.

## Results
| Dataset | Partition Scheme | # Files | File Size (gb) | Read whole dataset time (s) | Time to read 2020 year (s) | Hive Partitioning |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Argo (CSIRO subset) | netcdf File per netcdf file | 1179 | 7.60 | na | na | na |
| Argo (CSIRO subset) | Parquet file per netcdf file | 1179 | 2.49 | 74.08 | 26.75 | FALSE |
| Argo (CSIRO subset) | Parquet file/s per year | 42 | **1.37** | **34.91** | 5.05 | FALSE |
| Argo (CSIRO subset) | Parquet file/s per year | na | na | na | **5.04** | TRUE |
| Argo (CSIRO subset) | Parquet file/s per year per file | 7048 | 1.64 | 95.7 | 62.74 | FALSE |
| Argo (CSIRO subset) | Parquet file/s per year per file | na | na | na | 9.15 | TRUE |

### Takeaways
What we see in these (admittedly small and unrepeated results) is:

1. Correlation in file size and time to read; less data over the wire `==` good
2. Inverse correlation in # of files and time to read; more https sessions `==` bad
3. Push down filtering (filtering on parquet metadata) is fast, but affected by # files; more files `==` more metadata `==` bad
4. Parquet compresses data far better than netcdf; parquet compression is a free good
5. Parquet compresses data better when organised such that similar data is next to each other; better parquet compression requires tuning