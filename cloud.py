import polars
import timer

hive_partitioned_lf = polars.scan_parquet(
    source="s3://data-uplift-public/capstone/parquet_partitioned/",
    storage_options={"skip_signature": "true"},
    hive_partitioning=True,
)

year_partitioned_lf = polars.scan_parquet(
    source="s3://data-uplift-public/capstone/parquet_partitioned/",
    storage_options={"skip_signature": "true"},
)

file_partitioned_lf = polars.scan_parquet(
    source="s3://data-uplift-public/capstone/parquet/",
    storage_options={"skip_signature": "true"},
)

with timer.timeit("hive_partitioned_lf"):
    df = (
        hive_partitioned_lf
        .filter(
            polars.col("year").eq(2020)
        )
    ).collect()
    print(df)

with timer.timeit("year_partitioned_lf"):
    df = (
        year_partitioned_lf
        .filter(
            polars.col("JULD").dt.year().eq(2020)
        )
    ).collect()
    print(df)

with timer.timeit("file_partitioned_lf"):
    df = (
        file_partitioned_lf
        .filter(
            polars.col("JULD").dt.year().eq(2020)
        )
    ).collect()
    print(df)

