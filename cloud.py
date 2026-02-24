import polars
import timer
import prefect
import datetime

@prefect.task(
    log_prints=True,
    task_run_name="{source}"
)
def time_lf_filter_and_collect(
    source: str,
    filter_expression: polars.Expr,
    hive_partitioning=False,
):
    
    lf = polars.scan_parquet(
        source=source,
        storage_options={"skip_signature": "true"},
        hive_partitioning=hive_partitioning,
    )

    with timer.timeit(source):
        df = lf.filter(filter_expression).collect()
        print(df)


@prefect.flow
def benchmark_cloud_native():

    time_lf_filter_and_collect(
        source="s3://data-uplift-public/capstone/parquet_year_partitioned/",
        filter_expression=(
            polars.col("year").eq(2020)
        ),
        hive_partitioning=True,
    )

    time_lf_filter_and_collect(
        source="s3://data-uplift-public/capstone/parquet_year_partitioned/",
        filter_expression=(
            polars.col("JULD").ge(datetime.date(2020, 1, 1))
            & polars.col("JULD").le(datetime.date(2020, 12, 31))
        ),
    )

    time_lf_filter_and_collect(
        source="s3://data-uplift-public/capstone/parquet/",
        filter_expression=(
            polars.col("JULD").ge(datetime.date(2020, 1, 1))
            & polars.col("JULD").le(datetime.date(2020, 12, 31))
        ),
    )

    time_lf_filter_and_collect(
        source="s3://data-uplift-public/capstone/parquet_year_file_partitioned/",
        filter_expression=(
            polars.col("year").eq(2020)
        ),
        hive_partitioning=True,
    )
    
    time_lf_filter_and_collect(
        source="s3://data-uplift-public/capstone/parquet_year_file_partitioned/",
        filter_expression=(
            polars.col("JULD").ge(datetime.date(2020, 1, 1))
            & polars.col("JULD").le(datetime.date(2020, 12, 31))
        ),
    )

if __name__ == "__main__":
    benchmark_cloud_native()