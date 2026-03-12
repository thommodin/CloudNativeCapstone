import pathlib
import polars
import polars_h3
import prefect
import prefect.cache_policies

@prefect.task(
    cache_policy=prefect.cache_policies.NO_CACHE,
)
def sink(
    lf: polars.LazyFrame,
    partition_by: polars.PartitionBy,
):

    logger = prefect.get_run_logger()
    logger.info(lf.explain())
    lf.sink_parquet(partition_by)

@prefect.flow
def partition(
    parquet_source: pathlib.Path = pathlib.Path("parquet/file"),
    parquet_output: pathlib.Path = pathlib.Path("parquet"),
):

    # Set up partitioning
    partition_path = parquet_output / "partition"
    defragment_path = parquet_output / "defragment"
    key = [
        polars.col("year"),
        polars.col("h3_index"),
    ]

    # Load file parquet
    lf = polars.scan_parquet(
        source=list(parquet_source.glob("**/*.parquet")),
        hive_partitioning=True,   
    )

    # Add partition columns
    lf = lf.with_columns(
            # Year
            polars.col("JULD").dt.year().alias("year"),

            # H3
            polars_h3.latlng_to_cell(
                lat=polars.col("LATITUDE"),
                lng=polars.col("LONGITUDE"),
                resolution=0,
                return_dtype=polars.Int64,
            ).alias("h3_index"),
    )

    # Sink partition
    sink(
        lf=lf,
        partition_by=polars.PartitionBy(
            base_path=partition_path,
            key=key
        ),
    )

    # Re-sink to defragment
    lf = polars.scan_parquet(
        source=list(partition_path.glob("**/*.parquet")),
        hive_partitioning=True,
    )

    # Sink defragmented and compress heavily
    sink(
        lf=lf,
        partition_by=polars.PartitionBy(
            base_path=defragment_path,
            key=key
        ),
    )


if __name__ == "__main__":
    partition()
