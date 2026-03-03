import pathlib
import polars
import prefect


@prefect.task(task_run_name="{key}")
def sink(
    lf: polars.LazyFrame,
    key: list[polars.Expr],
    base_path: pathlib.Path,
):
    logger = prefect.get_run_logger()
    logger.info(f"Partitioning to `{base_path}`: `{key}`")
    lf.sink_parquet(
        polars.PartitionBy(
            base_path=base_path,
            key=key,
        )
    )
    pass


@prefect.flow
def partition(
    parquet_source: pathlib.Path = pathlib.Path("parquet"),
    parquet_output: pathlib.Path = pathlib.Path("parquet_year_partitioned"),
):

    lf = polars.scan_parquet(
        source=list(parquet_source.glob("**/*.parquet")),
        missing_columns="insert",
        extra_columns="ignore",
    )

    # Partition by year
    sink(
        lf=lf,
        key=[
            polars.col("JULD").dt.year().alias("year"),
        ],
        base_path=parquet_output,
    )


if __name__ == "__main__":
    partition()
