import pyarrow.dataset
import pathlib
import polars
import prefect


@prefect.task
def partition():
    ds = pyarrow.dataset.dataset(
        source=pathlib.Path("parquet"),
    )
    lf = polars.scan_pyarrow_dataset(ds)
    lf.sink_parquet(
        polars.PartitionBy(
            base_path=pathlib.Path("parquet_partitioned"),
            key=[
                polars.col("JULD").dt.year().alias("year"),
            ],
        )
    )
