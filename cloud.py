import pyarrow
import pyarrow.dataset
import pyarrow.fs
import polars

FILE_SYSTEM = pyarrow.fs.S3FileSystem(
    region="ap-southeast-2", 
    anonymous=True,
)

ds = pyarrow.dataset.dataset(
    source="data-uplift-public/capstone/parquet_partitioned/",
    filesystem=FILE_SYSTEM,
)

print(ds.to_table())

# print(list(ds.get_fragments()))

# lf = polars.scan_pyarrow_dataset(ds)
# lf.filter(
#     polars.col("year").eq(2020)
# )

# df = lf.collect()