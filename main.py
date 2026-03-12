import pathlib

import prefect
import pyarrow

import config.argo_profile as argo_profile
from extract import extract
from transform import transform
from partition import partition


@prefect.flow
def main(
    s3_source: str = "s3://imos-data/IMOS/Argo/dac/",
    include_glob: str = "*_prof.nc",
    source_path: pathlib.Path = pathlib.Path("argo"),
    parquet_store_path: pathlib.Path = pathlib.Path("parquet"),
    parquet_output: pathlib.Path = pathlib.Path("parquet_year_partitioned"),
    output_schema: pyarrow.Schema = argo_profile.SCHEMA,
    required_variables: set[str] = argo_profile.REQUIRED_VARIABLES,
    required_dims: set[str] = argo_profile.REQUIRED_DIMS,
    drop_dims: set[str] = argo_profile.DROP_DIMS,
):
    # extract(s3_source=s3_source, local_path=source_path, include_glob=include_glob)
    transform(
        source_path=source_path,
        parquet_store_path=parquet_store_path,
        output_schema=output_schema,
        required_variables=required_variables,
        required_dims=required_dims,
        drop_dims=drop_dims,
    )
    partition(parquet_source=parquet_store_path, parquet_output=parquet_output)


if __name__ == "__main__":
    main()
