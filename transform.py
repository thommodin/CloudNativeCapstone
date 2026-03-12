import prefect
import prefect.task_runners
import prefect.futures
import pathlib
import xarray
import pyarrow
import pyarrow.dataset
import polars
import polars_h3

import config.argo_profile as argo_profile


def _transform_netcdf_to_parquet(
    ds: xarray.Dataset,
    store_path: pathlib.Path,
    file_name: str,
    collection_name: str,
    output_schema: pyarrow.Schema,
) -> pyarrow.dataset.Dataset:

    # Pre-filter to only schema variables before loading into memory
    schema_names = {field.name for field in output_schema}
    ds = ds.drop_vars([v for v in ds.data_vars if v not in schema_names])

    # Load to Polars DataFrame and select schema columns
    df = polars.from_pandas(ds.to_dataframe().reset_index())
    del ds

    # Conform to Schema
    df = (
        df
        
        # Take only required columns
        .select(
            [
                field.name 
                for field in output_schema 
                if field.name in df.columns
            ],
        )

        # Add missing columns
        .with_columns(
            [
                polars.lit(None).alias(field.name)
                for field in output_schema
                if field.name not in df.columns
            ],
        )
    )

    # Filter
    df = (
        df

        # Filter to rows that have a location
        .filter(
            polars.col("JULD").is_not_null()
            & polars.col("LATITUDE").is_not_null()
            & polars.col("LONGITUDE").is_not_null()
        )

        # Filter to rows where there is at least one of the measurements
        .filter(
            polars.col("PRES").is_not_null()
            | polars.col("TEMP").is_not_null()
            | polars.col("PSAL").is_not_null()
        )
    )

    # Partition Set Up
    df = (
        df

        # Add partition columns
        .with_columns(
            polars.lit(collection_name).alias("collection"),
            polars.lit(file_name).alias("file"),
        )

        # Sort by future partition structure
        .sort(
            polars.col("JULD"),
            polars.col("N_PROF"),
            polars.col("N_LEVELS"),
        )
    )
    
    # Convert to pyarrow table and validate by casting with partition fields
    output_schema = (
        output_schema
        .append(pyarrow.field("collection", pyarrow.string()))
        .append(pyarrow.field("file", pyarrow.string()))
    )
    table = df.to_arrow().select(output_schema.names).cast(output_schema)
    del df

    # Set write options for better compression
    # Save on S3, trade-off back toward time instead of space
    parquet_format = pyarrow.dataset.ParquetFileFormat()
    parquet_options = parquet_format.make_write_options(
        # version="2.6",
        compression="zstd",
        compression_level=11,
        use_dictionary=True,
        write_statistics=True,
        use_byte_stream_split=True,
    )

    return pyarrow.dataset.write_dataset(
        data=table,
        base_dir=store_path,
        partitioning=["collection", "file"],
        partitioning_flavor="hive",
        format=parquet_format,
        file_options=parquet_options,
        existing_data_behavior="delete_matching",
    )


@prefect.task(task_run_name="transform-nc-{path}")
def transform_netcdf(
    path: pathlib.Path,
    parquet_store_path: pathlib.Path = pathlib.Path("parquet"),
    output_schema: pyarrow.Schema = argo_profile.SCHEMA,
    required_variables: set = argo_profile.REQUIRED_VARIABLES,
    required_dims: set = argo_profile.REQUIRED_DIMS,
    drop_dims: set[str] = argo_profile.DROP_DIMS,
):

    # Open Dataset
    ds = xarray.open_dataset(path)

    # Drop dimensions
    ds = ds.drop_dims(drop_dims)

    # Check missing required variables
    missing_vars = required_variables - set(ds.data_vars)
    missing_dims = required_dims - set(ds.dims)
    if missing_vars or missing_dims:
        raise ValueError(f"Skipping {path} - missing variables: {missing_vars or None }, missing dims: {missing_dims or None}")

    return _transform_netcdf_to_parquet(
        ds=ds,
        store_path=parquet_store_path,
        file_name=path.name,
        collection_name=path.parts[1],
        output_schema=output_schema,
    )


@prefect.flow(
    task_runner=prefect.task_runners.ProcessPoolTaskRunner(minmax_workers=16),
)
def transform(
    netcdf_source: pathlib.Path = pathlib.Path("argo"),
    parquet_output: pathlib.Path = pathlib.Path("parquet/file"),
    output_schema: pyarrow.Schema = argo_profile.SCHEMA,
    required_variables: set[str] = argo_profile.REQUIRED_VARIABLES,
    required_dims: set[str] = argo_profile.REQUIRED_DIMS,
    drop_dims: set[str] = argo_profile.DROP_DIMS,
):
    parquet_output.mkdir(
        exist_ok=True,
        parents=True,
    )
    prefect.futures.wait(
        [
            transform_netcdf.submit(
                path,
                parquet_store_path=parquet_output,
                output_schema=output_schema,
                required_variables=required_variables,
                required_dims=required_dims,
                drop_dims=drop_dims,
            )
            for path in netcdf_source.glob("**/*_prof.nc")
        ]
    )


if __name__ == "__main__":
    transform()
