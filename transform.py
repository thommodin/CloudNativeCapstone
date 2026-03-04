import prefect
import prefect.task_runners
import prefect.futures
import pathlib
import xarray
import pyarrow
import pyarrow.dataset

import config.argo_profile as argo_profile


def _transform_netcdf_to_parquet(
    ds: xarray.Dataset,
    store_path: pathlib.Path,
    file_name: str,
    collection_name: str,
    output_schema: pyarrow.Schema,
) -> pyarrow.dataset.Dataset:
    
    logger = prefect.get_run_logger()

    # Pre-filter to only schema variables before loading into memory
    schema_names = {field.name for field in output_schema}
    ds = ds.drop_vars([v for v in ds.data_vars if v not in schema_names])

    # Load the dataframe and cut to output schema
    df = ds.to_dataframe().reset_index()
    df = df[[
        field.name
        for field in output_schema
        if field.name in df
    ]]
    del ds

    # Convert to pyarrow table
    table = pyarrow.table(data=df)
    del df

    # Find and fill missing fields in a single table reconstruction
    existing_columns = set(table.column_names)
    missing_fields = [field for field in output_schema if field.name not in existing_columns]
    if missing_fields:
        for missing_field in missing_fields:
            logger.info(f"Adding missing field {missing_field}")
        table = pyarrow.table({
            **{name: table.column(name) for name in table.column_names},
            **{f.name: pyarrow.nulls(len(table), f.type) for f in missing_fields},
        })
    
    # Validate by Cast
    table = table.select(output_schema.names).cast(output_schema)

    # Add collection and file columns
    table = table.append_column(
        field_="collection",
        column=pyarrow.repeat(collection_name, len(table)),
    )
    table = table.append_column(
        field_="file",
        column=pyarrow.repeat(file_name, len(table)),
    )

    return pyarrow.dataset.write_dataset(
        data=table,
        base_dir=store_path,
        partitioning=["collection", "file"],
        partitioning_flavor="hive",
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
    )


@prefect.task(task_run_name="transform-nc-{path}")
def transform_netcdf(
    path: pathlib.Path,
    parquet_store_path: pathlib.Path = pathlib.Path("parquet"),
    output_schema: pyarrow.Schema = argo_profile.SCHEMA,
    required_variables: set[str] = argo_profile.REQUIRED_VARIABLES,
    required_dims: set[str] = argo_profile.REQUIRED_DIMS,
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
    task_runner=prefect.task_runners.ProcessPoolTaskRunner(max_workers=16),
)
def transform(
    source_path: pathlib.Path = pathlib.Path("argo"),
    parquet_store_path: pathlib.Path = pathlib.Path("parquet"),
    output_schema: pyarrow.Schema = argo_profile.SCHEMA,
    required_variables: set[str] = argo_profile.REQUIRED_VARIABLES,
    required_dims: set[str] = argo_profile.REQUIRED_DIMS,
    drop_dims: set[str] = argo_profile.DROP_DIMS,
):
    parquet_store_path.mkdir(exist_ok=True)
    prefect.futures.wait(
        [
            transform_netcdf.submit(
                path,
                parquet_store_path=parquet_store_path,
                output_schema=output_schema,
                required_variables=required_variables,
                required_dims=required_dims,
                drop_dims=drop_dims,
            )
            for path in source_path.glob("**/*_prof.nc")
        ]
    )


if __name__ == "__main__":
    transform()
