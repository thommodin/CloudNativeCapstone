import prefect
import prefect.task_runners
import prefect.futures
import pathlib
import xarray
import pyarrow
import pyarrow.dataset
import collections

import config.argo_profile as argo_profile


def _transform_netcdf_to_parquet(
    ds: xarray.Dataset,
    store_path: pathlib.Path,
    file_name: str,
    collection_name: str,
    output_schema: pyarrow.Schema,
) -> pyarrow.dataset.Dataset:

    store_path.mkdir(exist_ok=True)

    df = ds.to_dataframe().reset_index()
    selected_fields = [field for field in output_schema if field.name in df]
    table = (
        pyarrow.table(df)
        # Select only the columns we are interested in, and those that are in the df
        .select([field.name for field in selected_fields])
        # Cast using only the fields present in this file
        .cast(pyarrow.schema(selected_fields))
    )
    table = table.append_column(
        "collection",
        pyarrow.array(
            [collection_name] * len(table),
            pyarrow.string(),
        ),
    )
    table = table.append_column(
        "file",
        pyarrow.array(
            [file_name] * len(table),
            pyarrow.string(),
        ),
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
    logger = prefect.get_run_logger()

    ds = xarray.open_dataset(path)

    # Find the dimension structures
    dd = collections.defaultdict(list)
    for k, v in ds.variables.items():
        dd[v.dims].append(k)

    ds = ds.drop_dims(drop_dims)

    missing_vars = required_variables - set(ds.data_vars)
    missing_dims = required_dims - set(ds.dims)
    if missing_vars or missing_dims:
        logger.warning(
            "Skipping %s — missing variables: %s, missing dims: %s",
            path,
            missing_vars or "none",
            missing_dims or "none",
        )
        return None

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
    prefect.futures.wait
    (
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
