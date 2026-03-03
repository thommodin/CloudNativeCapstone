import prefect
import prefect.task_runners
import prefect.futures
import pathlib
import xarray
import pyarrow
import pyarrow.dataset
import collections

# Authoritative schema derived from the Argo profile NetCDF variable definitions.
# Hardcoded so that all-NaN columns are never mistyped as null.
ARGO_PROFILE_SCHEMA = pyarrow.schema(
    [
        pyarrow.field("N_PROF", pyarrow.int64()),
        pyarrow.field("N_LEVELS", pyarrow.int64()),
        pyarrow.field("DATA_TYPE", pyarrow.string()),
        pyarrow.field("FORMAT_VERSION", pyarrow.string()),
        pyarrow.field("HANDBOOK_VERSION", pyarrow.string()),
        pyarrow.field("REFERENCE_DATE_TIME", pyarrow.string()),
        pyarrow.field("DATE_CREATION", pyarrow.string()),
        pyarrow.field("DATE_UPDATE", pyarrow.string()),
        pyarrow.field("PLATFORM_NUMBER", pyarrow.string()),
        pyarrow.field("PROJECT_NAME", pyarrow.string()),
        pyarrow.field("PI_NAME", pyarrow.string()),
        pyarrow.field("CYCLE_NUMBER", pyarrow.float64()),
        pyarrow.field("DIRECTION", pyarrow.string()),
        pyarrow.field("DATA_CENTRE", pyarrow.string()),
        pyarrow.field("DC_REFERENCE", pyarrow.string()),
        pyarrow.field("DATA_STATE_INDICATOR", pyarrow.string()),
        pyarrow.field("DATA_MODE", pyarrow.string()),
        pyarrow.field("PLATFORM_TYPE", pyarrow.string()),
        pyarrow.field("FLOAT_SERIAL_NO", pyarrow.string()),
        pyarrow.field("FIRMWARE_VERSION", pyarrow.string()),
        pyarrow.field("WMO_INST_TYPE", pyarrow.string()),
        pyarrow.field("JULD", pyarrow.timestamp("ns")),
        pyarrow.field("JULD_QC", pyarrow.string()),
        pyarrow.field("JULD_LOCATION", pyarrow.timestamp("ns")),
        pyarrow.field("LATITUDE", pyarrow.float64()),
        pyarrow.field("LONGITUDE", pyarrow.float64()),
        pyarrow.field("POSITION_QC", pyarrow.string()),
        pyarrow.field("POSITIONING_SYSTEM", pyarrow.string()),
        pyarrow.field("PROFILE_PRES_QC", pyarrow.string()),
        pyarrow.field("PROFILE_TEMP_QC", pyarrow.string()),
        pyarrow.field("PROFILE_PSAL_QC", pyarrow.string()),
        pyarrow.field("VERTICAL_SAMPLING_SCHEME", pyarrow.string()),
        pyarrow.field("CONFIG_MISSION_NUMBER", pyarrow.float64()),
        pyarrow.field("PRES", pyarrow.float32()),
        pyarrow.field("PRES_QC", pyarrow.string()),
        pyarrow.field("PRES_ADJUSTED", pyarrow.float32()),
        pyarrow.field("PRES_ADJUSTED_QC", pyarrow.string()),
        pyarrow.field("PRES_ADJUSTED_ERROR", pyarrow.float32()),
        pyarrow.field("TEMP", pyarrow.float32()),
        pyarrow.field("TEMP_QC", pyarrow.string()),
        pyarrow.field("TEMP_ADJUSTED", pyarrow.float32()),
        pyarrow.field("TEMP_ADJUSTED_QC", pyarrow.string()),
        pyarrow.field("TEMP_ADJUSTED_ERROR", pyarrow.float32()),
        pyarrow.field("PSAL", pyarrow.float32()),
        pyarrow.field("PSAL_QC", pyarrow.string()),
        pyarrow.field("PSAL_ADJUSTED", pyarrow.float32()),
        pyarrow.field("PSAL_ADJUSTED_QC", pyarrow.string()),
        pyarrow.field("PSAL_ADJUSTED_ERROR", pyarrow.float32()),
    ]
)


def _transform_netcdf_to_parquet(
    ds: xarray.Dataset,
    store_path: pathlib.Path,
    file_name: str,
    collection_name: str,
) -> pyarrow.dataset.Dataset:

    store_path.mkdir(exist_ok=True)

    df = ds.to_dataframe().reset_index()
    selected_fields = [field for field in ARGO_PROFILE_SCHEMA if field.name in df]
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
):

    ds = xarray.open_dataset(path)

    # Find the dimension structures
    dd = collections.defaultdict(list)
    for k, v in ds.variables.items():
        dd[v.dims].append(k)

    ds = ds.drop_dims({"N_PARAM", "N_HISTORY", "N_CALIB"})

    return _transform_netcdf_to_parquet(
        ds=ds,
        store_path=parquet_store_path,
        file_name=path.name,
        collection_name=path.parts[1],
    )


@prefect.flow(
    task_runner=prefect.task_runners.ProcessPoolTaskRunner(max_workers=16),
)
def transform(
    path: pathlib.Path = pathlib.Path("argo"),
):
    prefect.futures.wait(
        [transform_netcdf.submit(path) for path in path.glob("**/*_prof.nc")]
    )


if __name__ == "__main__":
    transform()
