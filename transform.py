import prefect
import prefect.task_runners
import prefect.futures
import pystac
import pathlib
import xarray
import pyarrow
import pyarrow.dataset
import collections


@prefect.task(
    task_run_name="{file_name}",
)
def _transform_netcdf_to_parquet(
    ds: xarray.Dataset,
    store_path: pathlib.Path,
    file_name: str,
) -> pyarrow.dataset.Dataset:

    store_path.mkdir(exist_ok=True)

    table = pyarrow.table(ds.to_dataframe().reset_index())
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
        partitioning=["file"],
        partitioning_flavor="hive",
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
    )


@prefect.task
def transform_netcdf(
    path: pathlib.Path,
    parquet_store_path: pathlib.Path = pathlib.Path("parquet"),
    zarr_store_path: pathlib.Path = pathlib.Path("zarr"),
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
    )


@prefect.flow(
    task_runner=prefect.task_runners.ProcessPoolTaskRunner(max_workers=16),
)
def transform(
    path: pathlib.Path = pathlib.Path("argo"),
):
    prefect.futures.wait([
        transform_netcdf.submit(path)
        for path in path.glob("*/*_prof.nc")
    ])

if __name__ == "__main__":
    transform()