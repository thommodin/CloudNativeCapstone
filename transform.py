import prefect
import pystac
from catalog import get_catalog
import pathlib
import xarray
import polars
import pyarrow
import pyarrow.parquet
import pyarrow.dataset
import collections
import rich


@prefect.task
def get_items(
    catalog: pystac.Catalog = get_catalog(),
) -> list[pystac.Item]:
    collection = catalog.get_child("argo-csiro")
    return list(collection.get_items())


@prefect.task
def _transform_netcdf_to_parquet(
    ds: xarray.Dataset,
    store_path: pathlib.Path,
    file_name: str,
) -> None:
    
    store_path.mkdir(exist_ok=True)
    
    table = pyarrow.table(ds.to_dataframe().reset_index())
    table = table.append_column(
        "file",
        pyarrow.array(
            [file_name] * len(table),
            pyarrow.string(),
        ),
    )

    pyarrow.dataset.write_dataset(
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

    for k, v in dd.items():
        print(f"\n\nDIMENSIONS: {k}\n\n")
        print(ds[v])
    
    ds = ds.drop_dims({"N_PARAM", "N_HISTORY", "N_CALIB"})

    _transform_netcdf_to_parquet(
        ds=ds,
        store_path=parquet_store_path,
        file_name=path.name,
    )


@prefect.flow
def main():
    items = get_items()
    for item in items:
        transform_netcdf(pathlib.Path(item.assets["profile"].href))


if __name__ == "__main__":
    main()