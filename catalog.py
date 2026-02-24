import pystac
import pathlib
import prefect
import prefect.task_runners
import prefect.futures
import xarray
import shapely
import datetime

@prefect.task(
    task_run_name="get-catalog-{path}",
)
def get_catalog(
    path: pathlib.Path = pathlib.Path("catalog/catalog.json"),
    id: str = "aodn",
    description: str = "a mock AODN STAC catalog",
) -> pystac.Catalog:
    logger = prefect.get_run_logger()
    if not path.is_file():
        logger.info(f"did not find catalog @ `{path}`, creating new...")
        catalog = pystac.Catalog(
            id=id,
            description=description,
            href=path,
        )
    else:
        logger.info(f"found catalog @ `{path}`")
        catalog = pystac.Catalog.from_file(path)
    return catalog


@prefect.task(
    task_run_name="{path}",
)
def create_argo_item(
    path: pathlib.Path,
) -> pystac.Item | None:
    
    ds = xarray.open_dataset(path)

    logger = prefect.get_run_logger()

    juld_min = ds["JULD"].min().values.astype("datetime64[s]").item()
    juld_max = ds["JULD"].max().values.astype("datetime64[s]").item()
    longitude_min = ds["LONGITUDE"].min().item()
    longitude_max = ds["LONGITUDE"].max().item()
    latitude_min = ds["LATITUDE"].min().item()
    latitude_max = ds["LATITUDE"].max().item()

    try:
        geometry = shapely.Polygon(
            shell=[
                [longitude_min, latitude_min],
                [longitude_max, latitude_min],
                [longitude_max, latitude_max],
                [longitude_min, latitude_max],
                [longitude_min, latitude_min],
            ],
        )
    except:
        logger.info(ds)
        logger.info(longitude_min)        
        logger.info(longitude_max)
        logger.info(latitude_min)
        logger.info(latitude_max)
        logger.error(f"`{path}` failed to generate a geometry")
        raise

    asset = pystac.Asset(
        href=path.absolute(),
        title=path.with_suffix("").name,
        description="argo netcdf",
        media_type="application/netcdf",
    )
    item = pystac.Item(
        id=f"csiro/{path}".replace("/", "-"),
        assets={"profile": asset},
        datetime=juld_min,
        start_datetime=juld_min,
        end_datetime=juld_max,
        properties={},
        geometry=geometry.__geo_interface__,
        bbox=geometry.bounds,
    )
    return item

@prefect.flow(
    flow_run_name="{path}",
    task_runner=prefect.task_runners.ThreadPoolTaskRunner(max_workers=8),
)
def create_argo_collection(
    path: pathlib.Path = pathlib.Path("argo"),
) -> pystac.Collection:
    
    futures = [
        create_argo_item.submit(path)
        for path in list(path.glob("*/*_prof.nc"))
    ]

    items = []
    for future in prefect.futures.as_completed(futures):
        if future.state.is_completed():
            items.append(future.result())

    bboxes = [item.bbox for item in items]
    intervals = [
        (
            datetime.datetime.strptime(
                item.properties["start_datetime"],
                "%Y-%m-%dT%H:%M:%SZ",
            ),
            datetime.datetime.strptime(
                item.properties["end_datetime"],
                "%Y-%m-%dT%H:%M:%SZ",
            ),
        )
        for item in items
    ]

    collection = pystac.Collection(
        id="argo-csiro",
        description="a mock ARGO float collection",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent(bboxes),
            temporal=pystac.TemporalExtent(intervals)
        ),
    )
    collection.add_items(items)
    return collection


@prefect.flow
def main():
    
    # Get the catalog
    catalog = get_catalog()

    # Add the collection if it has not been added
    if catalog.get_child("argo-csiro") is None:
        collection = create_argo_collection()
        catalog.add_child(collection)
        catalog.normalize_hrefs("./catalog")
        catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

    # Get the collection
    collection = catalog.get_child("argo-csiro")


if __name__ == "__main__":
    main()