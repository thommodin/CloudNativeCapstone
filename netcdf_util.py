import xarray
import prefect
import pathlib
import collections
import rich
import json


@prefect.task
def get_netcdf_dimension_groups(
    ds: xarray.Dataset,
):
    


@prefect.task(
    task_run_name="{path}"
)
def get_netcdf_structure(path: pathlib.Path):

    ds = xarray.open_dataset(path)
    structure = collections.defaultdict(list)
    for variable_name, variable_data in ds.variables.items():
        dims = variable_data.dims
        structure[dims].append(variable_name)
    return structure


@prefect.flow(
    flow_run_name="{path}"
)
def get_netcdf_structure_files(path: pathlib.Path):
    structure_files = collections.defaultdict(list)
    for path in path.glob("**/*.nc"):
        structure = get_netcdf_structure(path)
        structure_files[str(structure)].append(str(path))
    return structure_files

if __name__ == "__main__":

    structure = get_netcdf_structure(pathlib.Path("argo/7900948/7900948_prof.nc"))
    rich.print(structure)
