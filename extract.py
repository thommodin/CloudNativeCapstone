import sh
import sys
import prefect
import pathlib

@prefect.task
def extract():
    """Extract all csiro profiles to the folder `argo`"""

    path = pathlib.Path("argo")

    if not path.is_dir():

        pathlib.Path()
        sh.aws(
            "s3",
            "sync",
            "s3://imos-data/IMOS/Argo/dac/csiro/",
            "argo",
            "--no-sign-request",
            "--exclude",
            "*",
            "--include",
            "*_prof.nc",
            _out=sys.stdout,
            _err=sys.stderr,
        )
