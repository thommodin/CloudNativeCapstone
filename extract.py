import sh
import sys
import prefect


@prefect.flow
def extract():
    """Extract all csiro profiles to the folder `argo`"""

    sh.aws(
        "s3",
        "sync",
        "s3://imos-data/IMOS/Argo/dac/",
        "argo",
        "--no-sign-request",
        "--exclude",
        "*",
        "--include",
        "*_prof.nc",
        _out=sys.stdout,
        _err=sys.stderr,
    )


if __name__ == "__main__":
    extract()
