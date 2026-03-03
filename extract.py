import sh
import sys
import prefect
import pathlib


@prefect.flow
def extract(
    s3_source: str = "s3://imos-data/IMOS/Argo/dac/",
    local_path: pathlib.Path = pathlib.Path("argo"),
    include_glob: str = "*_prof.nc",
):
    """Extract Argo profiles from an S3 bucket to a local directory."""

    sh.aws(
        "s3",
        "sync",
        s3_source,
        str(local_path),
        "--no-sign-request",
        "--exclude",
        "*",
        "--include",
        include_glob,
        _out=sys.stdout,
        _err=sys.stderr,
    )


if __name__ == "__main__":
    extract()
