import sh
import sys

# Extract all csiro profiles to the folder "argo"
sh.aws(
    "s3", "sync", 
    "s3://imos-data/IMOS/Argo/dac/csiro/", "argo", 
    "--no-sign-request", 
    "--exclude", "*",
    "--include", "*_prof.nc",
    _out=sys.stdout,
    _err=sys.stderr,
)