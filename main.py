import prefect
from catalog import catalog
from transform import transform
from partition import partition
from cloud import benchmark_cloud_native

@prefect.flow
def main():
    # extract()
    catalog()
    transform()
    partition()
    benchmark_cloud_native()


if __name__ == "__main__":
    main()