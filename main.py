import prefect
from catalog import catalog
from transform import transform
from partition import partition


@prefect.flow
def main():
    # extract()
    catalog()
    transform()
    partition()


if __name__ == "__main__":
    main()