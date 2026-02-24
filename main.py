import prefect
from extract import extract
from catalog import catalog
from transform import transform
from partition import partition

@prefect.flow
def main():
    # extract()
    catalog()
    transform()
    partition()

main()