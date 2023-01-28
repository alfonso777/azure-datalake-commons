from azdatalakecommons.utils import load_config
from azdatalakecommons.calculate import AzureBlobService 

if __name__ == '__main__':
    conf = load_config(path_file="conf/app_test.conf", use_row_parser=True)

    metadata_service = AzureBlobService(conf["datalake"]['blob_url'] , "test1", conf["datalake"]["sas"])
    size = metadata_service.get_blob_size("temp/9838.oda", 'test1')
    print(f"Size of temp/9838.oda: {size}")

    total_size = metadata_service.get_size("temp", 'test1')
    print(f"Size of temp folder: {total_size}")
