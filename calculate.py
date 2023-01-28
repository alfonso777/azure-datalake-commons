from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

class AzureDatalakeMetadata():
    _account_url: str
    _default_container: str
    _sas: str
    _blob_service_client: BlobServiceClient

    def __init__(self, account_url: str, container: str, sas: str):
        self._account_url = account_url
        self._default_container = container
        self._sas = sas
        self._blob_service_client =  BlobServiceClient(account_url=self._account_url, credential=self._sas) 

    def exists(self, relative_file_path:str, container = None) -> bool:
        if not self._blob_service_client:
            self._blob_service_client = BlobServiceClient(account_url=self._account_url, credential=self._sas) 
        
        blob_client = self._blob_service_client.get_blob_client(container=self._get_container(container), blob=relative_file_path)
        return blob_client.exists()
 
    def _get_container(self, container: str = None):
        return container if container else self._default_container

    def _get_container_client(self, container: str = None):
        return self._blob_service_client.get_container_client(self._get_container(container))

    def list_blob_names(self, folder_path, container: str  = None):
        blob_names = self._get_container_client(container).list_blob_names(name_starts_with = folder_path)
        return blob_names
    
    def get_blob_size(self, relative_file_path: str, container: str = None)-> str:
        try:
            blob_client = self._blob_service_client.get_blob_client(container=self._get_container(container), blob=relative_file_path)
            return blob_client.get_blob_properties().size
            #return blob_client.
        except ResourceNotFoundError as ex:
            return None

    
    def get_size(self, blob_path: str, container: str = None)-> str:
        try:
            #blob_client = self._blob_service_client.get_blob_client(container=self._get_container(container), blob=folder_path)
            blob_names = self.list_blob_names(blob_path, container)
            size_total = 0
            for blob_path in blob_names:
                size_total = size_total + self.get_blob_size(blob_path, container)
            return size_total
        except ResourceNotFoundError as ex:
            return None

