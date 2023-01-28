import os
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient, PathProperties
from typing import Iterable, List, Callable

class AzureDatalakeService():
    _datalake_service_client: DataLakeServiceClient
    _file_system_clients: dict
    _default_container: str

    def __init__(self, account_url: str, container: str, az_sas: str ) -> None:
        self._datalake_service_client = DataLakeServiceClient(account_url, az_sas)
        #self._file_system_client = self._datalake_service_client.get_file_system_client(container)
        self._default_container = container
        self._file_system_clients = {}
        self._file_system_clients[container] = self._datalake_service_client.get_file_system_client(self._default_container)

    def _add_file_system_clients(self, containers: list):
        for container in containers:
            self._add_file_system_client(container)

    def _add_file_system_client(self, container: str):
        if not self._file_system_clients.get(container):
            self._file_system_clients[container] = self._datalake_service_client.get_file_system_client(container)


    def _get_file_system_client(self, container: str = None) -> FileSystemClient:
        if container is None:
            return  self._file_system_clients[self._default_container]
        else:
            self._add_file_system_client(container)
            return self._file_system_clients[ container]

    def _get_file_client(self, az_folder, file_name, container: str = None):
        directory_client = self._get_file_system_client(container).get_directory_client(az_folder)
        az_file_client = directory_client.get_file_client(file_name)
        return az_file_client

    def _get_directory_client(self, az_folder, container: str = None):
        return self._get_file_system_client(container).get_directory_client(az_folder)

    def az_create_folder(self, az_folder: str, container: str = None):
        self._get_file_system_client(container).create_directory(az_folder)

    def az_create_folder_if_not_exists(self, az_folder: str, container: str = None):
        if not self.az_check_if_folder_exists(az_folder, container):
            self._get_file_system_client(container).create_directory(az_folder, container)

    def az_check_if_folder_exists(self, az_folder: str, container: str = None) -> bool:
        directory_client = self._get_file_system_client(container).get_directory_client(az_folder)
        return directory_client.exists()

    def az_check_if_file_exists(self, az_folder: str, file_name: str, container: str = None) -> bool:
        file_client = self._get_file_client(az_folder, file_name, container)
        return file_client.exists()

    def az_get_files_with_properties(self, az_folder_path: str, container: str = None, filter_end = None) -> Iterable[PathProperties]:
        az_paths: Iterable[PathProperties] = self._get_file_system_client(container).get_paths(path = az_folder_path)
        
        az_files  = [ az_path for az_path in az_paths if not az_path.is_directory and  (not filter_end or  az_path.name.endswith(filter_end) )   ]
        return az_files

    def az_get_files_property(self, az_folder_path: str, property, container: str = None, filter_end = None) -> Iterable:
        files_with_property  = self.az_get_files_with_properties(az_folder_path, container, filter_end)
        return [ file_property.get(property) for file_property in files_with_property ]

    def az_get_files_property(self, az_folder_path: str, property, container: str = None, filter_end = None) -> Iterable:
        files_with_property  = self.az_get_files_with_properties(az_folder_path, container, filter_end)
        return [ ( file_property.name, file_property.get(property)) for file_property in files_with_property ]

    def az_get_total_size(self, az_folder_path: str, container: str = None, filter_end = None) -> Iterable:
        files_with_property  = self.az_get_files_with_properties(az_folder_path, container, filter_end)
        return sum([ file_property.content_length for file_property in files_with_property ])
