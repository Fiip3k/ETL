#!source venv/bin/activate
import os
from config import azureConfig
from azure.storage.filedatalake import DataLakeServiceClient

def initialize_storage_account_ad(storage_account_name, storage_account_key):
    try:
        global service_client
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    except Exception as e:
        print(e)

def create_file_system(name: str):
    try:
        global file_system_client
        file_system_client = service_client.create_file_system(file_system=name)
    except Exception as e:
        print(e)

def upload_file_to_directory(path: str, directory: str, serverFileName: str = None):
    fileName = os.path.basename(path)
    if not serverFileName:
        serverFileName = fileName
    try:
        file_system_client = service_client.get_file_system_client(file_system="newdata")
        directory_client = file_system_client.get_directory_client(directory)
        file_client = directory_client.get_file_client(serverFileName)

        if file_client.exists():
            print("File exists, replacing...")
        else:
            print("File doesn't exist, creating new file...")

        local_file = open(path,'r')
        file_contents = local_file.read()
        file_client.upload_data(file_contents, overwrite=True)
    except Exception as e:
        print(e)
        return -1
    return 0

if __name__ == "__main__":

    storage_account_name = 'fiip3kdatalake'
    storage_account_key = azureConfig["key"]
    
    initialize_storage_account_ad(storage_account_name, storage_account_key)
    result = upload_file_to_directory("test.csv", "test")
    if result == 0:
        print("Success")