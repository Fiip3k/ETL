import os
from config import AzureConfig
from azure.storage.filedatalake import DataLakeServiceClient

class LakeClient:

    serviceClient = None

    def __init__(self, lakeName: str):
        try:
            self.serviceClient = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", lakeName), credential=AzureConfig["key"])
        except Exception as e:
            print(e)



    def uploadFile(self, filePath: str, directory: str, serverFileName: str = None, overwrite: bool = True):
        fileName = os.path.basename(filePath)
        if not serverFileName:
            serverFileName = fileName
        try:
            fileSystemClient = self.serviceClient.get_file_system_client(file_system=directory)
            fileClient = fileSystemClient.get_file_client(serverFileName)

            if fileClient.exists() and overwrite:
                print("File exists, replacing...")
            elif fileClient.exists():
                print("File exists, aborting...")
                return -1
            else:
                print("File doesn't exist, creating new file...")

            with open(filePath,'r') as localFile:
                fileContents = localFile.read()
                fileClient.upload_data(fileContents, overwrite=overwrite)
            print("File uploaded successfully.")
        except Exception as e:
            print(e)
            return -1
        return 0
    
    def downloadFile(self, directory: str, serverFileName: str, localFilePath: str):
        try:
            fileSystemClient = self.serviceClient.get_file_system_client(file_system=directory)
            fileClient = fileSystemClient.get_file_client(serverFileName)

            if fileClient.exists():
                with open(localFilePath, 'wb') as localFile:
                    fileContents = fileClient.download_file()
                    fileContents.readinto(localFile)
                print("File downloaded successfully.")
                return 0
            else:
                print("File does not exist in the specified directory.")
                return -1
        except Exception as e:
            print(e)
            return -1

