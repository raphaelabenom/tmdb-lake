#Imports

import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()  #Carrega as variáveis de ambiente do arquivo .env

azure_key = os.getenv('AZURE_KEY')  #Obtendo o azure_key

service_client = BlobServiceClient.from_connection_string(azure_key)


# Criando um container

container = 'newcontainer'
container_client = service_client.create_container(container)

# Subindo os arquivos

folder = './data/'

for file_name in os.listdir(folder):
    blob_obj = service_client.get_blob_client(container='newcontainer', blob=file_name)
    print(f'Uploading: {file_name}...')
    
    with open(os.path.join(folder, file_name), mode='r+') as file_data:
        blob_obj.upload_blob(file_data)