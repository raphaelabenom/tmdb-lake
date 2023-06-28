import os
import argparse
import requests
import datetime
import json
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()  #Carrega as variáveis de ambiente do arquivo .env

token = os.getenv('API_KEY')  #Obtendo o API_KEY
azure_key = os.getenv('AZURE_KEY')  #Obtendo o API_KEY

class Ingestor:

    def __init__(self, token, delay) -> None:
        self.headers = {
            'Authorization': f'Bearer {token}',
            'accept': 'application/json',
        }
        self.base_url = "https://api.themoviedb.org/3/discover/{sufix}"
        self.delay = delay

    def get_data(self, page, sufix):

        # Data atual menos a quantidades de dias retroativas que quero buscar
        date_gte = (datetime.datetime.now() - datetime.timedelta(days=self.delay)).strftime('%Y-%m-%d')
        date_lte = datetime.datetime.now().strftime('%Y-%m-%d')

        # Parâmetros da consulta
        params = {
            'language': 'pt-BR', # Tradução
            'sort_by': '', # popularity.desc
            'include_adult': 'false',
            'include_video': 'false',
            'page': page,
            'with_original_language': 'en', # Linguagem original do filme
            'primary_release_date.gte': date_gte,
            'primary_release_date.lte': date_lte,
            'air_date.gte':date_gte,
            'air_date.lte':date_lte,
        }

        url = self.base_url.format(sufix=sufix) # Sufix movie ou tv
        response = requests.get(url, headers=self.headers, params=params)
        data = response.json()
        return data

    # Salvando os dados com a data e hora da consulta mais o número da página
    def save_data(self, data, page, sufix):

        if not os.path.exists(f"raw/{sufix}"):
            os.makedirs(f"raw/{sufix}")

        name = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        filename = f"raw/{sufix}/{name}_{page}.json"

        with open(filename, 'w', encoding='utf8') as open_file:
            json.dump(data, open_file, ensure_ascii=False)
        return True
    
    
    def get_and_save(self, page, sufix):
        data = self.get_data(page, sufix)
        self.save_data(data, page, sufix)
        return data
    
    def process(self, sufix):

        print("Iniciando loop...")
        page = 1

        while True:
            print(f"Obtendo dados da página {page}...")
            data = self.get_data(page, sufix)

            if data['total_pages'] < page:
                print("Finalizando loop...")
                break

            self.save_data(data, page, sufix)

            print(f"Dados da página {page} salvos com sucesso.")
            page += 1

    def azure_ingest(self, azure_key, directory_path, container_name):
        service_client = BlobServiceClient.from_connection_string(azure_key)

        # Checa se o container existe, se não existir, criar
        container_client = service_client.get_container_client(container_name)
        if not container_client.exists():
            service_client.create_container(container_name)

        # Subindo os arquivos para azure
        for file_name in os.listdir(directory_path):
            blob_client = service_client.get_blob_client(container=container_name, blob=file_name)
            print(f'Uploading: {file_name}...')

            with open(os.path.join(directory_path, file_name), mode='r') as file_data:
                blob_client.upload_blob(file_data.read().encode())

            # Deletar o arquivo após upload
            os.remove(os.path.join(directory_path, file_name))
            print(f'File {file_name} deleted')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestor TMDB")

    # add argument
    parser.add_argument('--delay', type=int, default=1, help='Dias retroativos para buscar da data de lançamento - release_date')
    parser.add_argument('--sufix', type=str, default='movie', help='Tipo de conteúdo a ser consultado - movie ou tv - para TV incluir first_air_date para funcionar a data retroativa nos parâmetros')
    parser.add_argument('--container', type=str, default='data', help='Nome do container do Azure Blob Storage para fazer o upload dos arquivos')
    parser.add_argument('--local', default=True, action='store_false', help='Flag para determinar se os arquivos devem ser enviados para o Azure Blob Storage ou apenas armazenados localmente, para deixar locamente use a flag, se deixar sem a flag ele ira subir automaticamente')

    # args
    args = parser.parse_args()

    # Chamada das funções
    ingestor = Ingestor(token, args.delay)
    ingestor.process(args.sufix)
    if args.local:
        ingestor.azure_ingest(azure_key, f"raw/{args.sufix}", args.container)

