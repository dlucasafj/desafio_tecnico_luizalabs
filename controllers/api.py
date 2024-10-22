import requests
import os

def get_data() -> list:
    '''
        Obtem os dados da API https://fakestoreapi.com/products
    '''
    response = requests.get(os.getenv('API_URL')) 
    data = []
    
    # Transforma os dados da resposta em uma lista de dicionários
    data = response.json() if  response.status_code == 200 else print(f'Erro {response.status_code}')

    return data