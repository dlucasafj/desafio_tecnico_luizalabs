import os

def get_settings() -> dict:
    '''
        Retorna as propriedades para o spark se conectar com o Banco de dados
    '''
    data = {
        'url':f"jdbc:postgresql://{os.getenv('DB_HOSTNAME')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}",
        'properties':{
            "user": os.getenv('DB_USER'),
            "password": os.getenv('DB_PASSWORD'),
            "driver": "org.postgresql.Driver"
        }
    }
    return data
