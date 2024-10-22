from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

spark = SparkSession.builder.appName("SplitDataFrame").getOrCreate()

def persists_database(df:DataFrame, data:dict):
    """
        Divide o DataFrame em partes com 5 dados cada e salva cada parte no banco de dados. 
        Se durante a escrita a conexão com o banco de dados for interrompida é lançada uma exceção. 
    """
    
    # Coletando os dados em uma lista
    data_list = df.collect()

    # Dividindo a lista em partes menores de 5
    chunk_size = 5
    chunks = [data_list[i:i + chunk_size] for i in range(0, len(data_list), chunk_size)]

    # Convertendo os pedaços de volta para DataFrames do PySpark
    dfs = [spark.createDataFrame(chunk) for chunk in chunks]

    # Salvando as partes divididas no Banco de Dados
    for idx, chunk_df in enumerate(dfs):
        try:                    
            chunk_df.write.jdbc(url=data['url'], table="categoria_media", mode='append', properties=data['properties'])
            
            print(f"Parte {idx + 1} inserido com sucesso.")
        except Exception as e:
            print(f"Erro inesperado: {e}")