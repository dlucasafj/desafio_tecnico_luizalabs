import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg,round,trim,btrim, regexp_replace
import json

# '''
#     Obtem os dados da API 'https://fakestoreapi.com/products'
# '''
# url = 'https://fakestoreapi.com/products'
# response = requests.get(url) 

# dados = []

# # Transforma os dados da resposta em uma lista de dicionários
# dados = response.json() if  response.status_code == 200 else print(f'Erro ao obter os dados da API: Erro {response.status_code}')

# '''
#     Inicializa uma Sessão do Spark
# '''
# spark = SparkSession.builder \
#     .appName("Leitura API com PySpark") \
#     .config("spark.jars", "./database/postgresql-42.7.4.jar") \
#     .getOrCreate()

# Converte os dados da API para DataFrame
# df = spark.read.json(spark.sparkContext.parallelize([json.dumps(dados)]))


# # Limpeza e Transformação 

# # Trata os dados da coluna rating
# df_trated = df.withColumn("rating_count", col("rating.count")) \
#               .withColumn("rating_rate", col("rating.rate")) \
#               .drop("rating")
# df_trated = df_trated.withColumn("title",regexp_replace(col("title"), r"^[\s\u200B-\u200D\uFEFF]+|[\s\u200B-\u200D\uFEFF]+$", ""))
# df_trated.show()

# title = df_trated.filter(df.id == 3)

# title.show()
# remove valores nulos e inconsistentes 



# # Filtros
# print('Preco >=100.00(price)')
# df_trated.filter(df_trated.price >= 100).show()
# print('Avaliação >= 3.5(rate)')
# df_trated.filter(df_trated.rating_rate >= 3.5).show()


# # Dados Sumarizados
# df_sumarized = df_trated.groupBy("category").agg(
#                 round(avg('price'),3).alias('preco_medio'),
#                 round(avg('rating_rate'),3).alias('avaliacao_media')
#             ).withColumnRenamed('category', 'categoria')

# df_sumarized.show()






'''
    Filtra valores maiores ou iguais à 
'''
def filter_values_higher_or_equal(df, column, value):
    df = df.filter(df[column] >= value)
    return df

