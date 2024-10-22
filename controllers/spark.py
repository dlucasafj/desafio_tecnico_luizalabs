from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round
from pyspark.sql.dataframe import DataFrame
import json

class CreateSparkSession:
    
    def __init__(self, appName:str):
        '''
            Cria uma sessão SPARK conectada ao banco de dados postgres
        '''
        self.spark = SparkSession.builder \
            .master("local")\
            .appName(appName) \
            .config("spark.jars", "./database_drive/postgresql-42.7.4.jar") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")  # Você pode usar "FATAL" para logs ainda mais silenciosos


    def createDataFrame(self, data:list) -> DataFrame:
        '''
            Cria um data frame dos dados informados
        '''

        df = self.spark.read.json(self.spark.sparkContext.parallelize([json.dumps(data)]))
        return df
    

    def trated_data(self, df:DataFrame) -> DataFrame:
        '''
            Separa os dados da coluna rating adicionando duas novas colunas (rating_count e rating_rate) no DataFrame e 
            removendo a coluna rating
        '''
        df_trated = df.withColumn("rating_count", col("rating.count")) \
            .withColumn("rating_rate", col("rating.rate")) \
            .drop("rating")
        return df_trated
    


    def createSumaryDataFrame(self, df:DataFrame) -> DataFrame:
        '''
            Cria um dataFrame sumarizado com categoria, preço e avaliação
        '''
        df_sumarized = df.groupBy("category").agg(round(avg('price'),3).alias('preco_medio'),
                round(avg('rating_rate'),3).alias('avaliacao_media')).withColumnRenamed('category', 'categoria')
        return df_sumarized

