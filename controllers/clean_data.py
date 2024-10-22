from pyspark.sql.functions import col, trim, regexp_replace
from pyspark.sql.dataframe import DataFrame

def remove_more_two_spaces(df:DataFrame, column:str) -> DataFrame:
    '''
        Remove mais de dois espaços entre as palavras
    '''
    df = df.withColumn(column, regexp_replace(col(column), r"\s+", " "))    
    return df


def remove_spaces_start_end(df:DataFrame, column:str) -> DataFrame:
    '''
        Remove espaços no inicio e no final de um texto
    '''
    df = df.withColumn(column, trim(col(column)))    

    return df


def remove_null_values(df:DataFrame) -> DataFrame:
    '''
        Remove valores nulos do DataFrame
    '''
    df = df.na.drop()
    return df


def filter_values_higher_or_equal(df:DataFrame, column:str, value:float) -> DataFrame:
    '''
        Filtra valores maiores ou iguais à 
    '''
    df = df.filter(df[column] >= value)
    return df
