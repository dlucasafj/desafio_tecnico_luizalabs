
import sys
from dotenv import load_dotenv
from controllers.api import get_data
from controllers.clean_data import remove_more_two_spaces, remove_spaces_start_end, remove_null_values, filter_values_higher_or_equal
from controllers.settings_db import get_settings
from controllers.save_db import persists_database
from controllers.spark import CreateSparkSession
load_dotenv()

def main() -> int:

    print("*** ETAPA 01 ***")
    data = get_data() # Obtém os dados da API

    
    print("*** ETAPA 02 *** \n")
    
    session_spark = CreateSparkSession('Desafio Técnico') # Cria uma sessão Spark
    df = session_spark.createDataFrame(data) # Cria um dataFrame

    print('** Data Frame obtido dos dados da API **')

    df.show()

    #Trata o DataFrame criado
    df_trated = session_spark.trated_data(df)

    df_trated = remove_more_two_spaces(df_trated, 'title')
    df_trated = remove_spaces_start_end(df_trated, 'title')
    df_trated = remove_spaces_start_end(df_trated, 'description')

    df_trated = remove_null_values(df_trated)

   
    print('** Data Frame Tratado **')

    df_trated.show()
   
    #Filtros
    print('** Produtos com preço maior ou igual a 100.00 **')

    df_price = filter_values_higher_or_equal(df_trated, 'price', 100.00)
    df_price.show()
   
    print('** Produtos com avaliação maior ou igual a 3.5 **')
    df_rate = filter_values_higher_or_equal(df_trated, 'rating_rate', 3.5)
    df_rate.show()
   
    print('\n')


    #Cria os dados sumarizados
    print('** Dados Sumarizados **')

    df_sumarized = session_spark.createSumaryDataFrame(df_trated)
    df_sumarized.show()


    print("*** ETAPA 03 e 04 \n")
    print("*** Dividindo os dados e Salvando no Banco***")
    data = get_settings()
    persists_database(df_trated, data)

    return 0

if __name__ == '__main__':
    sys.exit(main())  