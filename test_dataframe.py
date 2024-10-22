import pytest
from pyspark.sql import SparkSession
from controllers.clean_data import remove_more_two_spaces, remove_spaces_start_end, remove_null_values,filter_values_higher_or_equal
from pyspark.testing.utils import assertDataFrameEqual

@pytest.fixture(scope="module")
def spark_fixture():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark
    spark.stop()

class TestDataFrame:
    
    def test_remove_spaces_start_end(self, spark_fixture):
        data = [{"capital":"   Salvador", "state": "Bahia"},
                {"capital":" São Paulo  ", "state": "São Paulo"}]
        
        df = spark_fixture.createDataFrame(data)
        transformed_df = remove_spaces_start_end(df, "capital")

        expected_data = [{"capital":"Salvador", "state": "Bahia"},
                {"capital":"São Paulo", "state": "São Paulo"}]
        expected_df = spark_fixture.createDataFrame(expected_data)

        assertDataFrameEqual(transformed_df, expected_df)
   
    def test_remove_more_two_spaces(self,spark_fixture):
        sample_data = [{"name": "John    D.", "age": 30},
                   {"name": "Alice   G.", "age": 25},
                   {"name": "Bob  T.", "age": 35},
                   {"name": "Eve   A.", "age": 28}]

        # Create a Spark DataFrame
        original_df = spark_fixture.createDataFrame(sample_data)

        transformed_df = remove_more_two_spaces(original_df, "name")
        expected_data = [{"name": "John D.", "age": 30},
            {"name": "Alice G.", "age": 25},
            {"name": "Bob T.", "age": 35},
            {"name": "Eve A.", "age": 28}]
        
        expected_df = spark_fixture.createDataFrame(expected_data)

        assertDataFrameEqual(transformed_df, expected_df)

    def test_remove_null_values(self, spark_fixture):
        data = [{"capital": None, "state": "Bahia"},
            {"capital":"São Paulo", "state": "São Paulo"},
            {"capital":"Rio de Janeiro", "state": None}]
        
        original_df = spark_fixture.createDataFrame(data)
        transformed_df = remove_null_values(original_df)

        expected_data = [{"capital":"São Paulo", "state": "São Paulo"}]

        expected_df = spark_fixture.createDataFrame(expected_data)
        assertDataFrameEqual(transformed_df, expected_df)

    def test_filter(self, spark_fixture):
        data = [
            {'name': 'Teste 1', 'age': 20, 'salary': 2500.00},
            {'name': 'Teste 2', 'age': 28, 'salary': 48450.00},
            {'name': 'Teste 3', 'age': 21, 'salary': 50.00},
            {'name': 'Teste 4', 'age': 19, 'salary': 40.00},
        ]

        original_df = spark_fixture.createDataFrame(data)
        filter_salary = filter_values_higher_or_equal(original_df, 'salary', 2500.00 )

        expected_salary = [
            {'name': 'Teste 1', 'age': 20, 'salary': 2500.00},
            {'name': 'Teste 2', 'age': 28, 'salary': 48450.00}
        ]

        expected_df_salary = spark_fixture.createDataFrame(expected_salary)     

        filter_age = filter_values_higher_or_equal(original_df, 'age', 22)

        expected_age = [
            {'name': 'Teste 2', 'age': 28, 'salary': 48450.00}
        ]

        expected_df_age = spark_fixture.createDataFrame(expected_age)


        assertDataFrameEqual(filter_salary, expected_df_salary)
        assertDataFrameEqual(filter_age, expected_df_age)