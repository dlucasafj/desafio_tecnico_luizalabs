# Desafio Técnico LuizaLabs

Esse desafio tem como objetivo ler os dados de uma api e salvar em um banco de dados <b>Postgres</b> utilizando o <b>Pyspark</b>

## Pré-requisitos
- Python 3.10
- Postgres
  

### Instalação
1. Clone este repositório
   ```
   $ git clone
   ```
2. Renomeie o arquivo .env.example para .env e preencha as informações.
3. Instale o virtual-env
   ```
     $ pip install virtualenv
   ```
4. Crie um ambiente virtual
   ```shell
   $ python -m virtualenv venv
   ```
5. Ative o ambiente virtual:

   - Linux:
      ```shell
      $ source venv/bin/activate
      ```
6. Instale as dependências:
   ```shell
   $ pip install -r requirements.txt
   ```
### Execução 

Execute o arquivo com o seguinte comando no seu terminal:
```shell
  spark-submit --jars ./database_drive/postgresql-42.7.4.jar main.py
```
