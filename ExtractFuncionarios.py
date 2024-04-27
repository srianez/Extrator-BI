import pandas as pd
import json
import datetime
import subprocess  
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

try:
    print("#### Iniciando a leitura do arquivo de configuracoes.")

    # Carregando as configurações do arquivo JSON
    with open('configs.json') as config_file:
        config = json.load(config_file)
   
    # Obtendo informações do Json de configuração
    database_url = config['databaseVendor']['origin']
    tab_func = config['tables']['func']
    parquet_file_path = config['files']['fileFunc']

    print("#### Inicio da geração do arquivo ",tab_func,".parquet | ", "Inicio do processamento: ", datetime.datetime.now() )

    # Obter a data atual
    data_atual = datetime.datetime.now()

    # Criando uma conexão com o banco de dados Oracle
    print("#### Criando uma conexão com o banco de dados Oracle.")
    engine = create_engine(database_url)

    # Criar uma sessão para execução da procedure
    #Session = sessionmaker(bind=engine)
    #session = Session()
    print("    #### Conexão estabelecida.")

    # Executar a procedure
    print("#### Inicio do processamento do ETL CARREGA_FUNCIONARIO.", datetime.datetime.now())
    #cursor = session.connection().connection.cursor()
    #cursor.callproc("CARREGA_FUNCIONARIO", [data_atual])

    # Fechar o cursor e a sessão
    #cursor.close()
    #session.close()
    print("    #### Processamento do ETL concluido.")

    # Consulta SQL para selecionar os dados da tabela, esses dados estarão contidos no arquivo parquet.
    print("#### Gerando o arquivo ",tab_func,".parquet ", datetime.datetime.now())
    condicao = "tenantid = 1"
    #rows = f"SELECT * FROM {tab_func}"
    rows = f"SELECT * FROM {tab_func} WHERE {condicao}"

    # Pandas para ler os dados do banco de dados e transferi-los para um DataFrame
    df = pd.read_sql_query(rows, engine)

    # Salva o DataFrame como um arquivo Parquet
    df.to_parquet(parquet_file_path, index=False)
    print("    #### Termino da geracao do arquivo ",tab_func,".parquet ", datetime.datetime.now())

   
    # Executar o arquivo import.py
    subprocess.run(["python", "ImportFuncionarios.py"])

    print("Arquivo Parquet carregado com sucesso!")

except Exception as e:
    # Tratamento genérico para qualquer exceção não tratada
    print("Ocorreu um erro no processamento da extração dos funcionários. Erro => :", e)
