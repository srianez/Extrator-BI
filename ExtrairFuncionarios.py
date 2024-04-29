import pandas as pd
import json
import datetime
import subprocess  
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class ExtrairFuncionarios:
    try:       
        def extrair_funcionarios(self, banco_oriem, diretorio_arquivo_parquet, tab_func, tenant_id):
            retorno = "ERRO"
            # Obter a data atual
            data_atual = datetime.datetime.now()

            # Criando uma conexão com o banco de dados Oracle
            print("#### Criando uma conexão com o banco de dados Oracle.")
            engine = create_engine(banco_oriem)

            # Criar uma sessão para execução da procedure
            Session = sessionmaker(bind=engine)
            session = Session()
            print("    #### Conexão estabelecida.")

            # Executar a procedure
            print("#### Inicio do processamento do ETL CARREGA_FUNCIONARIO.", datetime.datetime.now())
            cursor = session.connection().connection.cursor()
            params = {
                'P_DATA': data_atual,
                'P_TENANT_ID': tenant_id
            }            
            cursor.callproc("CARREGA_FUNCIONARIO", keywordParameters=params)

            # Fechar o cursor e a sessão
            cursor.close()
            session.close()
            print("    #### Término do processamento do ETL CARREGA_FUNCIONARIO.", datetime.datetime.now())

            # Consulta SQL para selecionar os dados da tabela, esses dados estarão contidos no arquivo parquet.
            print("#### Inicio da geração do arquivo "+ tab_func +".parquet | Inicio do processamento: ", datetime.datetime.now() )
            condicao = f"tenantid = {tenant_id}"
            rows = f"SELECT * FROM {tab_func} WHERE {condicao}"

            # Pandas para ler os dados do banco de dados e transferi-los para um DataFrame
            df = pd.read_sql_query(rows, engine)

            # Salva o DataFrame como um arquivo Parquet
            df.to_parquet(diretorio_arquivo_parquet, index=False)
            print("    #### Término da geração do arquivo "+ tab_func +".parquet | Inicio do processamento: ", datetime.datetime.now() )
            
            retorno = "SUCESSO"
            return retorno
        
            # Executar o arquivo import.py
            #subprocess.run(["python", "ImportFuncionarios.py"])

    except Exception as e:
        print("Ocorreu um erro no processamento da extração dos funcionários. Erro => :", e)
        
