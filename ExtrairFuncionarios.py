import pandas as pd
import json
import datetime
import subprocess  
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from AuditoriaProcessos import AuditoriaProcessos


class ExtrairFuncionarios:
    try:       
        def extrair_funcionarios(self, session, banco_oriem, diretorio_arquivo_parquet, tab_func, tenant_id, id_exec):
            retorno = "ERRO"
            auditoria = AuditoriaProcessos()
            
            data_atual = datetime.datetime.now()

            # Criando uma conexão com o banco de dados
            #print("#### Criando uma conexão com o banco de dados.")
            engine = create_engine(banco_oriem)

            # Criar uma sessão para execução da procedure
            #Session = sessionmaker(bind=engine)
            #session = Session()
            #print("    #### Conexão estabelecida.")

            # Executar a procedure
            cursor = session.connection().connection.cursor()
            params = {
                'P_DATA': data_atual,
                'P_TENANT_ID': tenant_id
            }
                      
            auditoria.gera_log_detail(session, id_exec, "    #### Processo de carga dos dados dos funcionáios foi iniciado em: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            cursor.callproc("CARREGA_FUNCIONARIO", keywordParameters=params)
            
            cursor.close()
            session.close()

            auditoria.gera_log_detail(session, id_exec, "        #### Processo de carga dos dados dos funcionáios foi concluído em: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            # Consulta SQL para selecionar os dados da tabela, esses dados estarão contidos no arquivo parquet.
            print("    #### Inicio da geração do arquivo "+ tab_func +".parquet | Inicio do processamento: ", datetime.datetime.now() )
            auditoria.gera_log_detail(session, id_exec, "    #### Inicio da geração do arquivo "+ tab_func +".parquet | Inicio do processamento: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            condicao = f"tenantid = {tenant_id}"
            rows = f"SELECT * FROM {tab_func} WHERE {condicao}"

            # Pandas para ler os dados do banco de dados e transferi-los para um DataFrame
            df = pd.read_sql_query(rows, engine)

            # Salva o DataFrame como um arquivo Parquet
            df.to_parquet(diretorio_arquivo_parquet, index=False)
            print("        #### Término da geração do arquivo "+ tab_func +".parquet | Inicio do processamento: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            auditoria.gera_log_detail(session, id_exec, "        #### Término da geração do arquivo "+ tab_func +".parquet | Inicio do processamento: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
            retorno = "SUCESSO"
            return retorno
    
    except Exception as e:
        print("Ocorreu um erro no processamento da extração dos funcionários. Erro => :", e)

        
