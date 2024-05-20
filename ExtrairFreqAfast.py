import pandas as pd
import json
import datetime
import subprocess  
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from AuditoriaProcessos import AuditoriaProcessos

class ExtrairFreqAfast:
    try:       
        def extrair_freq_afast(self, session, banco_oriem, diretorio_arquivo_parquet, tab_freqAfast, tenant_id, id_exec, logger):
            
            retorno = "ERRO"
            data_atual = datetime.datetime.now()
            
            auditoria = AuditoriaProcessos()

            # Criando uma conexão com o banco de dados
            engine = create_engine(banco_oriem)

            # Executar a procedure
            print("    #### Inicio do processamento do ETL CARREGA_FREQ_AFAST." + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            auditoria.gera_log_detail(session, id_exec, "    #### Inicio do processamento dos dados de Frequência e Afastamentos. Início: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            logger.log_info("O processo de carga dos dados de frequÊncias e afastamentos foi iniciado")
            cursor = session.connection().connection.cursor()
            params = {
                'P_DATA': data_atual,
                'P_TENANT_ID': tenant_id
            }            
            cursor.callproc("CARREGA_FREQ_AFAST", keywordParameters=params)

            cursor.close()
            session.close()
            print("        #### Término do processamento do ETL CARREGA_FREQ_AFAST." + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            auditoria.gera_log_detail(session, id_exec, "       #### Término do processamento dos dados de Frequência e Afastamentos em: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            logger.log_info("O processo de carga dos dados de frequÊncias e afastamentos foi concluído")
            # Consulta SQL para selecionar os dados da tabela, esses dados estarão contidos no arquivo parquet.
            
            print("    #### Inicio da geração do arquivo "+ tab_freqAfast +".parquet | Inicio do processamento: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            auditoria.gera_log_detail(session, id_exec, "    #### Inicio da geração do arquivo "+ tab_freqAfast +".parquet | Processamento concluído em: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            logger.log_info("Inicio da geração do arquivo "+ tab_freqAfast +".parquet")
            condicao = f"tenantid = {tenant_id}"
            rows = f"SELECT * FROM {tab_freqAfast} WHERE {condicao}"

            # Pandas para ler os dados do banco de dados e transferi-los para um DataFrame
            df = pd.read_sql_query(rows, engine)

            # Salva o DataFrame como um arquivo Parquet
            df.to_parquet(diretorio_arquivo_parquet, index=False)
            print("        #### Término da geração do arquivo "+ tab_freqAfast +".parquet | Processo concluído em: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            auditoria.gera_log_detail(session, id_exec, "        #### Término da geração do arquivo "+ tab_freqAfast +".parquet | Processo concluído em: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            logger.log_info("Término da geração do arquivo "+ tab_freqAfast +".parquet")

            retorno = "SUCESSO"
            return retorno
        
    except Exception as e:
        print("Ocorreu um erro no processamento da extração dos funcionários. Erro => :", e)
        
