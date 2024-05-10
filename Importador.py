import pandas as pd
import json
import sqlalchemy
import datetime
from sqlalchemy import create_engine, inspect, MetaData, Table, delete
from AuditoriaProcessos import AuditoriaProcessos

class Importador:
    try:
        def importar_arquivo(self, banco_destino, diretorio_arquivo_parquet, tab, tenant_id, session, id_exec):
            
            auditoria = AuditoriaProcessos()

            # Carregar os dados do arquivo Parquet em um DataFrame Pandas
            print("    #### Inicio da importacao do arquivo "+ tab + ".parquet | Inicio do processamento: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            auditoria.gera_log_detail(session, id_exec, "    #### Inicio da importacao do arquivo "+ tab + ".parquet | Inicio do processamento: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            df_parquet = pd.read_parquet(diretorio_arquivo_parquet)

            #definindo a conexão que será criada conforme o banco de dados
            engine = sqlalchemy.create_engine(banco_destino)

            # Criando um objeto MetaData. Metadata contém as especificações das tabelas, não os dados contidos nelas.
            metadata = MetaData()
            metadata.reflect(bind=engine)

            # Cria um objeto de tabela no SQLAlchemy que representa a estrutura da tabela existente no banco
            tabela = Table(tab, metadata, autoload=True, autoload_with=engine)

            # deleta os registros existentes de um determinado inquilino
            auditoria.gera_log_detail(session, id_exec, "    #### Removendo dados da tabela "+ tab + " | Inicio do processamento: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            stmt = delete(tabela).where(tabela.c.tenantid == tenant_id)
            with engine.connect() as conn:
                conn.execute(stmt)
                conn.commit()
            conn.close()   
            
            auditoria.gera_log_detail(session, id_exec, "        #### Remoção concluída em: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            commit_frequency = 1000
            iteration_count = 0
            # Crie uma nova conexão com o engine para execução do insert
            conn_insert = engine.connect()

            auditoria.gera_log_detail(session, id_exec, "    #### Carregando os dados da tabela "+ tab + ".parquet | Inicio do processamento: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            for index, row in df_parquet.iterrows():
                conn_insert.execute(tabela.insert().values(**row))
                iteration_count += 1
                if iteration_count % commit_frequency == 0:
                    conn_insert.commit()                

            conn_insert.commit()
            conn_insert.close()
            
            auditoria.gera_log_detail(session, id_exec, "        #### Término da importacao do arquivo " + tab + ".parquet em: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            print("        #### Término da importacao do arquivo " + tab + ".parquet em: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            return "SUCESSO"
            

    except Exception as e:
        # Tratamento genérico para qualquer exceção não tratada
        print("Erro na execução do arquivo Importador. Erro => ", e)