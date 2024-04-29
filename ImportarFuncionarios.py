import pandas as pd
import json
import sqlalchemy
import datetime
from sqlalchemy import create_engine, inspect, MetaData, Table, delete

class ImportarFuncionarios:
    try:
        def importar_funcionarios(self, banco_destino, diretorio_arquivo_parquet, tab_func, tenant_id):

            # Carregue os dados do arquivo Parquet em um DataFrame Pandas
            print("#### Inicio da importacao do arquivo ",tab_func,".parquet | Inicio do processamento: ", datetime.datetime.now() )
            df_parquet = pd.read_parquet(diretorio_arquivo_parquet)

            #define a conexão que será criada conforme o banco de dados
            engine = sqlalchemy.create_engine(banco_destino)

            # Crie um objeto MetaData. Metadata contém as especificações das tabelas, não os dados contidos nelas.
            metadata = MetaData()
            metadata.reflect(bind=engine)

            # Cria um objeto de tabela no SQLAlchemy que representa a estrutura da tabela existente no banco
            tabela = Table(tab_func, metadata, autoload=True, autoload_with=engine)

            # deleta os registros existentes
            stmt = delete(tabela).where(tabela.c.tenantid == {tenant_id})
            with engine.connect() as conn:
                conn.execute(stmt)
                conn.commit
            conn.close   

            # Crie uma nova conexão com o engine para execução do insert
            conn_insert = engine.connect()

            for index, row in df_parquet.iterrows():
                conn_insert.execute(tabela.insert().values(**row))

            conn_insert.commit()
            conn_insert.close()
            
            print("    #### Término da importacao do arquivo ",tab_func, ".parquet em:", datetime.datetime.now() )
            return "SUCESSO"
            

    except Exception as e:
        # Tratamento genérico para qualquer exceção não tratada
        print("Erro ao importar o aquirvo funcionarios.parquet. Erro => ", e)