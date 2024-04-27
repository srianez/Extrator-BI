import pandas as pd
import json
import sqlalchemy
import datetime
from sqlalchemy import create_engine, inspect, MetaData, Table, delete

# Carregando as configurações do arquivo JSON
with open('configs.json') as config_file:
    config = json.load(config_file)

# Obtendo informações do Json de configuração
database_vendor = config['databaseVendor']['vendor']
database_oracle = config['databaseVendor']['origin']
database_ms = config['databaseVendor']['originMS']
tab_func = config['tables']['func']
parquet_file_path = config['files']['fileFunc']

# Carregue os dados do arquivo Parquet em um DataFrame Pandas
print("#### Inicio da importacao do arquivo ",tab_func,".parquet | Os dados serão carregados na base ", database_vendor, "Inicio do processamento: ", datetime.datetime.now() )
df_parquet = pd.read_parquet(parquet_file_path)

#define a conexão que será criada conforme o banco de dados
if database_vendor == 'oracle':
    engine = sqlalchemy.create_engine(database_oracle)
    print("    ###### Conexao estabelecida com a base ", database_vendor)
else:
    engine = sqlalchemy.create_engine(database_ms)
    print("    ###### Conexao estabelecida com a base ", database_vendor)

# Crie um objeto MetaData. Metadata contém as especificações das tabelas, não os dados contidos nelas.
metadata = MetaData()
metadata.reflect(bind=engine)

# Cria um objeto de tabela no SQLAlchemy que representa a estrutura da tabela de funcionarios existente no banco
tabela = Table(tab_func, metadata, autoload=True, autoload_with=engine)

try:
    # deleta os registros existentes
    stmt = delete(tabela).where(tabela.c.tenantid == 1)
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

    print("#### Término da importacao do arquivo ",tab_func, ".parquet em:", datetime.datetime.now() )

except Exception as e:
    # Tratamento genérico para qualquer exceção não tratada
    print("Erro ao importar o aquirvo ", tab_func, ".parquet. Erro => ", e)


# Use o Pandas para carregar os dados do DataFrame para a tabela do banco de dados
# Certifique-se de substituir 'if_exists' com 'replace' ou 'append' dependendo da sua necessidade
# Se quiser substituir a tabela existente, use 'replace', se quiser adicionar aos dados existentes, use 'append'
# df_parquet.to_sql(tab_func, engine, if_exists='replace', index=False)

#df_parquet.to_sql(tab_func, engine, if_exists='append', index=False)

