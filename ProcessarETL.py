import json
from ExtrairFuncionarios import ExtrairFuncionarios
from ExtrairFreqAfast import ExtrairFreqAfast
from sqlalchemy.orm import sessionmaker
from Importador import Importador
from AuditoriaProcessos import AuditoriaProcessos
import datetime
from sqlalchemy import create_engine


class ProcessarETL:
    try:
        tenant_id = 2

        try:

            #carrega as informações do arquivo de configurações
            def carregar_configuracoes():
                with open('configs.json') as config_file:
                    return json.load(config_file)

            #cria a conexão com o banco de dados
            def conectar_banco(configuracoes):
                conexao_banco = configuracoes['conexao_banco']['origem']
                engine = create_engine(conexao_banco)
                Session = sessionmaker(bind=engine)
                return Session()
            
            configuracoes = carregar_configuracoes()
            session = conectar_banco(configuracoes)

            auditoria = AuditoriaProcessos()
            id_exec = auditoria.gera_log_header(session, tenant_id)
            auditoria.gera_log_detail(session, id_exec, "#### Início do processamento ####")

            #processa funcionarios
            extrator_func = ExtrairFuncionarios()            
            status = extrator_func.extrair_funcionarios(session, configuracoes['conexao_banco']['origem'], configuracoes['arquivo']['arquivoFunc'], configuracoes['tabela']['func'], tenant_id, id_exec)    
          
            #importar o arquivo de funcionarios se sucesso
            if status == "SUCESSO":
                importador = Importador()
                status = importador.importar_arquivo(configuracoes['conexao_banco']['destino'], configuracoes['arquivo']['arquivoFunc'], configuracoes['tabela']['func'], tenant_id, session, id_exec)

        except Exception as exFunc:
            print("Ocorreu um erro no processamento do ETL de Funcionários. Erro => :", exFunc)    
        
        try:
            #processa frequencias
            extrator_freq_afast = ExtrairFreqAfast()
            status = extrator_freq_afast.extrair_freq_afast(session, configuracoes['conexao_banco']['origem'], configuracoes['arquivo']['arquivoFreqAfast'], configuracoes['tabela']['freqAfast'], tenant_id, id_exec)    

            if status == "SUCESSO":
                #importa o arquivo de frequencias se sucesso
                importador = Importador()
                status = importador.importar_arquivo(configuracoes['conexao_banco']['destino'], configuracoes['arquivo']['arquivoFreqAfast'], configuracoes['tabela']['freqAfast'], tenant_id, session, id_exec)

        except Exception as exFreq:
            print("Ocorreu um erro no processamento do ETL de Frequencias Erro => :", exFreq)

    except Exception as e:
        print("Ocorreu um erro no processamento da execução da classe ProcessarETL. Erro => :", e)
