import json
from ExtrairFuncionarios import ExtrairFuncionarios
from Logger import Logger
from ExtrairFreqAfast import ExtrairFreqAfast
from sqlalchemy.orm import sessionmaker
from Importador import Importador
from AuditoriaProcessos import AuditoriaProcessos
import datetime
from sqlalchemy import create_engine

class ProcessarETL:

    @staticmethod
    def configure_logger(log_directory, log_file):
        Logger.configure(log_directory, log_file)

    @staticmethod
    def carregar_configuracoes():
        with open('configs.json') as config_file:
            return json.load(config_file)

    @staticmethod
    def conectar_banco(configuracoes):
        conexao_banco = configuracoes['conexao_banco']['origem']
        engine = create_engine(conexao_banco)
        Session = sessionmaker(bind=engine)
        return Session()

    @staticmethod
    def executar():
        try:
            tenant_id = 2

            configuracoes = ProcessarETL.carregar_configuracoes()
            session = ProcessarETL.conectar_banco(configuracoes)

            ProcessarETL.configure_logger(configuracoes['arquivo']['diretorioLogETL'], configuracoes['arquivo']['arquivoLogETL'])

            auditoria = AuditoriaProcessos()
            id_exec = auditoria.gera_log_header(session, tenant_id)
            auditoria.gera_log_detail(session, id_exec, "#### Início do processamento ####")
            
            Logger.log_info("Início do processamento")

            # Processa funcionarios
            try:
                extrator_func = ExtrairFuncionarios()            
                status = extrator_func.extrair_funcionarios(session, configuracoes['conexao_banco']['origem'], configuracoes['arquivo']['arquivoFunc'], configuracoes['tabela']['func'], tenant_id, id_exec, Logger)    
              
                # Importar o arquivo de funcionarios se sucesso
                if status == "SUCESSO":
                    importador = Importador()
                    status = importador.importar_arquivo(configuracoes['conexao_banco']['destino'], configuracoes['arquivo']['arquivoFunc'], configuracoes['tabela']['func'], tenant_id, session, id_exec, Logger)
            
            except Exception as exFunc:
                Logger.log_error(f"Ocorreu um erro no processamento do ETL de Funcionários. Erro => : {exFunc}")
                print(f"Ocorreu um erro no processamento do ETL de Funcionários. Erro => : {exFunc}")

            # Processa frequencias
            try:
                extrator_freq_afast = ExtrairFreqAfast()
                status = extrator_freq_afast.extrair_freq_afast(session, configuracoes['conexao_banco']['origem'], configuracoes['arquivo']['arquivoFreqAfast'], configuracoes['tabela']['freqAfast'], tenant_id, id_exec, Logger)    

                if status == "SUCESSO":
                    # Importa o arquivo de frequencias se sucesso
                    importador = Importador()
                    status = importador.importar_arquivo(configuracoes['conexao_banco']['destino'], configuracoes['arquivo']['arquivoFreqAfast'], configuracoes['tabela']['freqAfast'], tenant_id, session, id_exec, Logger)
            
                Logger.log_info("Término do processamento")
            
            except Exception as exFreq:
                Logger.log_error(f"Ocorreu um erro no processamento do ETL de Frequencias. Erro => : {exFreq}")
                print(f"Ocorreu um erro no processamento do ETL de Frequencias. Erro => : {exFreq}")

        except Exception as e:
            Logger.log_error(f"Ocorreu um erro no processamento da execução da classe ProcessarETL. Erro => : {e}")
            print(f"Ocorreu um erro no processamento da execução da classe ProcessarETL. Erro => : {e}")

if __name__ == "__main__":
    ProcessarETL.executar()
