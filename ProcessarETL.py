import json
from ExtrairFuncionarios import ExtrairFuncionarios
from ExtrairFreqAfast import ExtrairFreqAfast
from Importador import Importador
import datetime


class ProcessarETL:
    try:
        def __init__(self):
            tenant_id = 1

            with open('configs.json') as config_file:
                config = json.load(config_file)
        
            # Obtendo informações do Json de configuração
            banco_oriem = config['conexao_banco']['origem']
            banco_destino = config['conexao_banco']['destino']  
            arquivo_func = config['arquivo']['arquivoFunc']
            arquivo_freq = config['arquivo']['arquivoFreqAfast']
            arquivo_pagto_cabec = config['arquivo']['arquivoPagtoCabec']
            arquivo_pagto_rubri = config['arquivo']['arquivoPagtoRubri']
            

            tab_func = config['tabela']['func']
            tab_freqAfast = config['tabela']['freqAfast']
            tab_pagtoCabec = config['tabela']['pagtoCabec']
            tab_pagtoRubri = config['tabela']['pagtoRubri']

            try:
                #########################################################################
                ##################### PROCESSAMENTO DE FUNCIONARIOS #####################
                #########################################################################
                extrator_func = ExtrairFuncionarios()
                status = extrator_func.extrair_funcionarios(banco_oriem, arquivo_func, tab_func, tenant_id)    

                if status == "SUCESSO":
                    importador = Importador()
                    status = importador.importar_arquivo(banco_destino, arquivo_func, tab_func, tenant_id)

            except Exception as exFunc:
                print("Ocorreu um erro no processamento do ETL de Funcionários. Erro => :", exFunc)    
            
            try:
                #########################################################################
                #####################  PROCESSAMENTO DE FREQ AFAST  #####################
                #########################################################################
                extrator_freq_afast = ExtrairFreqAfast()
                status = extrator_freq_afast.extrair_freq_afast(banco_oriem, arquivo_freq, tab_freqAfast, tenant_id)    

                if status == "SUCESSO":
                    importador = Importador()
                    status = importador.importar_arquivo(banco_destino, arquivo_freq, tab_freqAfast, tenant_id)

            except Exception as exFreq:
                print("Ocorreu um erro no processamento do ETL de Frequencias Erro => :", exFreq)

    except Exception as e:
        print("Ocorreu um erro no processamento da execução da classe ProcessarETL. Erro => :", e)

def main():
    processarETL = ProcessarETL()

if __name__ == "__main__":
  #garante que o código seja executado apenas quando o script for executado diretamente e não importado como um módulo.
  main()    
            
