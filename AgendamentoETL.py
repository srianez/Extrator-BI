import schedule
import json
import time
import subprocess
from sqlalchemy import create_engine, Column, String, update
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker


def carregar_configuracoes():
    with open('configs.json') as config_file:
        return json.load(config_file)


def conectar_banco(configuracoes):
    conexao_banco = configuracoes['conexao_banco']['origem']
    engine = create_engine(conexao_banco)
    Session = sessionmaker(bind=engine)
    return Session()

Base = declarative_base()
class Agendamentos(Base):
    __tablename__ = 'agendamento_bi'
    hora_execucao = Column(String, primary_key=True)
    status = Column(String)

def recuperar_horarios_execucao(session):
    return [agendamento.hora_execucao for agendamento in session.query(Agendamentos).all()]


def executar_ETL():
    subprocess.run(["python", "ProcessarETL.py"])


def configurar_agendamentos(horarios_execucao):
    for hora_execucao in horarios_execucao:
        print("Vou processar às", hora_execucao)
        schedule.every().day.at(hora_execucao).do(executar_ETL)

def atualizar_status_em_processamento(session):
    session.execute(update(Agendamentos).values(status='EM PROCESSAMENTO'))


def atualizar_status_concluido(session):
    session.execute(update(Agendamentos).values(status='CONCLUÍDO'))


if __name__ == "__main__":
    configuracoes = carregar_configuracoes()
    session = conectar_banco(configuracoes)
    horarios_execucao = recuperar_horarios_execucao(session)
    configurar_agendamentos(horarios_execucao)

    while True:
        atualizar_status_em_processamento(session)
        schedule.run_pending()
        atualizar_status_concluido(session)
        time.sleep(1)