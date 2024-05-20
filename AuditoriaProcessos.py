import datetime
from sqlalchemy import text

class AuditoriaProcessos:
    try:       
        def gera_log_header(self, session, tenant_id):
            try:   
                data_atual = datetime.datetime.now()
                #cursor = session.connection().connection.cursor()
                max_id_exec = session.execute(text("SELECT MAX(ID_EXEC) FROM LOG_HEADER")).scalar()

                if max_id_exec is None:
                    max_id_exec = 1
                else:
                    max_id_exec += 1

                session.execute(text("INSERT INTO LOG_HEADER (ID_EXEC, DATA_EXEC, TENANT_ID) VALUES (:id_exec, :data_exec, :tenant_id)"),
                                {"id_exec": max_id_exec, "data_exec": data_atual, "tenant_id": tenant_id})
                session.commit()

                return max_id_exec
            
            except Exception as e:
                print("Ocorreu um erro no processamento da inserção do log header. Erro:", e)

        def gera_log_detail(self, session, id_exec, texto):
            try:
                max_id = session.execute(text("SELECT MAX(ID) FROM LOG_DETAIL")).scalar()
                if max_id is None:
                    max_id = 1
                else:
                    max_id += 1                
                session.execute(text("INSERT INTO LOG_DETAIL (ID, ID_EXEC, TEXTO) VALUES (:id, :id_exec, :texto)"),
                                {"id": max_id, "id_exec": id_exec, "texto": texto})
                session.commit()
            except Exception as e:
                print("Ocorreu um erro no processamento da inserção do log detalhe. Erro:", e)

    except Exception as e:
        print("Ocorreu um erro no processamento da extração dos funcionários. Erro => :", e)
        
