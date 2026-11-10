
import logging
import os

import azure.functions as func
import pyodbc

app = func.FunctionApp()


# ============================================================
# FUNÇÃO AUXILIAR PARA EXTRAÇÃO DAS TABELAS
# ============================================================

def extract_table(table_name: str) -> None:

    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    if not all([sql_server, sql_database, sql_user, sql_pass]):
        raise ValueError("Variáveis de conexão não configuradas.")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    query = f"SELECT * FROM itsm.{table_name}"

    try:
        with pyodbc.connect(conn_str) as conn:

            cursor = conn.cursor()

            cursor.execute(query)

            rows = cursor.fetchall()

            logging.info(
                "Tabela %s: %d registros extraídos.",
                table_name,
                len(rows)
            )

    except Exception:
        logging.exception(
            "Erro ao ler tabela itsm.%s",
            table_name
        )
        raise


# ============================================================
# EXTRAÇÃO DA TABELA ANALISTA
# ============================================================

@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def extract_analista(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração: analista")

    extract_table("analista")


# ============================================================
# EXTRAÇÃO DA TABELA CATEGORIA
# ============================================================

@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def extract_categoria(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração: categoria")

    extract_table("categoria")


# ============================================================
# EXTRAÇÃO DA TABELA CHAMADO
# ============================================================

@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def extract_chamado(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração: chamado")

    extract_table("chamado")


# ============================================================
# EXTRAÇÃO DA TABELA CHAMADO_SLA
# ============================================================

@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def extract_chamado_sla(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração: chamado_sla")

    extract_table("chamado_sla")


# ============================================================
# EXTRAÇÃO DA TABELA CHAMADO_STATUS_HISTORICO
# ============================================================

@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def extract_chamado_status_historico(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração: chamado_status_historico")

    extract_table("chamado_status_historico")


# ============================================================
# EXTRAÇÃO DA TABELA CLIENTE_ORGANIZACAO
# ============================================================

@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def extract_cliente_organizacao(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração: cliente_organizacao")

    extract_table("cliente_organizacao")


# ============================================================
# EXTRAÇÃO DA TABELA CSAT
# ============================================================

@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def extract_csat(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração: csat")

    extract_table("csat")


# ============================================================
# EXTRAÇÃO DA TABELA CSAT_AVALIACAO
# ============================================================

@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração: csat_avaliacao")

    extract_table("csat_avaliacao")


# ============================================================
# EXTRAÇÃO DA TABELA FILA
# ============================================================

@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def extract_fila(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração: fila")

    extract_table("fila")


# ============================================================
# EXTRAÇÃO DA TABELA SLA
# ============================================================

@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def extract_sla(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração: sla")

    extract_table("sla")


# ============================================================
# EXTRAÇÃO DA TABELA SOLICITANTE
# ============================================================

@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def extract_solicitante(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração: solicitante")

    extract_table("solicitante")
