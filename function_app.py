import csv
import io
import logging
import os
from datetime import UTC, datetime

import azure.functions as func
import pyodbc
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()
logger = logging.getLogger(__name__)


def extract_table(table_name: str) -> None:
    """
    Extrai os dados de uma tabela do SQL Server,
    converte para CSV e salva no Azure Data Lake.
    """

    # 1. Ler as variáveis de ambiente
    host_sql = os.getenv("HOST")
    database_sql = os.getenv("DATABASE")
    user_sql = os.getenv("USER")
    password_sql = os.getenv("PASSWORD")
    datalake_connection = os.getenv("DATALAKE_CONNECTION_STRING")

    # 2. Validar as configurações
    if not all(
        [
            host_sql,
            database_sql,
            user_sql,
            password_sql,
            datalake_connection,
        ]
    ):
        raise ValueError(
            "Configure HOST, DATABASE, USER, PASSWORD e "
            "DATALAKE_CONNECTION_STRING nas variáveis de ambiente."
        )

    # 3. Configurar a conexão com SQL Server
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # 4. Consultar a tabela
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            query = f"SELECT * FROM itsm.{table_name}"
            cursor.execute(query)

            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()

        logger.info(
            "Tabela itsm.%s: %s registros encontrados.",
            table_name,
            len(rows),
        )

        # 5. Converter os dados para CSV
        buffer = io.StringIO()
        writer = csv.writer(buffer)

        writer.writerow(columns)
        writer.writerows(rows)

        csv_data = buffer.getvalue().encode("utf-8-sig")
        buffer.close()

        # 6. Criar o caminho do arquivo com data e hora UTC
        now = datetime.now(UTC)

        file_path = (
            f"{table_name}/" f"{now:%Y/%m/%d}/" f"{table_name}_{now:%Y%m%d_%H%M%S}.csv"
        )

        # 7. Conectar ao Azure Data Lake
        datalake = DataLakeServiceClient.from_connection_string(datalake_connection)

        # 8. Selecionar o contêiner bronze e o arquivo
        file_system_client = datalake.get_file_system_client("bronze")
        file_client = file_system_client.get_file_client(file_path)

        # 9. Enviar o CSV para o Data Lake
        file_client.upload_data(
            csv_data,
            overwrite=True,
        )

        logger.info(
            "Arquivo gravado com sucesso: bronze/%s",
            file_path,
        )

        datalake.close()

    except Exception:
        logger.exception(
            "Erro ao extrair ou salvar a tabela itsm.%s.",
            table_name,
        )
        raise


# 1. AVALIAÇÃO CSAT


@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:
    extract_table("csat_avaliacao")


# 2. FILA


@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_fila(myTimer: func.TimerRequest) -> None:
    extract_table("fila")
