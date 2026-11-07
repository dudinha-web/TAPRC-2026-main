import csv
import io
import logging
import os
from datetime import UTC, date, datetime, time

import azure.functions as func
import pyodbc
from azure.core.exceptions import AzureError, ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()
logger = logging.getLogger(_name_)


def _table_metadata(cursor, schema: str, table_name: str):
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        schema,
        table_name,
    )
    columns = {
        row[0].lower(): (row[0], row[1].lower(), row[2] == "YES")
        for row in cursor.fetchall()
    }
    if not columns:
        raise ValueError(f"Tabela {schema}.{table_name} não existe no banco de destino")

    cursor.execute(
        "SELECT kcu.COLUMN_NAME "
        "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
        "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu "
        "  ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME "
        " AND kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA "
        " AND kcu.TABLE_NAME = tc.TABLE_NAME "
        "WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' "
        "  AND tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ? "
        "ORDER BY kcu.ORDINAL_POSITION",
        schema,
        table_name,
    )
    pk_cols = [row[0] for row in cursor.fetchall()]

    cursor.execute(
        "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?) AND is_identity = 1",
        f"{schema}.{table_name}",
    )
    identity_cols = [row[0] for row in cursor.fetchall()]

    return columns, pk_cols, identity_cols


def _convert(value: str, data_type: str, nullable: bool):
    if value == "":
        return None if nullable else value
    if data_type in ("datetime", "datetime2", "smalldatetime"):
        return datetime.fromisoformat(value)
    if data_type == "date":
        return date.fromisoformat(value[:10])
    if data_type == "time":
        return time.fromisoformat(value)
    if data_type == "bit":
        return value.strip().lower() in ("1", "true")
    return value


def load_datalake_to_sql(
    schema: str, table_name: str, run_date: date | None = None
) -> int:
    datalake_conn_str = os.getenv("DATALAKE_CONNECTION_STRING")
    user_sql = os.getenv("DEST_USER") or os.getenv("USER")
    database_sql = os.getenv("DEST_DATABASE") or os.getenv("DATABASE")
    host_sql = os.getenv("DEST_HOST") or os.getenv("HOST")
    password_sql = (os.getenv("DEST_PASSWORD") or os.getenv("PASSWORD") or "").replace(
        "}", "}}"
    )

    if not all([datalake_conn_str, user_sql, database_sql, host_sql, password_sql]):
        raise RuntimeError(
            "Variáveis de ambiente ausentes (DATALAKE_CONNECTION_STRING, USER, DATABASE, HOST, PASSWORD)"
        )

    sql_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    conn = None
    try:
        # Achar o CSV mais recente da pasta do dia (UTC, igual ao extract)
        run_date = run_date or datetime.now(UTC).date()
        folder_path = f"{schema}/{table_name}/{run_date:%Y/%m/%d}"

        datalake = DataLakeServiceClient.from_connection_string(datalake_conn_str)
        file_system_client = datalake.get_file_system_client("bronze")

        try:
            file_paths = [
                p.name
                for p in file_system_client.get_paths(path=folder_path, recursive=False)
                if not p.is_directory and p.name.lower().endswith(".csv")
            ]
        except ResourceNotFoundError:
            file_paths = []

        if not file_paths:
            logger.info(f"Nenhum arquivo CSV encontrado em bronze/{folder_path}")
            return 0

        latest_file_path = max(file_paths)  # nome tem timestamp => max = mais novo
        logger.info(f"Iniciando leitura do arquivo: {latest_file_path}")

        file_client = file_system_client.get_file_client(latest_file_path)
        csv_string = file_client.download_file().readall().decode("utf-8-sig")

        csv_reader = csv.reader(io.StringIO(csv_string, newline=""))
        columns = next(csv_reader, None)
        rows = [row for row in csv_reader if row]

        if not columns or not rows:
            logger.info("O arquivo CSV está vazio (apenas cabeçalho).")
            return 0

        if any(len(row) != len(columns) for row in rows):
            raise ValueError(
                f"{latest_file_path}: há linhas com número de colunas diferente do cabeçalho"
            )

        target = f"[{schema}].[{table_name}]"
        conn = pyodbc.connect(sql_conn_str)
        cursor = conn.cursor()

        meta, pk_cols, identity_cols = _table_metadata(cursor, schema, table_name)

        missing = [c for c in columns if c.lower() not in meta]
        if missing:
            raise ValueError(
                f"Colunas do CSV que não existem em {schema}.{table_name}: {missing}"
            )

        col_defs = [meta[c.lower()] for c in columns]
        real_cols = [d[0] for d in col_defs]
        data = [
            tuple(_convert(v, d[1], d[2]) for v, d in zip(row, col_defs, strict=True))
            for row in rows
        ]

        quoted = ", ".join(f"[{c}]" for c in real_cols)
        placeholders = ", ".join(["?"] * len(real_cols))
        identity_lower = {c.lower() for c in identity_cols}
        pk_lower = {c.lower() for c in pk_cols}
        has_identity = any(c.lower() in identity_lower for c in real_cols)

        # Carrega o CSV numa tabela temporária com a estrutura do destino
        cursor.execute(f"SELECT {quoted} INTO #stage FROM {target} WHERE 1 = 0")
        if has_identity:
            cursor.execute("SET IDENTITY_INSERT #stage ON")
        cursor.executemany(
            f"INSERT INTO #stage ({quoted}) VALUES ({placeholders})", data
        )
        if has_identity:
            cursor.execute("SET IDENTITY_INSERT #stage OFF")

        if has_identity:
            cursor.execute(f"SET IDENTITY_INSERT {target} ON")

        if pk_cols:
            if not pk_lower.issubset({c.lower() for c in real_cols}):
                raise ValueError(
                    f"CSV não contém todas as colunas da PK de {schema}.{table_name}: {pk_cols}"
                )
            on_clause = " AND ".join(f"t.[{k}] = s.[{k}]" for k in pk_cols)
            update_cols = [
                c
                for c in real_cols
                if c.lower() not in pk_lower and c.lower() not in identity_lower
            ]
            merge_sql = f"MERGE {target} AS t USING #stage AS s ON {on_clause} "
            if update_cols:
                set_clause = ", ".join(f"t.[{c}] = s.[{c}]" for c in update_cols)
                merge_sql += f"WHEN MATCHED THEN UPDATE SET {set_clause} "
            insert_vals = ", ".join(f"s.[{c}]" for c in real_cols)
            merge_sql += (
                f"WHEN NOT MATCHED BY TARGET THEN INSERT ({quoted}) "
                f"VALUES ({insert_vals});"
            )
            cursor.execute(merge_sql)
        else:
            logger.warning(
                f"{schema}.{table_name} não tem PK: usando INSERT simples (pode duplicar)"
            )
            cursor.execute(
                f"INSERT INTO {target} ({quoted}) SELECT {quoted} FROM #stage"
            )

        if has_identity:
            cursor.execute(f"SET IDENTITY_INSERT {target} OFF")

        conn.commit()

        logger.info(
            f"{len(rows)} linhas processadas com sucesso na tabela {schema}.{table_name}!"
        )
        return len(rows)

    except Exception as e:
        if conn is not None:
            conn.rollback()
        logger.error(
            f"Erro ao carregar dados do Data Lake para a tabela {schema}.{table_name}: {e!s}"
        )
        raise
    finally:
        if conn is not None:
            conn.close()


def extract_table_to_datalake(schema: str, table_name: str) -> None:
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

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
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = f"SELECT * FROM {schema}.{table_name}"
            cursor.execute(query)

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"{len(rows)} linhas lidas de {schema}.{table_name}")

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        now = datetime.now(UTC)
        file_path = (
            f"{schema}/{table_name}/{now:%Y/%m/%d}/{table_name}{now:%Y%m%d%H%M%S}.csv"
        )

        datalake = DataLakeServiceClient.from_connection_string(
            os.getenv("DATALAKE_CONNECTION_STRING")
        )
        file_client = datalake.get_file_system_client("bronze").get_file_client(
            file_path
        )

        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)
        logger.info(f"Arquivo gravado em bronze/{file_path}")

    except Exception as e:
        logger.error(f"Erro ao processar a tabela {schema}.{table_name}: {e!s}")
        raise


SCHEDULE_TIME = "0 30 22 * * *"


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "chamado")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_sla(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "chamado_sla")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "chamado_status_historico")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "cliente_organizacao")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_analista(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "analista")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_categoria(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "categoria")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_csat(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "csat")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "csat_avaliacao")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_fila(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "fila")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_sla(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "sla")


@app.timer_trigger(
    schedule=SCHEDULE_TIME, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_solicitante(myTimer: func.TimerRequest) -> None:
    extract_table_to_datalake("itsm", "solicitante")


@app.timer_trigger(
    schedule="0 35 22 * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def load_all_bronze_to_sql(myTimer: func.TimerRequest) -> None:
    # Pais antes dos filhos (respeita as FKs)
    tabelas_ordenadas = [
        "categoria",
        "cliente_organizacao",
        "fila",
        "sla",
        "analista",
        "solicitante",
        "chamado",
        "chamado_sla",
        "chamado_status_historico",
        "csat",
        "csat_avaliacao",
    ]

    logger.info(
        "A iniciar o carregamento em massa do Data Lake para o SQL (somente arquivos de hoje)..."
    )

    falhas: dict[str, str] = {}

    for tabela in tabelas_ordenadas:
        try:
            logger.info(f"A processar tabela: itsm.{tabela}")
            load_datalake_to_sql("itsm", tabela)

        except (pyodbc.Error, AzureError, ValueError, RuntimeError, csv.Error) as e:
            falhas[tabela] = str(e)
            logger.error(f"Falha ao carregar a tabela itsm.{tabela}: {e!s}")

    logger.info("Carregamento em massa concluído!")

    if falhas:
        # Faz a execução aparecer como falha no monitoramento do Azure
        raise RuntimeError(f"Falha em {len(falhas)} tabela(s): {falhas}")