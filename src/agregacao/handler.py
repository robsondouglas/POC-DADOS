"""Lambda de agregação.

Acionada pelo Kinesis em lotes (BatchSize=30 ou janela de 20s). Decodifica os
records, agrupa por `sexo` e grava um arquivo Parquet por grupo no bucket S3,
particionado por sexo. Usa DuckDB para serializar e gravar o Parquet (mesma
dependência da Lambda de consulta, mantendo o layer pequeno).
"""

from __future__ import annotations

import logging
import os
import tempfile

import boto3
import duckdb

from .processing import CAMPOS, agrupar_por_sexo, decodificar_records, montar_chave_s3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_s3 = boto3.client("s3")

OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "")

# DuckDB grava extensões/temporários em diretório gravável. No Lambda apenas
# /tmp é gravável.
_DUCKDB_HOME = "/tmp/duckdb"


def _conectar() -> duckdb.DuckDBPyConnection:
    os.makedirs(_DUCKDB_HOME, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET home_directory='{_DUCKDB_HOME}';")
    con.execute(f"SET extension_directory='{_DUCKDB_HOME}';")
    return con


def _gravar_parquet(
    con: duckdb.DuckDBPyConnection, bucket: str, key: str, registros: list[dict]
) -> None:
    """Serializa registros em Parquet (DuckDB, em arquivo local) e envia ao S3.

    A gravação é feita em arquivo local e o upload via boto3 (em vez de
    `COPY TO s3://`), o que mantém o uso da IAM role do Lambda e permite testar
    com mocks de S3 (moto), que interceptam o boto3.
    """
    colunas = list(CAMPOS)
    rows = [tuple(r[c] for c in colunas) for r in registros]
    con.execute("DROP TABLE IF EXISTS lote;")
    con.execute(
        "CREATE TABLE lote (id VARCHAR, nome VARCHAR, nascimento BIGINT, sexo VARCHAR);"
    )
    con.executemany(
        "INSERT INTO lote (id, nome, nascimento, sexo) VALUES (?, ?, ?, ?);", rows
    )

    os.makedirs(_DUCKDB_HOME, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        suffix=".parquet", dir=_DUCKDB_HOME, delete=False
    ) as tmp:
        local_path = tmp.name
    try:
        con.execute(f"COPY lote TO '{local_path}' (FORMAT PARQUET);")
        _s3.upload_file(local_path, bucket, key)
        logger.info("Gravado s3://%s/%s (%d registros)", bucket, key, len(registros))
    finally:
        if os.path.exists(local_path):
            os.unlink(local_path)


def handler(event: dict, context: object | None = None) -> dict:
    """Ponto de entrada da Lambda de agregação.

    Args:
        event: evento do Kinesis com a lista de records.
        context: contexto de execução Lambda (não utilizado).

    Returns:
        Resumo do processamento (quantidade gravada por sexo).

    Raises:
        Exception: propaga falhas de gravação para acionar retry/DLQ do
            event source mapping.
    """
    records = event.get("Records", [])
    registros = decodificar_records(records)
    grupos = agrupar_por_sexo(registros)

    resumo: dict[str, int] = {}
    if grupos:
        con = _conectar()
        try:
            for sexo, grupo in grupos.items():
                key = montar_chave_s3(sexo)
                _gravar_parquet(con, OUTPUT_BUCKET, key, grupo)
                resumo[sexo] = len(grupo)
        finally:
            con.close()

    logger.info("Lote processado: %s", resumo)
    return {"processados": resumo}
