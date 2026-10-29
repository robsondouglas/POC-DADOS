"""Lambda de consulta.

Lê os arquivos Parquet do bucket de dados (privado) via DuckDB, aplicando
filtro por `sexo` (pushdown de partição) e paginação. Retorna JSON para a
página HTML servida pelo CloudFront. Minimiza PII retornando apenas os campos
necessários à visualização.
"""

from __future__ import annotations

import json
import logging
import os

import duckdb

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DATA_BUCKET = os.environ.get("DATA_BUCKET", "")

# Campos expostos à visualização. `nome` e `nascimento` são PII e ficam de fora
# por padrão para minimizar exposição (Requisito 7.7).
CAMPOS_PUBLICOS = ["id", "sexo"]

MAX_LIMIT = 200
DEFAULT_LIMIT = 50


def _response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _parse_params(event: dict) -> tuple[str | None, int, int]:
    params = event.get("queryStringParameters") or {}
    sexo = params.get("sexo")
    # Campo aberto: qualquer valor não vazio é aceito como filtro de partição.
    if sexo is not None:
        sexo = sexo.strip()
        if not sexo:
            sexo = None

    try:
        limit = int(params.get("limit", DEFAULT_LIMIT))
        offset = int(params.get("offset", 0))
    except (TypeError, ValueError) as err:
        raise ValueError("limit/offset devem ser inteiros") from err

    limit = max(1, min(limit, MAX_LIMIT))
    offset = max(0, offset)
    return sexo, limit, offset


_DUCKDB_HOME = "/tmp/duckdb"


def _consultar(sexo: str | None, limit: int, offset: int) -> list[dict]:
    os.makedirs(_DUCKDB_HOME, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET home_directory='{_DUCKDB_HOME}';")
    con.execute(f"SET extension_directory='{_DUCKDB_HOME}';")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region=?;", [os.environ.get("AWS_REGION", "sa-east-1")])
    # Credenciais via role do Lambda (cadeia de credenciais da AWS).
    con.execute("CREATE OR REPLACE SECRET s3secret (TYPE s3, PROVIDER credential_chain);")

    colunas = ", ".join(CAMPOS_PUBLICOS)
    # hive_partitioning=1 permite filtrar pela coluna de partição `sexo`.
    glob = f"s3://{DATA_BUCKET}/sexo=*/*.parquet"
    sql = (
        f"SELECT {colunas} FROM read_parquet(?, hive_partitioning=1)"
    )
    where = ""
    args: list[object] = [glob]
    if sexo is not None:
        where = " WHERE sexo = ?"
        args.append(sexo)
    sql = sql + where + " LIMIT ? OFFSET ?"
    args.extend([limit, offset])

    rows = con.execute(sql, args).fetchall()
    nomes = [d[0] for d in con.description]
    return [dict(zip(nomes, row, strict=False)) for row in rows]


def handler(event: dict, context: object | None = None) -> dict:
    """Ponto de entrada da Lambda de consulta.

    Query params: `sexo` (opcional, qualquer valor), `limit`, `offset`.
    """
    try:
        sexo, limit, offset = _parse_params(event)
    except ValueError as exc:
        return _response(400, {"error": "invalid_params", "detail": str(exc)})

    try:
        itens = _consultar(sexo, limit, offset)
    except Exception:  # noqa: BLE001
        logger.exception("Falha ao consultar dados no S3")
        return _response(502, {"error": "query_failed"})

    return _response(
        200,
        {"itens": itens, "limit": limit, "offset": offset, "count": len(itens)},
    )
