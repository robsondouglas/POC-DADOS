"""Funções de processamento da Lambda de agregação.

Responsabilidades isoladas para facilitar testes unitários:
 - decodificar records do Kinesis (base64 -> JSON);
 - agrupar registros por `sexo`;
 - montar a chave S3 particionada por sexo.
"""

from __future__ import annotations

import base64
import json
import logging
import uuid
from datetime import UTC, datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CAMPOS = ("id", "nome", "nascimento", "sexo")
SEXOS_VALIDOS = ("M", "F")


def decodificar_records(records: list[dict]) -> list[dict]:
    """Decodifica os records do Kinesis para dicionários.

    Records individualmente inválidos são ignorados e logados, sem
    interromper o processamento do lote.

    Args:
        records: lista de records do evento Kinesis (cada um com kinesis.data base64).

    Returns:
        Lista de registros válidos decodificados.
    """
    registros: list[dict] = []
    for record in records:
        try:
            data_b64 = record["kinesis"]["data"]
            raw = base64.b64decode(data_b64)
            registro = json.loads(raw)
        except (KeyError, ValueError, TypeError):
            logger.warning("Record inválido ignorado: %s", record.get("eventID"))
            continue

        if not isinstance(registro, dict) or registro.get("sexo") not in SEXOS_VALIDOS:
            logger.warning("Registro sem sexo válido ignorado")
            continue

        registros.append(registro)
    return registros


def agrupar_por_sexo(registros: list[dict]) -> dict[str, list[dict]]:
    """Agrupa os registros por valor de `sexo`.

    Args:
        registros: registros decodificados.

    Returns:
        Mapa {sexo: [registros]} contendo apenas os sexos presentes no lote.
    """
    grupos: dict[str, list[dict]] = {}
    for registro in registros:
        grupos.setdefault(registro["sexo"], []).append(registro)
    return grupos


def montar_chave_s3(sexo: str, now: datetime | None = None) -> str:
    """Monta a chave S3 particionada (estilo Hive) com nome único.

    Formato: sexo=<sexo>/<yyyy>/<MM>/<dd>/<timestamp>-<uuid>.parquet

    Args:
        sexo: valor da partição ('M' ou 'F').
        now: timestamp de referência (default: agora, UTC).

    Returns:
        A chave (key) do objeto S3.
    """
    now = now or datetime.now(UTC)
    ts = now.strftime("%Y%m%dT%H%M%S%f")
    unique = uuid.uuid4().hex
    return (
        f"sexo={sexo}/"
        f"{now.year:04d}/{now.month:02d}/{now.day:02d}/"
        f"{ts}-{unique}.parquet"
    )
