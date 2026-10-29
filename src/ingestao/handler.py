"""Lambda de ingestão.

Recebe uma requisição HTTPS (API Gateway HTTP API, payload v2), valida o
registro e o publica no Kinesis Data Stream usando `sexo` como partition key.
"""

from __future__ import annotations

import json
import logging
import os

import boto3

from .validation import ValidationError, validar_registro

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_kinesis = boto3.client("kinesis")

STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME", "")


def _response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def handler(event: dict, context: object | None = None) -> dict:
    """Ponto de entrada da Lambda de ingestão.

    Args:
        event: evento do API Gateway HTTP API (payload format 2.0).
        context: contexto de execução Lambda (não utilizado).

    Returns:
        Resposta HTTP: 202 (aceito), 400 (validação), 502 (falha de publicação).
    """
    raw_body = event.get("body")
    if raw_body is None or raw_body == "":
        return _response(400, {"error": "invalid_body", "detail": "corpo ausente"})

    try:
        payload = json.loads(raw_body)
    except (json.JSONDecodeError, TypeError):
        return _response(400, {"error": "invalid_body", "detail": "JSON inválido"})

    try:
        registro = validar_registro(payload)
    except ValidationError as exc:
        return _response(
            400, {"error": "validation_error", "field": exc.field, "detail": exc.message}
        )

    try:
        _kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(registro).encode("utf-8"),
            PartitionKey=registro["sexo"],
        )
    except Exception:  # noqa: BLE001 - loga e retorna 502 para o cliente
        logger.exception("Falha ao publicar registro no Kinesis")
        return _response(502, {"error": "publish_failed"})

    return _response(202, {"status": "accepted", "id": registro["id"]})
