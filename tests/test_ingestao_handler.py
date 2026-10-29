"""Testes da Lambda de ingestão (Requisitos 1, 2, 3)."""

import json

import boto3
import pytest
from moto import mock_aws

STREAM = "eventos-pessoas-stream-test"


@pytest.fixture
def kinesis_stream(monkeypatch):
    with mock_aws():
        monkeypatch.setenv("KINESIS_STREAM_NAME", STREAM)
        monkeypatch.setenv("AWS_DEFAULT_REGION", "sa-east-1")
        client = boto3.client("kinesis", region_name="sa-east-1")
        client.create_stream(StreamName=STREAM, ShardCount=1)
        # Recarrega o módulo para capturar env/clients sob o mock.
        import importlib

        from src.ingestao import handler as h

        importlib.reload(h)
        yield h, client


def _event(body):
    return {"body": body if isinstance(body, str) else json.dumps(body)}


def test_registro_valido_retorna_202_e_publica(kinesis_stream):
    h, client = kinesis_stream
    evt = _event({"id": "a1", "nome": "Ana", "nascimento": 631152000, "sexo": "F"})
    resp = h.handler(evt)
    assert resp["statusCode"] == 202
    assert json.loads(resp["body"])["id"] == "a1"

    # Confirma que o record foi publicado com partition key = sexo.
    desc = client.describe_stream(StreamName=STREAM)
    shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
    it = client.get_shard_iterator(
        StreamName=STREAM, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]
    recs = client.get_records(ShardIterator=it)["Records"]
    assert len(recs) == 1
    assert recs[0]["PartitionKey"] == "F"


def test_corpo_ausente_retorna_400(kinesis_stream):
    h, _ = kinesis_stream
    resp = h.handler({"body": None})
    assert resp["statusCode"] == 400
    assert json.loads(resp["body"])["error"] == "invalid_body"


def test_json_invalido_retorna_400(kinesis_stream):
    h, _ = kinesis_stream
    resp = h.handler({"body": "{nao-json"})
    assert resp["statusCode"] == 400


def test_validacao_falha_retorna_400_com_campo(kinesis_stream):
    h, _ = kinesis_stream
    evt = _event({"id": "a1", "nome": "Ana", "nascimento": 1, "sexo": "X"})
    resp = h.handler(evt)
    assert resp["statusCode"] == 400
    assert json.loads(resp["body"])["field"] == "sexo"


def test_falha_publicacao_retorna_502(monkeypatch):
    # Sem stream criado + nome inexistente => put_record falha.
    monkeypatch.setenv("KINESIS_STREAM_NAME", "inexistente")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "sa-east-1")
    with mock_aws():
        import importlib

        from src.ingestao import handler as h

        importlib.reload(h)
        evt = _event({"id": "a1", "nome": "Ana", "nascimento": 1, "sexo": "M"})
        resp = h.handler(evt)
        assert resp["statusCode"] == 502
