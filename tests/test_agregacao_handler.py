"""Teste de integração da Lambda de agregação: grava Parquet no S3 (mock).

Cobre os Requisitos 5.1, 5.3-5.7: Parquet, partição por sexo, arquivos
separados por partição.
"""

import base64
import importlib
import json

import boto3
import pytest
from moto import mock_aws

BUCKET = "eventos-pessoas-dados-test"


def _record(registro: dict) -> dict:
    data = base64.b64encode(json.dumps(registro).encode("utf-8")).decode("ascii")
    return {"kinesis": {"data": data}, "eventID": "shardId-000:1"}


@pytest.fixture
def s3_bucket(monkeypatch):
    with mock_aws():
        monkeypatch.setenv("OUTPUT_BUCKET", BUCKET)
        monkeypatch.setenv("AWS_DEFAULT_REGION", "sa-east-1")
        client = boto3.client("s3", region_name="sa-east-1")
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "sa-east-1"},
        )
        from src.agregacao import handler as h

        importlib.reload(h)
        yield h, client


def test_lote_misto_gera_arquivos_por_particao(s3_bucket):
    h, client = s3_bucket
    event = {
        "Records": [
            _record({"id": "1", "nome": "A", "nascimento": 10, "sexo": "M"}),
            _record({"id": "2", "nome": "B", "nascimento": 20, "sexo": "F"}),
            _record({"id": "3", "nome": "C", "nascimento": 30, "sexo": "M"}),
        ]
    }
    resumo = h.handler(event)
    assert resumo["processados"] == {"M": 2, "F": 1}

    objs = client.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    keys = [o["Key"] for o in objs]
    assert any(k.startswith("sexo=M/") for k in keys)
    assert any(k.startswith("sexo=F/") for k in keys)
    assert all(k.endswith(".parquet") for k in keys)


def test_parquet_preserva_schema(s3_bucket):
    h, client = s3_bucket
    event = {
        "Records": [
            _record({"id": "x", "nome": "Nome X", "nascimento": 12345, "sexo": "F"}),
        ]
    }
    h.handler(event)
    objs = client.list_objects_v2(Bucket=BUCKET)["Contents"]
    body = client.get_object(Bucket=BUCKET, Key=objs[0]["Key"])["Body"].read()

    # Lê o Parquet de volta com DuckDB a partir dos bytes em /tmp.
    import os
    import tempfile

    import duckdb

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp.write(body)
        tmp_path = tmp.name
    try:
        con = duckdb.connect()
        cols = [
            r[0]
            for r in con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [tmp_path]
            ).fetchall()
        ]
        assert cols == ["id", "nome", "nascimento", "sexo"]
        row = con.execute(
            "SELECT id, nascimento FROM read_parquet(?)", [tmp_path]
        ).fetchone()
        assert row[0] == "x"
        assert int(row[1]) == 12345
    finally:
        os.unlink(tmp_path)
