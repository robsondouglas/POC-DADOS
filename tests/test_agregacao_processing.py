"""Testes da lógica de processamento da Lambda de agregação (Requisitos 4, 5)."""

import base64
import json
from datetime import UTC, datetime

from src.agregacao.processing import (
    agrupar_por_sexo,
    decodificar_records,
    montar_chave_s3,
)


def _record(registro: dict) -> dict:
    data = base64.b64encode(json.dumps(registro).encode("utf-8")).decode("ascii")
    return {"kinesis": {"data": data}, "eventID": "shardId-000:1"}


def test_decodificar_records_validos():
    regs = [
        {"id": "1", "nome": "A", "nascimento": 10, "sexo": "M"},
        {"id": "2", "nome": "B", "nascimento": 20, "sexo": "F"},
    ]
    out = decodificar_records([_record(r) for r in regs])
    assert out == regs


def test_decodificar_ignora_record_invalido():
    bom = _record({"id": "1", "nome": "A", "nascimento": 10, "sexo": "M"})
    ruim = {"kinesis": {"data": "###nao-base64-valido"}, "eventID": "x"}
    # sexo vazio é inválido (campo aberto, mas exige valor não vazio).
    sem_sexo = _record({"id": "3", "nome": "C", "nascimento": 30, "sexo": ""})
    out = decodificar_records([bom, ruim, sem_sexo])
    assert len(out) == 1
    assert out[0]["id"] == "1"


def test_decodificar_aceita_sexo_aberto():
    regs = [
        {"id": "1", "nome": "A", "nascimento": 10, "sexo": "I"},
        {"id": "2", "nome": "B", "nascimento": 20, "sexo": "A"},
        {"id": "3", "nome": "C", "nascimento": 30, "sexo": 2},
    ]
    out = decodificar_records([_record(r) for r in regs])
    assert [r["sexo"] for r in out] == ["I", "A", "2"]


def test_agrupar_por_sexo():
    regs = [
        {"id": "1", "sexo": "M"},
        {"id": "2", "sexo": "F"},
        {"id": "3", "sexo": "M"},
        {"id": "4", "sexo": "I"},
    ]
    grupos = agrupar_por_sexo(regs)
    assert set(grupos.keys()) == {"M", "F", "I"}
    assert len(grupos["M"]) == 2
    assert len(grupos["F"]) == 1
    assert len(grupos["I"]) == 1


def test_montar_chave_s3_particionada():
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    key = montar_chave_s3("M", now=now)
    assert key.startswith("sexo=M/")
    # Não deve conter particionamento por ano/mês/dia.
    assert key.count("/") == 1
    assert key.endswith(".parquet")


def test_montar_chave_s3_unica():
    assert montar_chave_s3("F") != montar_chave_s3("F")
