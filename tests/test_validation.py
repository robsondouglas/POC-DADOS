"""Testes unitários da validação de schema (Requisito 2)."""

import pytest

from src.ingestao.validation import ValidationError, validar_registro


def registro_valido(**overrides):
    base = {"id": "a1", "nome": "Maria", "nascimento": 631152000, "sexo": "F"}
    base.update(overrides)
    return base


def test_registro_valido_retorna_normalizado():
    out = validar_registro(registro_valido())
    assert out == {"id": "a1", "nome": "Maria", "nascimento": 631152000, "sexo": "F"}


def test_payload_nao_dict():
    with pytest.raises(ValidationError) as exc:
        validar_registro(["nao", "é", "dict"])
    assert exc.value.field == "body"


@pytest.mark.parametrize("valor", [None, "", "   ", 123])
def test_id_invalido(valor):
    with pytest.raises(ValidationError) as exc:
        validar_registro(registro_valido(id=valor))
    assert exc.value.field == "id"


@pytest.mark.parametrize("valor", [None, "", "   ", 123])
def test_nome_invalido(valor):
    with pytest.raises(ValidationError) as exc:
        validar_registro(registro_valido(nome=valor))
    assert exc.value.field == "nome"


@pytest.mark.parametrize("valor", [None, "123", 3.14, True])
def test_nascimento_invalido(valor):
    with pytest.raises(ValidationError) as exc:
        validar_registro(registro_valido(nascimento=valor))
    assert exc.value.field == "nascimento"


@pytest.mark.parametrize("valor", [None, "", "X", "m", "f", 1])
def test_sexo_invalido(valor):
    with pytest.raises(ValidationError) as exc:
        validar_registro(registro_valido(sexo=valor))
    assert exc.value.field == "sexo"


@pytest.mark.parametrize("sexo", ["M", "F"])
def test_sexo_valido(sexo):
    out = validar_registro(registro_valido(sexo=sexo))
    assert out["sexo"] == sexo
