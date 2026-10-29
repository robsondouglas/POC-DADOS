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


@pytest.mark.parametrize("valor", [None, "", "   ", True])
def test_sexo_invalido(valor):
    with pytest.raises(ValidationError) as exc:
        validar_registro(registro_valido(sexo=valor))
    assert exc.value.field == "sexo"


@pytest.mark.parametrize("sexo", ["M", "F", "I", "A"])
def test_sexo_valido(sexo):
    out = validar_registro(registro_valido(sexo=sexo))
    assert out["sexo"] == sexo


@pytest.mark.parametrize("valor,esperado", [(0, "0"), (1, "1"), (2, "2")])
def test_sexo_inteiro_normalizado_para_string(valor, esperado):
    out = validar_registro(registro_valido(sexo=valor))
    assert out["sexo"] == esperado


def test_sexo_string_com_espacos_e_trim():
    out = validar_registro(registro_valido(sexo="  I  "))
    assert out["sexo"] == "I"
