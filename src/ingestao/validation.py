"""Validação de schema do registro de entrada da Lambda de ingestão.

Schema esperado:
    {
        "id": str,            # obrigatório, não vazio
        "nome": str,          # obrigatório, não vazio
        "nascimento": int,    # obrigatório, unixtime em segundos
        "sexo": "M" | "F"     # obrigatório
    }
"""

from __future__ import annotations

SEXOS_VALIDOS = ("M", "F")


class ValidationError(Exception):
    """Erro de validação de um campo do registro.

    Attributes:
        field: nome do campo que falhou.
        message: descrição do problema.
    """

    def __init__(self, field: str, message: str) -> None:
        self.field = field
        self.message = message
        super().__init__(f"{field}: {message}")


def validar_registro(payload: object) -> dict:
    """Valida o payload de entrada e retorna o registro normalizado.

    Args:
        payload: objeto decodificado do corpo JSON da requisição.

    Returns:
        Dicionário com os campos validados: id, nome, nascimento, sexo.

    Raises:
        ValidationError: se qualquer campo estiver ausente ou inválido.
    """
    if not isinstance(payload, dict):
        raise ValidationError("body", "corpo deve ser um objeto JSON")

    id_ = payload.get("id")
    if not isinstance(id_, str) or not id_.strip():
        raise ValidationError("id", "obrigatório, deve ser string não vazia")

    nome = payload.get("nome")
    if not isinstance(nome, str) or not nome.strip():
        raise ValidationError("nome", "obrigatório, deve ser string não vazia")

    nascimento = payload.get("nascimento")
    # bool é subclasse de int em Python; rejeitar explicitamente.
    if not isinstance(nascimento, int) or isinstance(nascimento, bool):
        raise ValidationError(
            "nascimento", "obrigatório, deve ser inteiro (unixtime em segundos)"
        )

    sexo = payload.get("sexo")
    if sexo not in SEXOS_VALIDOS:
        raise ValidationError("sexo", "obrigatório, deve ser 'M' ou 'F'")

    return {
        "id": id_,
        "nome": nome,
        "nascimento": nascimento,
        "sexo": sexo,
    }
