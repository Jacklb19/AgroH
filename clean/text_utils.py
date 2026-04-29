"""Helpers compartidos de normalizacion de texto para llaves naturales."""

import unicodedata


def normalizar_clave_texto(valor: str) -> str:
    """Mayusculas, sin tildes y con espacios colapsados."""
    if not isinstance(valor, str):
        return ""
    valor = unicodedata.normalize("NFD", valor.strip().upper())
    valor = "".join(c for c in valor if unicodedata.category(c) != "Mn")
    return " ".join(valor.split())
