"""Divisão DEV × HOLDOUT dos casos de teste (metade, por hash do id).

Neutra de propósito: a bancada (avaliar) e os exemplos do dono (exemplos)
leem a mesma divisão, sem um importar o outro. O holdout nunca vira exemplo
nem ajuste de prompt; caso que inspirou exemplo do prompt fica sempre no DEV."""
import hashlib


def holdout(caso_id: str) -> bool:
    return int(hashlib.sha1(caso_id.encode()).hexdigest(), 16) % 2 == 0


def em_holdout(d: dict) -> bool:
    """Caso que inspirou um exemplo do prompt nunca é holdout."""
    return not d.get("inspirou_prompt") and holdout(d["id"])
