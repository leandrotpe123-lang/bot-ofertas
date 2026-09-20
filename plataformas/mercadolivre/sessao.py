"""
Sessão do Programa de Afiliados — Mercado Livre.

Responsabilidade ÚNICA: estado e validade da credencial de sessão.
Fonte única de verdade, em memória, para todo o pacote.

Não faz HTTP. Não decide fluxo. Não conhece produto, cache nem
publicação.

═══════════════════════════════════════════════════════════════════
POR QUE NÃO HÁ RENOVAÇÃO AUTOMÁTICA AQUI
═══════════════════════════════════════════════════════════════════
Não há mecanismo oficial, documentado e estável de renovação
automática da sessão do Programa de Afiliados que esteja comprovado
e que possamos usar no Foguetão.

A auditoria verificou: não existe API pública oficial de afiliados;
o `createLink` é endpoint interno do portal e aceita apenas sessão
de navegador; o OAuth da API de vendedores é outro sistema e não o
autentica. Implementações públicas de terceiros usam exatamente o
mesmo modelo — cookie provisionado manualmente, uma vez.

Portanto a sessão é tratada aqui como CREDENCIAL PROVISIONADA
EXTERNAMENTE, cujo ciclo de vida o sistema observa e reporta, mas
não renova.

Este módulo NÃO contém login, NÃO contém renovação e NÃO simula
nenhuma das duas. `marcar_expirada` registra um fato; não tenta
resolvê-lo.

═══════════════════════════════════════════════════════════════════
CONCORRÊNCIA
═══════════════════════════════════════════════════════════════════
O lock e o contador de geração existem para preparar a arquitetura:
quando houver renovação, várias ofertas simultâneas detectando
expiração não poderão disparar vários ciclos.

Hoje o lock protege apenas a transição de estado — a primeira
detecção marca e alerta; as seguintes leem o estado já marcado e
não repetem o alerta. Nenhuma tentativa de renovação é feita.
"""
from __future__ import annotations

import asyncio
import base64
import os
import re
import time
from dataclasses import dataclass
from typing import Optional

from logger import log_nrm

from . import ajustes


# ── Origem da credencial ──────────────────────────────────────────
_ENV_COOKIE = "ML_SESSION_COOKIE"
_ENV_CSRF = "ML_CSRF_TOKEN"

# Antecedência do aviso proativo.
_DIAS_AVISO = ajustes.numero("ML_AVISO_EXPIRACAO_DIAS", 3)


class SessaoExpirada(Exception):
    """
    A sessão provisionada não está mais autenticada.

    Falha de CREDENCIAL, não de conversão: nenhuma outra tentativa
    e nenhuma outra URL resolvem. Exige reprovisionamento manual.
    """


class SessaoAusente(Exception):
    """Nenhuma credencial provisionada no ambiente."""


@dataclass(frozen=True)
class Credencial:
    """
    Material de autenticação para uma chamada.

    NUNCA é logada. O `__repr__` é sobrescrito porque um dataclass
    normal imprimiria o cookie inteiro em qualquer traceback.
    """
    cookie: str
    csrf: str

    def __repr__(self) -> str:
        return (
            f"Credencial(cookie=<{len(self.cookie)} chars>, "
            f"csrf=<{len(self.csrf)} chars>)"
        )


# ── Estado do módulo — fonte única de verdade ─────────────────────
_credencial: Optional[Credencial] = None
_expirada = False
_carregada_em = 0.0
_geracao = 0
_lock: Optional[asyncio.Lock] = None
_avisou_proximidade = False


def _obter_lock() -> asyncio.Lock:
    """Lock preguiçoso, instanciado no loop correto."""
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


def _limpar(valor: str) -> str:
    """
    Remove quebras de linha e caracteres de controle.

    Painéis de variáveis inserem quebras no valor colado, e a
    biblioteca HTTP recusa um header com `\\n` antes da requisição
    sair — falha de transporte que não diz nada sobre a sessão.
    """
    if not valor:
        return ""
    limpo = re.sub(r"[\r\n\t]+", "", valor)
    return "".join(c for c in limpo if ord(c) >= 32 or c == " ").strip()


def _decodificar(bruto: str) -> str:
    """
    Aceita a credencial em texto cru ou em Base64.

    A extensão de captura que o operador usa entrega Base64 — o
    mesmo formato que AMAZON_COOKIE já recebe. Aceitar os dois faz
    com que a mesma captura sirva às duas plataformas, sem exigir
    conversão manual.

    O teste é estrutural, não por tentativa: um header Cookie tem
    `=` e `;`. Só o que NÃO tem essa forma é candidato a Base64, e
    a decodificação só é aceita se produzir algo que tenha.

    ATENÇÃO ao preparo antes de decodificar. `validate=True` recusa
    qualquer byte fora do alfabeto e exige padding exato, e as duas
    coisas quebram em campo com credencial legítima:

      - painel de variáveis quebra valor longo em linhas, e o `\\n`
        é caractere fora do alfabeto;
      - captura que entrega Base64 sem `=` no fim dispara
        "Incorrect padding".

    Nos dois casos o `except` devolvia o Base64 CRU, que então saía
    no header `Cookie:` e o servidor respondia 401 — falha que
    parece sessão expirada e não é. Por isso o espaço sai e o
    padding é recomposto ANTES da tentativa. `validate=True`
    continua: o que se quer tolerar é forma de transporte, não
    conteúdo inválido.
    """
    bruto = (bruto or "").strip()
    if not bruto:
        return ""
    if ";" in bruto and "=" in bruto:
        return bruto
    candidato = re.sub(r"\s+", "", bruto)
    candidato += "=" * (-len(candidato) % 4)
    try:
        decodificado = base64.b64decode(
            candidato, validate=True).decode("utf-8")
    except Exception:
        return bruto
    # Só troca se o resultado for reconhecível como cookie. Caso
    # contrário devolve o original — decodificar lixo em lixo seria
    # pior que não decodificar.
    if ";" in decodificado and "=" in decodificado:
        return decodificado
    return bruto


# ── Carga ─────────────────────────────────────────────────────────
def carregar() -> Credencial:
    """
    Carrega a credencial do ambiente, uma vez, para a memória.

    Levanta SessaoAusente quando não há credencial provisionada e
    SessaoExpirada quando a sessão já foi marcada como expirada
    nesta execução.
    """
    global _credencial, _carregada_em, _geracao

    if _expirada:
        raise SessaoExpirada("sessão marcada como expirada")

    if _credencial is not None:
        return _credencial

    cookie = _limpar(_decodificar(os.environ.get(_ENV_COOKIE) or ""))
    csrf = _limpar(os.environ.get(_ENV_CSRF) or "")

    if not cookie:
        raise SessaoAusente(f"{_ENV_COOKIE} não definida")
    if not csrf:
        raise SessaoAusente(f"{_ENV_CSRF} não definida")

    _credencial = Credencial(cookie=cookie, csrf=csrf)
    _carregada_em = time.time()
    _geracao += 1

    log_nrm.info(
        f"🛒 ML sessão carregada | cookies={_contar_cookies(cookie)} "
        f"geracao={_geracao}"
    )
    _avaliar_proximidade(cookie)
    return _credencial


def _contar_cookies(cookie: str) -> int:
    """
    Quantidade de cookies UTILIZÁVEIS no header. Nunca devolve valores.

    Conta pares `nome=valor`, que é a mesma regra com que o cliente
    semeia o jar. Contar pedaços separados por `;` media outra coisa:
    um valor quebrado, sem um único `=`, aparecia aqui como
    "cookies=1" e dava a impressão de credencial carregada. O defeito
    só reaparecia três passos adiante, como HTTP 401 — que parece
    sessão expirada e não é.

    Com a contagem certa, `cookies=0` na carga já separa credencial
    MALFORMADA de credencial RECUSADA pelo servidor, antes de gastar
    requisição.
    """
    total = 0
    for parte in (cookie or "").split(";"):
        nome, sep, _valor = parte.strip().partition("=")
        if sep and nome:
            total += 1
    return total


# ── Validade ──────────────────────────────────────────────────────
_RE_EXPIRES = re.compile(r"expires=([^;]+)", re.I)
_RE_MAX_AGE = re.compile(r"max-age=(\d+)", re.I)


def expira_em() -> Optional[float]:
    """
    Segundos até a expiração, quando a credencial informar uma.

    Um header `Cookie` de requisição carrega apenas pares
    nome=valor: `expires` e `max-age` são atributos de `Set-Cookie`,
    da resposta. Portanto, no caso normal, não há validade
    declarada e esta função devolve None.

    None significa "não sei", nunca "não expira". Nenhuma data é
    inventada.
    """
    if _credencial is None:
        return None

    bruto = _credencial.cookie
    achado = _RE_MAX_AGE.search(bruto)
    if achado:
        try:
            idade = float(achado.group(1))
            return max(0.0, idade - (time.time() - _carregada_em))
        except (TypeError, ValueError):
            return None
    return None


def _avaliar_proximidade(cookie: str) -> None:
    """
    Avisa com antecedência quando há validade declarada.

    Silencioso quando não há: um aviso baseado em data inventada
    seria pior que nenhum aviso.
    """
    global _avisou_proximidade
    restante = expira_em()
    if restante is None:
        log_nrm.info(
            "🛒 ML sessão sem validade declarada — expiração será "
            "detectada em uso"
        )
        return
    dias = restante / 86400.0
    if dias <= _DIAS_AVISO and not _avisou_proximidade:
        _avisou_proximidade = True
        log_nrm.warning(
            f"⚠️ 🛒 ML sessão próxima de expirar | ~{dias:.1f} dia(s) "
            f"— renovação manual necessária em breve"
        )


def disponivel() -> bool:
    """
    Verdadeiro se há credencial utilizável agora.

    Consulta barata, sem levantar exceção: permite decidir cedo se
    vale a pena seguir o fluxo.
    """
    if _expirada:
        return False
    if _credencial is not None:
        return True
    return bool(
        (os.environ.get(_ENV_COOKIE) or "").strip()
        and (os.environ.get(_ENV_CSRF) or "").strip()
    )


def expirada() -> bool:
    """Verdadeiro se a sessão foi marcada como expirada."""
    return _expirada


def geracao() -> int:
    """
    Geração corrente da credencial.

    Incrementa a cada carga. Permite que uma tarefa saiba se a
    credencial mudou desde que ela começou — base da proteção de
    concorrência quando houver renovação.
    """
    return _geracao


async def marcar_expirada(motivo: str) -> None:
    """
    Registra que a sessão expirou. NÃO tenta renovar.

    Protegido por lock e idempotente por geração: várias ofertas
    simultâneas detectando expiração produzem UM registro e UM
    alerta, não N.

    Quando houver mecanismo de renovação comprovado, é aqui que ele
    entra — e a proteção de concorrência já estará no lugar.
    """
    global _expirada, _credencial

    async with _obter_lock():
        if _expirada:
            return
        _expirada = True
        _credencial = None
        log_nrm.error(
            f"🛒 ML sessão expirada — renovação manual necessária "
            f"| motivo={motivo}"
        )


def estado() -> dict:
    """
    Retrato do estado da sessão, seguro para log.

    Nenhum campo contém cookie, token ou fragmento de segredo.
    """
    restante = expira_em()
    return {
        "disponivel": disponivel(),
        "carregada": _credencial is not None,
        "expirada": _expirada,
        "geracao": _geracao,
        "expira_em_s": None if restante is None else round(restante),
        "idade_s": (
            round(time.time() - _carregada_em) if _carregada_em else None
        ),
    }


def _resetar_para_teste() -> None:
    """
    Restaura o estado inicial. Uso exclusivo de teste.

    O estado deste módulo é global por desenho — é a fonte única de
    verdade. Testes precisam de um jeito explícito de reiniciá-lo,
    e é melhor que exista uma função nomeada do que cada teste
    manipular as globais por conta própria.
    """
    global _credencial, _expirada, _carregada_em, _geracao
    global _lock, _avisou_proximidade
    _credencial = None
    _expirada = False
    _carregada_em = 0.0
    _geracao = 0
    _lock = None
    _avisou_proximidade = False
