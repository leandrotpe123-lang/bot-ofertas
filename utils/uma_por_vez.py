"""
Mecanismo — EXCLUSÃO MÚTUA POR CHAVE.

Responsabilidade ÚNICA: garantir que, para uma dada chave, no máximo
UMA operação esteja em execução POR VEZ. Chaves diferentes seguem
100% independentes.

═══════════════════════════════════════════════════════════════════
O QUE ESTE MECANISMO NÃO É
═══════════════════════════════════════════════════════════════════
NÃO É CACHE. Não guarda, não reaproveita e não expira resultado.

NÃO COMPARTILHA RESULTADO. Quem espera recalcula a operação por
conta própria. Um chamador NUNCA recebe o valor produzido para
outro. Compartilhar resultado sob uma chave normalizada quebraria
plataformas cuja afiliação depende de parâmetros que a chave
descarta — caso verificado em produção: a Netshoes usa utm_campaign,
utm_term e utm_content como credenciais de afiliado, e esses três
são justamente removidos por utils.urls._cache_key. Duas URLs com a
mesma chave produzem ali resultados semanticamente opostos: uma
monetizada, a outra intacta.

NÃO É SINGLE-FLIGHT. Single-flight garante UMA execução; este
mecanismo garante execução NÃO-SIMULTÂNEA. São garantias diferentes,
e a diferença é o que preserva a soberania das plataformas.

NÃO DECIDE SE DUAS URLS SÃO SEMANTICAMENTE IGUAIS. Não conhece URL,
plataforma, cache, identidade nem pipeline.

NÃO PARTICIPA da ordem de locks ORIGEM → IDENTIDADE → POST. É
adquirido e liberado inteiramente dentro da normalização, antes de
qualquer lock de pipeline existir. Quem espera aqui não segura lock
nenhum, nem semáforo de HTTP, nem semáforo de envio.

NÃO É REENTRANTE. asyncio.Lock não é reentrante: chamar uma_por_vez
com a MESMA chave de dentro de uma operação já sob essa chave é
auto-deadlock. Proibido por contrato. Chaves diferentes aninhadas
são seguras.

═══════════════════════════════════════════════════════════════════
A CHAVE
═══════════════════════════════════════════════════════════════════
A chave é OPACA. É apenas a unidade de exclusão escolhida pelo
chamador. Quem define o significado é quem chama. Chave larga demais
causa serialização desnecessária; chave estreita demais perde
deduplicação. NUNCA causa resultado errado — porque o resultado
nunca é compartilhado.

INVARIANTE DE CONTRATO (regra de engenharia, não verificada por
código): para os adapters atuais — amazon, magalu, netshoes, shopee
— nenhum parâmetro removido por utils.urls._cache_key é utilizado
para decidir I/O externo. Toda decisão de rede é tomada por host.
Se uma nova plataforma passar a depender de algum desses parâmetros
para definir trabalho externo, a chave de exclusão do chamador deve
ser revista ANTES de a plataforma ser incorporada. Violar isso não
produz resultado errado — produz contenção desnecessária.

═══════════════════════════════════════════════════════════════════
POR QUE NÃO HÁ MUTEX GLOBAL
═══════════════════════════════════════════════════════════════════
As duas seções que tocam o dicionário compartilhado não contêm
nenhum await. Num event loop de thread única, uma corrotina só cede
o controle num await; logo essas seções são atômicas e, além disso,
ininterruptíveis por cancelamento (CancelledError só é entregue em
pontos de await). Isso dispensa qualquer trava sobre o pool.

pipeline.exclusao precisa de um mutex global porque a sua varredura
de higiene é longa. Aqui não existe varredura: a entrada é removida
por contagem de participantes, na mesma seção atômica em que o
contador zera. Sem TTL, sem LRU, sem tarefa de fundo.

GARANTIA: uma chave → uma entrada → um lock. Nunca coexistem duas
entradas para a mesma chave, porque a entrada existe no dicionário
se e somente se o seu contador é maior que zero.

MEMÓRIA: uma entrada vive apenas enquanto alguém está dentro dela.
Em repouso, em_execucao() devolve 0.
"""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict


class _Entrada:
    """Lock de uma chave e a contagem de participantes vivos nela."""

    __slots__ = ("lock", "n")

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.n = 0


# Único estado compartilhado do módulo. Tocado exclusivamente nas
# duas seções síncronas de uma_por_vez.
_entradas: Dict[str, _Entrada] = {}


async def uma_por_vez(
    chave: str,
    operacao: Callable[..., Awaitable[Any]],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    Executa `operacao(*args, **kwargs)` garantindo que, para `chave`,
    no máximo uma execução ocorra por vez.

    Devolve exatamente o que a operação devolver, para ESTE chamador.
    Exceções da operação propagam apenas para quem as provocou.

    Operações que não cedem o event loop não sofrem contenção alguma:
    o líder conclui — inclusive a remoção da entrada — antes de
    qualquer outra tarefa executar.
    """
    # ── seção síncrona de entrada — NENHUM await ──────────────────
    entrada = _entradas.get(chave)
    if entrada is None:
        entrada = _entradas[chave] = _Entrada()
    entrada.n += 1
    # ── fim da seção síncrona de entrada ──────────────────────────
    try:
        async with entrada.lock:
            return await operacao(*args, **kwargs)
    finally:
        # ── seção síncrona de saída — NENHUM await ────────────────
        entrada.n -= 1
        if entrada.n == 0:
            _entradas.pop(chave, None)
        # ── fim da seção síncrona de saída ────────────────────────


def em_execucao() -> int:
    """
    Número de chaves com participantes vivos.

    Consumidor único: a rede de testes. Não há log, métrica nem
    contador de produção neste módulo.
    """
    return len(_entradas)
