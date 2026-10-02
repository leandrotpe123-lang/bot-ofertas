"""
Frente 8 — Convergência de famílias JÁ NASCIDAS.

POR QUE EXISTE (produção, 29/09):
  18:41 Fada publica 1010SHO → post 24133. 18:47:13 Promotom publica
  MODA15SHO → post 24135 (nada provava equivalência: publicar foi
  certo). 18:47:42 a Fada EDITA e passa a exibir 1010SHO + MODA15SHO:
  a partir daí o 24133 cobre o 24135 inteiro — e o 24135 ficou vivo.
  Mesmo desenho com produtos: P1 → 24137, P2 → 24138, e o Samuel junta
  P1+P2 no 24138 — o 24137 ficou vivo com P1 duplicado.

RESPONSABILIDADE ÚNICA: depois que um post passou a EXIBIR conteúdo
novo (publicação, evolução, sincronização, renascimento), encontrar os
posts vivos que ficaram REDUNDANTES e fundi-los — e só então apagá-los
do destino.

  1. consulta indexada da composição exibida do post e dos vizinhos
     que exibem alguma âncora forte dele;
  2. familia.plano_fusao decide (regra de CONTENÇÃO; pura);
  3. locks de POST em ordem CRESCENTE de msg_id (ordem da casa:
     ORIGEM → IDENTIDADE(sorted) → POST(sorted)); o chamador já tem
     ORIGEM e IDENTIDADE e NÃO segura lock de post nenhum aqui;
  4. RELÊ sob os locks e decide de novo — o plano otimista nunca é
     aplicado sem prova atual;
  5. database.db_fundir_posts — transação atômica;
  6. solta os locks e agenda a remoção física em TASK PRÓPRIA: nenhum
     I/O de Telegram acontece dentro de lock, da lane ou do caminho da
     mensagem que disparou a fusão.

REMOÇÃO FÍSICA:
  · só depois do commit; usa saida.apagar_post (_SEM_ENVIO normal);
  · tenta de novo com espera crescente e honra FloodWait;
  · falhar NÃO desfaz a fusão: o post já está morto e redirecionado no
    banco; o status fica 'falhou' e é retomado no próximo boot.
  · RECUPERAÇÃO NO BOOT: uma consulta local única (retomar_remocoes),
    sem varredura periódica.

NÃO faz:
  - decidir publicar/editar/ignorar (pipeline.decisao)
  - escolher o post parente da mensagem (pipeline.familia)
  - derivar identidade (pipeline.identidade_oferta / resolucao_identidade)

AUTORIDADE ÚNICA DA PERGUNTA "ISSO CRIA DUPLICIDADE ESTRUTURAL?"
  Sincronização, evolução, mídia, score e identidade PRODUZEM estado;
  só este módulo decide o conflito estrutural, em DOIS pontos fixos do
  mesmo fluxo (nunca duas máquinas):
    avaliar()    — ANTES de aplicar uma escrita de conteúdo num post
                   existente: PERMITIR ou BLOQUEAR (Caminho 2).
    consolidar() — DEPOIS de a escrita estar persistida: funde os posts
                   que ficaram redundantes (Caminho 1).
  A primeira publicação de uma oferta sem família viva não passa por
  nenhum dos dois.
"""
from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass, field

from telethon.errors import FloodWaitError

import globals as g
from database import (db_composicoes, db_exibida, db_fundir_posts,
                      db_registrar_conflito, db_remocoes_pendentes,
                      db_set_delete_status, db_vizinhos)
from logger import log_out
from pipeline import espelho_cupons, exclusao, familia, saida

__all__ = ["avaliar", "auditar_composicao", "consolidar", "Veredito", "agendar_remocao",
           "retomar_remocoes"]

# Remoção física: tentativas e espera-base (s) entre elas.
_TENTATIVAS = 3
_ESPERA_BASE_S = 2.0
_FLOOD_MAX_S = 300
# Vizinhança da consolidação: saltos a partir do post escrito.
_SALTOS = 2
# Recuperação no boot: teto de remoções retomadas de uma vez.
_RETOMAR_LIMITE = 100

# Tasks de remoção em voo (referência forte + idempotência por post).
_EM_CURSO: dict = {}


@dataclass
class Veredito:
    """Resposta de avaliar(): a escrita pode ser aplicada?"""
    permitido: bool
    plano: familia.Plano = field(default_factory=familia.Plano)
    motivo: str = ""


def _grupo(alvo: int, chaves_alvo, score_alvo: int, excluir: set,
           agora: float):
    """Composição do alvo + vizinhos que exibem as fortes dele, em até
    dois saltos (quem cobre o vizinho sem tocar o alvo). Só leituras
    indexadas; teto de posts por consulta em database.db_vizinhos."""
    comps = {alvo: set(chaves_alvo)}
    scores = {alvo: score_alvo or 0}
    fronteira = set(familia.fortes(chaves_alvo))
    vistas = set(fronteira)
    for _ in range(_SALTOS):
        viz = db_vizinhos(sorted(fronteira), set(excluir) | set(comps), agora)
        if not viz:
            break
        nova = set()
        for mid, (ch, sc) in viz.items():
            comps[mid], scores[mid] = ch, sc
            nova |= familia.fortes(ch)
        fronteira = nova - vistas
        vistas |= nova
        if not fronteira:
            break
    return comps, scores


def avaliar(alvo: int, exibidas, score: int, *, superar=None,
            chat: str = "", msg_id: int = 0) -> Veredito:
    """VEREDITO ESTRUTURAL — antes de aplicar uma escrita de CONTEÚDO
    (evolução, sincronização, renascimento) no post `alvo`.

    Simula o alvo exibindo `exibidas` e roda a mesma regra pura da
    consolidação. Se sobrar duplicidade envolvendo o alvo sem nenhum
    post redundante que a resolva (nada real cobre a composição sem
    perda), a escrita é BLOQUEADA: o estado válido anterior fica, o
    conflito é registrado. Se o próprio alvo ficar redundante, ou os
    vizinhos ficarem contidos, é PERMITIDA — a consolidação resolve
    depois da aplicação real. Nunca levanta: na dúvida, permite (o
    comportamento de antes desta frente)."""
    # o alvo já entra no grupo com a composição simulada; só o post
    # SUPERADO (renascimento, vira histórico) fica de fora
    excluir = set() if superar is None else {superar}
    return _simular(alvo, alvo, exibidas, score, excluir,
                    "CONFLITO_ESTRUTURAL", chat, msg_id)


def auditar_composicao(alvo: int, candidato, score: int, *,
                       chat: str = "", msg_id: int = 0) -> Veredito:
    """AUDITORIA do bloqueio por COMPOSIÇÃO (REDUZ/PARCIAL de outra
    fonte, decidido em pipeline.decisao): o texto já não entra. Aqui só
    se responde se o conteúdo do candidato ficou SEM representação real —
    simulando-o como um post à parte entre os vivos. Se a simulação não
    deixa conflito (o candidato está coberto pelo que já é exibido, caso
    comum do REDUZ), nada é registrado. Se deixa, o conflito é
    registrado com o post da família como rótulo — o mesmo registro que
    o veredito produziria se a família tivesse escolhido outro post do
    grupo. Não muda decisão nenhuma; nunca levanta."""
    return _simular(-1, alvo, candidato, score, {alvo},
                    "COMPOSICAO_PERDERIA_IDENTIDADE", chat, msg_id,
                    pendencia_basta=True)


def _simular(chave: int, rotulo: int, exibidas, score: int, excluir: set,
             motivo: str, chat: str, msg_id: int, *,
             pendencia_basta: bool = False) -> Veredito:
    try:
        if not familia.fortes(exibidas):
            return Veredito(True)
        agora = time.time()
        comps, scores = _grupo(chave, exibidas, score, excluir, agora)
        if chave != rotulo:
            # o post da família participa com o que EXIBE hoje
            atual = db_composicoes([rotulo], agora)
            if rotulo in atual and rotulo not in comps:
                comps[rotulo], scores[rotulo] = atual[rotulo]
        if len(comps) < 2:
            return Veredito(True)
        plano = familia.plano_fusao(chave, comps, scores)
        if not plano.envolve(chave) and not (
                pendencia_basta and _pendentes(chave, comps)):
            return Veredito(True, plano)
        _registrar_conflito(chave, comps, plano, chat, msg_id,
                            rotulo=rotulo, motivo=motivo)
        return Veredito(False, plano, motivo)
    except Exception as e:                          # noqa: BLE001
        log_out.error(f"❌ avaliar post:{rotulo}: {e}", exc_info=True)
        return Veredito(True)


def _pendentes(alvo, comps) -> set:
    """Fortes que a escrita traria e que NENHUM post vivo exibe (nem o
    próprio alvo hoje): conteúdo real sem representação no canal."""
    atual = familia.fortes(db_exibida(alvo)) if alvo >= 0 else frozenset()
    fora = set().union(*(familia.fortes(c) for m, c in comps.items() if m != alvo))
    return set(familia.fortes(comps[alvo])) - atual - fora


def _registrar_conflito(alvo, comps, plano, chat, msg_id, *, rotulo=None,
                        motivo="CONFLITO_ESTRUTURAL") -> None:
    rotulo = alvo if rotulo is None else rotulo
    f = {m: familia.fortes(c) for m, c in comps.items()}
    envolvidos = sorted({m for x, y, _ in plano.conflitos if alvo in (x, y)
                         for m in (x, y)} - {alvo})
    duplicadas = set().union(*(d for x, y, d in plano.conflitos
                               if alvo in (x, y)))
    exclusivas = set()
    for m in envolvidos:
        exclusivas |= f[m] - f[alvo]
    pendentes = _pendentes(alvo, comps)
    log_out.warning(
        f"⛔ [{motivo}] escrita em post:{rotulo} BLOQUEADA "
        f"(id={msg_id} chat={chat}) | duplicaria={sorted(duplicadas)} "
        f"com post(s) {envolvidos} | exclusivas_que_se_perderiam="
        f"{sorted(exclusivas)} | pendentes={sorted(pendentes)} — estado "
        f"anterior preservado; reavaliado na próxima escrita")
    db_registrar_conflito(rotulo, chat, msg_id, motivo,
                          duplicadas, exclusivas, pendentes)


async def consolidar(escrito: int) -> list:
    """CONSOLIDAÇÃO — depois que `escrito` passou a exibir conteúdo novo
    (persistido). Funde os posts vivos que ficaram redundantes e devolve
    os msg_id fundidos.

    Chamada pela publicação com o msg_id EFETIVO (o novo, se houve
    substituição), dentro dos locks de ORIGEM e IDENTIDADE e FORA de
    qualquer lock de post. Nunca levanta: consolidação é auxiliar e não
    pode derrubar a publicação que já aconteceu."""
    try:
        agora = time.time()
        base = db_composicoes([escrito], agora)
        if escrito not in base:
            return []
        chaves, score = base[escrito]
        comps, scores = _grupo(escrito, chaves, score, {escrito}, agora)
        if len(comps) < 2:
            return []
        plano = familia.plano_fusao(escrito, comps, scores)
        if not plano.fusoes:
            _avisar_pendentes(escrito, plano)
            return []
        envolvidos = sorted(comps)
        fusoes_feitas = []
        async with contextlib.AsyncExitStack() as pilha:
            for mid in envolvidos:                   # ordem CRESCENTE
                await pilha.enter_async_context(await exclusao.lock_post(mid))
            agora = time.time()
            atual = db_composicoes(envolvidos, agora)
            plano = familia.plano_fusao(
                escrito, {m: c for m, (c, _) in atual.items()},
                {m: s for m, (_, s) in atual.items()})
            for perdedor, principal, donos in plano.fusoes:
                if db_fundir_posts(principal, [perdedor], agora, donos):
                    fusoes_feitas.append((perdedor, principal))
        # ── fora de TODOS os locks de post ──
        _avisar_pendentes(escrito, plano)
        for perdedor, principal in fusoes_feitas:
            g.midia_aceita_drop(perdedor)
            ch_p = atual.get(perdedor, (set(), 0))[0]
            ch_s = atual.get(principal, (set(), 0))[0]
            log_out.info(
                f"🧬 [FUSAO] post:{perdedor} → post:{principal} | "
                f"coberto={sorted(familia.fortes(ch_p))} "
                f"sobrevivente={sorted(familia.fortes(ch_s))} "
                f"gatilho=post:{escrito}")
            agendar_remocao(perdedor)
        return [p for p, _ in fusoes_feitas]
    except Exception as e:                          # noqa: BLE001
        log_out.error(f"❌ consolidar post:{escrito}: {e}", exc_info=True)
        return []


def _avisar_pendentes(escrito, plano) -> None:
    for x, y, d in plano.conflitos:
        log_out.warning(
            f"⚠️ [SOBREPOSICAO_PENDENTE] post:{x} × post:{y} exibem "
            f"{sorted(d)} sem post real que cubra os dois (gatilho="
            f"post:{escrito}) — nenhum conteúdo exclusivo é apagado")


def agendar_remocao(msg_id_dest: int) -> None:
    """Agenda a remoção física do post fundido numa task PRÓPRIA —
    nunca aguardada por quem funde. Idempotente por post."""
    t = _EM_CURSO.get(msg_id_dest)
    if t is not None and not t.done():
        return
    t = asyncio.get_running_loop().create_task(_remover(msg_id_dest))
    _EM_CURSO[msg_id_dest] = t
    t.add_done_callback(lambda _t, m=msg_id_dest: _EM_CURSO.pop(m, None)
                        if _EM_CURSO.get(m) is _t else None)


async def _remover(msg_id_dest: int) -> None:
    for tentativa in range(1, _TENTATIVAS + 1):
        if g._encerrando:
            return                          # fica 'pendente' para o boot
        try:
            await saida.apagar_post(msg_id_dest)
            db_set_delete_status(msg_id_dest, "ok")
            log_out.info(f"🗑 [FUSAO_REMOVIDO] post:{msg_id_dest}")
            espelho_cupons.removido(msg_id_dest)
            return
        except asyncio.CancelledError:
            raise
        except FloodWaitError as e:
            espera = min(float(e.seconds), _FLOOD_MAX_S)
        except Exception as e:                      # noqa: BLE001
            log_out.warning(
                f"⚠️ [FUSAO_REMOCAO] post:{msg_id_dest} "
                f"tentativa {tentativa}/{_TENTATIVAS}: {type(e).__name__}: {e}")
            espera = _ESPERA_BASE_S * (2 ** (tentativa - 1))
        if tentativa < _TENTATIVAS:
            await asyncio.sleep(espera)
    db_set_delete_status(msg_id_dest, "falhou")
    log_out.warning(
        f"⚠️ [FUSAO_REMOCAO_FALHOU] post:{msg_id_dest} — a fusão lógica "
        f"permanece; remoção retomada no próximo boot")


def retomar_remocoes() -> int:
    """BOOT — uma consulta local única: reagenda a remoção dos posts
    fundidos com status 'pendente' ou 'falhou'. Não é varredura
    periódica: roda uma vez, no boot. Devolve quantas agendou."""
    ids = db_remocoes_pendentes(_RETOMAR_LIMITE)
    for mid in ids:
        agendar_remocao(mid)
    if ids:
        log_out.info(f"🗑 [FUSAO_RETOMADA] {len(ids)} remoção(ões) "
                     f"pendente(s) reagendada(s): {ids}")
    return len(ids)
