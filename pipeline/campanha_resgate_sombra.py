"""
CAMPANHA POR PÁGINA DE RESGATE — SOMBRA (só registra).

Responsabilidade ÚNICA: para uma mensagem NOVA, reunir do estado os fatos
que a regra pura (pipeline.campanha_resgate) precisa, pedir a decisão e
REGISTRAR o que ela faria: 🎟 [RESGATE_SOMBRA]. Não publica, não edita, não
apaga, não funde, não escreve no banco e não muda a família que o pipeline
escolheu — o retorno é sempre None. Qualquer erro vira uma linha e mais
nada (a publicação segue igual).

Chamada por publicacao._enviar_inner logo depois da regra de cupom sem
código, sob os mesmos locks (ORIGEM → IDENTIDADE), só para mensagem nova.
Custo: nada para quem tem âncora forte ou não leva a uma página de resgate
declarada (a imensa maioria); o resto lê os posts vivos da plataforma.

LINHA (só quando a regra junta, duvida ou mudaria o canal; ids, chaves,
página canônica, nomes e benefícios — nada de texto nem link publicado):
  🎟 [RESGATE_SOMBRA] msg=(chat,id) acao= post: motivo= Δ= pagina=
     hoje=post:<id>|post_novo ident_atual=[…] ident_proposta=[…]
     destino_hoje= destino_previsto= nome=(cand|post) beneficio=(cand|post)
     outros=[posts que também têm a página como destino principal]
"""
from __future__ import annotations

import contextlib
import time
from typing import Optional

import config
from database import (db_exibida, db_get_post, db_ofertas_de_post,
                      db_origens_do_post, db_posts_vivos_com_prefixo)
from database_espelho import db_espelho_get
from logger import log_out
from plataformas import registry
from pipeline import campanha_resgate as regra
from pipeline import cupom_lancamento
from pipeline.ingestao import links_de
from pipeline.resolucao_identidade import eh_identidade_cupom
from pipeline.vida_oferta import VIDA_OFERTA_S

__all__ = ["registrar", "avaliar", "pagina_de_resgate"]

# Espécies fracas: as únicas em que a regra olha (âncora forte tem dono).
_FRACAS = ("cupb", "url", "camp", "cash", "txt")


def pagina_de_resgate(plat: str, canonica: str) -> str:
    """`canonica` quando a plataforma `plat` a declara página de resgate
    de cupons (Plataforma.paginas_resgate); senão ""."""
    if not plat or not canonica:
        return ""
    donos = registry.compor_capacidade("paginas_resgate").get(canonica)
    return canonica if donos and plat in donos else ""


def _publicada_da_ancora(norm) -> str:
    """O link publicado da página da âncora: a 1ª entrada do mapa é a da 1ª
    canônica (normalizacao.resolver_e_afiliar preenche os dois juntos, e
    ancora_url é a 1ª canônica)."""
    return next(iter((getattr(norm, "mapa", None) or {}).values()), "")


def _espelhado(mid: int) -> bool:
    esp = db_espelho_get(mid)
    return esp is not None and esp != 0


def _vivos(plat: str) -> list:
    ids = []
    for especie in _FRACAS:
        for mid in db_posts_vivos_com_prefixo(f"{plat}|{especie}|"):
            if mid not in ids:
                ids.append(mid)
    vivos = []
    for mid in ids:
        est = db_get_post(mid)
        if not est:             # fundido e apagado pelo dono já ficaram fora
            continue            # (db_posts_vivos_com_prefixo, Frente M)
        vivos.append(regra.PostVivo(
            msg_id=mid, plat=est.get("plat") or plat,
            chaves=frozenset(db_exibida(mid)) | frozenset(db_ofertas_de_post(mid)),
            primeiro_link=next(iter(links_de(est.get("texto") or "")), ""),
            perfil=cupom_lancamento.perfil(est.get("texto") or "", plat),
            nascimento=float(est.get("janela_fim") or 0.0) - VIDA_OFERTA_S,
            fontes=frozenset(str(o) for o, _m in db_origens_do_post(mid)),
            espelhado=_espelhado(mid)))
    return vivos


def _par(a, b) -> str:
    return f"({a or '-'}|{b or '-'})"


def avaliar(norm, ofertas, familia_hoje: Optional[int],
            agora: Optional[float] = None):
    """(candidato, posts vivos, decisão) para a mensagem `norm`, ou None
    quando ela está fora da regra (âncora forte ou nenhuma página de
    resgate declarada). Só LÊ o estado; quem decide é a regra pura."""
    plat = getattr(norm, "plat", "") or ""
    chaves = tuple(ofertas or ())
    if not regra.sem_ancora_forte(chaves):
        return None
    pagina = pagina_de_resgate(plat, getattr(norm, "ancora_url", "") or "")
    if not pagina:
        return None
    agora = time.time() if agora is None else agora
    c = regra.Candidato(
        plat=plat, chat=str(getattr(norm, "chat", "")), chaves=chaves,
        pagina=pagina, publicada=_publicada_da_ancora(norm),
        perfil=cupom_lancamento.perfil(getattr(norm, "texto_analise", "") or "", plat),
        familia_hoje=familia_hoje,
        espelho_hoje=(_espelhado(familia_hoje) if familia_hoje is not None
                      else eh_identidade_cupom(chaves)))
    vivos = _vivos(plat)
    return c, vivos, regra.avaliar(c, vivos, agora,
                                   float(config.CUPOM_LIMITE_SUPERIOR_S))


def _beneficio(perfil) -> str:
    return perfil.pct or ",".join(sorted(perfil.valores))


def registrar(norm, ofertas, familia_hoje: Optional[int],
              agora: Optional[float] = None) -> None:
    """Registra o que a regra faria para a mensagem `norm`. Nunca levanta e
    nunca muda nada; sempre devolve None."""
    try:
        r = avaliar(norm, ofertas, familia_hoje, agora)
        if r is None:
            return None
        c, vivos, d = r
        if (d.acao == regra.MANTER and d.destino_previsto == d.destino_hoje
                and not d.outros):
            return None
        p = next((v for v in vivos if v.msg_id == d.post), None)
        delta = f"{d.delta_s / 60:.1f}min" if d.delta_s is not None else "-"
        hoje = f"post:{familia_hoje}" if familia_hoje is not None else "post_novo"
        proposta = sorted(set(c.chaves) | {regra.chave_resgate(c.plat, c.pagina)})
        log_out.info(
            f"🎟 [RESGATE_SOMBRA] msg=({c.chat},{getattr(norm, 'msg_id', '')})"
            f" acao={d.acao} post:{d.post if d.post is not None else '-'}"
            f" motivo={d.motivo} Δ={delta} pagina={c.pagina} hoje={hoje}"
            f" ident_atual={sorted(c.chaves)} ident_proposta={proposta}"
            f" destino_hoje={d.destino_hoje} destino_previsto={d.destino_previsto}"
            f" nome={_par(c.perfil.nome, p.perfil.nome if p else '')}"
            f" beneficio={_par(_beneficio(c.perfil), _beneficio(p.perfil) if p else '')}"
            f" outros={list(d.outros)}")
    except Exception as e:                                 # noqa: BLE001
        # nem o registro do erro pode escapar para a publicação
        with contextlib.suppress(Exception):
            log_out.error(f"❌ [RESGATE_SOMBRA] {type(e).__name__}")
    return None
