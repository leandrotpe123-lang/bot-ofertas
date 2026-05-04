"""
Plataforma Magalu — Afiliação, encurtamento e fallback.
Versão 80.1 — Desencurtador agressivo até a alma + validação rigorosa.

═════════════════════════════════════════════════════════════════════
PROBLEMA RESOLVIDO NESTA VERSÃO
═════════════════════════════════════════════════════════════════════
Os grupos profissionais NUNCA mandam URL Magalu nativa. Sempre vem
encurtada por algum desses caminhos:

  • https://cutt.ly/SxuxFfj-magalu      (encurtador genérico)
  • https://maga.lu/abc123              (oficial Magalu)
  • https://divulgador.magalu.com/xyz   (oficial Magalu, novo)
  • https://bit.ly/...                  (qualquer outro)

ANTES, o bot reconhecia 'divulgador.magalu.com' como domínio Magalu
e tentava afiliar SEM desencurtar primeiro. Resultado: trocava os
IDs no link ENCURTADO (que ignora os parâmetros), sem afiliação real.

AGORA: SEMPRE desencurta primeiro até a alma — segue todos os
redirects HTTP, meta-refresh, e até JavaScript redirects, até chegar
em URL final Magalu (magazineluiza.com.br ou magazinevoce.com.br).
SÓ ENTÃO troca os IDs.

═════════════════════════════════════════════════════════════════════
FLUXO COMPLETO
═════════════════════════════════════════════════════════════════════
  1. Cache RAM (_get_final)         → instantâneo
  2. Cache SQLite (db_get_link)     → milissegundos
  3. DESENCURTAR ATÉ A ALMA         → segue todos os redirects
  4. VALIDA URL FINAL Magalu        → magazineluiza/magazinevoce
  5. AFILIA (troca os IDs)          → adiciona partner_id, promoter_id
  6. Encurtador próprio leoind.com.br
  7. Cuttly (fallback)
  8. Se ambos falharem: retorna URL longa afiliada + agenda
     background que tenta os 2 em paralelo e edita a mensagem
     quando algum responder

REGRA ABSOLUTA: Se desencurtar não chegar em URL final Magalu,
                DESCARTA a oferta (não publica sem afiliação).

CARACTERÍSTICAS SENIOR:
  • Detecção de colisão de hash (7 → 8 → 9 chars com warning)
  • Cuttly aceita APENAS status 7 + valida URL retornada
  • Semáforo HTTP em todas as chamadas externas
  • Cache RAM + SQLite atualizados em todos os pontos chave
  • Background com retry escalonado (0/5/15/30s) e timeout 90s
  • Logs detalhados em cada decisão crítica
  • Sufixo '-magalu' REMOVIDO (links ficam mais limpos)
"""
from __future__ import annotations

import asyncio
import random
import re
import time
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import aiohttp

import config
from config import (
    _MGL_PARTNER, _MGL_PROMOTER, _MGL_PID,
    _CUTTLY_KEY, _SHORT_BASE, USER_AGENTS,
)
from database import db_get_link, db_set_link, _db
from globals import _get_final, _set_final
from logger import log_nrm
from plataformas.cleaners import _limpar_params_magalu
from utils.hashes import _gerar_code_magalu
from utils.urls import _netloc, _sanitizar_url


# ═════════════════════════════════════════════════════════════════════
# DOMÍNIOS — separa "encurtadores Magalu" de "URLs FINAIS Magalu"
# ═════════════════════════════════════════════════════════════════════

# URLs FINAIS Magalu — onde os IDs de afiliado FUNCIONAM de verdade.
# Quando o link cai aqui, podemos trocar os IDs e ele vai redirecionar
# pro produto creditando você como afiliado.
_MGL_DOMINIOS_FINAIS = frozenset({
    "magazineluiza.com.br",
    "magazinevoce.com.br",
    "m.magazineluiza.com.br",          # versão mobile
    "sacola.magazineluiza.com.br",     # carrinho
})

# ENCURTADORES Magalu — sempre precisam ser desencurtados antes de afiliar.
# Esses domínios IGNORAM parâmetros adicionados na URL — só seguem o
# código original e redirecionam.
_MGL_ENCURTADORES = frozenset({
    "maga.lu",
    "divulgador.magalu.com",
})

# ENCURTADORES genéricos que podem virar Magalu (cutt.ly, bit.ly, etc.)
# Lista expandida pra cobrir os principais que aparecem nos grupos.
_ENCURTADORES_GENERICOS = frozenset({
    "cutt.ly", "bit.ly", "tinyurl.com", "t.co", "ow.ly", "goo.gl",
    "rb.gy", "is.gd", "tiny.cc", "buff.ly", "short.io", "bl.ink",
    "rebrand.ly", "shorturl.at", "tidd.ly", "encurtador.com.br",
    "lstu.fr", "v.gd", "shorten.com",
})


def _eh_url_final_magalu(url: str) -> bool:
    """Verifica se URL é uma página final Magalu (onde os IDs funcionam)."""
    nl = _netloc(url)
    return any(nl == d or nl.endswith("." + d) for d in _MGL_DOMINIOS_FINAIS)


def _precisa_desencurtar(url: str) -> bool:
    """
    Detecta se a URL é um encurtador (Magalu ou genérico).
    Retorna True se precisa desencurtar antes de afiliar.
    """
    nl = _netloc(url)
    todos = _MGL_ENCURTADORES | _ENCURTADORES_GENERICOS
    if nl in todos:
        return True
    # Subdomínios também
    return any(nl.endswith("." + d) for d in todos)


# ═════════════════════════════════════════════════════════════════════
# 1. DESENCURTADOR AGRESSIVO — segue até a alma
# ═════════════════════════════════════════════════════════════════════

_MAX_REDIRECTS = 15            # profundidade máxima de desencurtamento
_TIMEOUT_HTTP = 15             # segundos por tentativa HTTP


async def _desencurtar_completo(
    url: str, sessao: aiohttp.ClientSession, depth: int = 0,
) -> Optional[str]:
    """
    Desencurta URL recursivamente seguindo:
      1. Redirects HTTP (301/302/303/307/308)
      2. Meta-refresh (<meta http-equiv="refresh" content="...url=X">)
      3. JavaScript (window.location, location.replace, location.href)

    Para quando chegar numa URL que NÃO é encurtador, ou após
    _MAX_REDIRECTS redirects.

    Retorna a URL final, ou None se houve erro irrecuperável.
    """
    if depth >= _MAX_REDIRECTS:
        log_nrm.warning(
            f"  ⚠️ MGL desencurtar: profundidade máxima ({_MAX_REDIRECTS})"
        )
        return url

    url = _sanitizar_url(url)
    if not url.startswith(("http://", "https://")):
        return url

    # Se chegou em URL final OU URL não-encurtador, retorna
    if not _precisa_desencurtar(url):
        return url

    headers = {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Cache-Control":   "no-cache",
    }

    try:
        async with config._SEM_HTTP:
            # GET sempre — alguns encurtadores não respondem corretamente a HEAD
            async with sessao.get(
                url, headers=headers, allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=_TIMEOUT_HTTP),
                max_redirects=20,
            ) as r:
                # 1. URL final após redirects HTTP automáticos
                final_url = str(r.url)

                if final_url != url:
                    log_nrm.debug(
                        f"  🔓 MGL HTTP redirect d={depth}: "
                        f"{_netloc(url)} → {_netloc(final_url)}"
                    )
                    return await _desencurtar_completo(
                        final_url, sessao, depth + 1
                    )

                # 2. Sem redirect HTTP — checa meta-refresh / JS
                try:
                    html = await r.text(errors="ignore")
                except Exception:
                    return final_url

                if len(html) > 800_000:
                    return final_url

                # meta http-equiv="refresh"
                m_refresh = re.search(
                    r'<meta[^>]+http-equiv\s*=\s*["\']?refresh["\']?[^>]*'
                    r'content\s*=\s*["\']?[^"\']*url\s*=\s*([^\s"\'>;]+)',
                    html, re.I,
                )
                if m_refresh:
                    novo = m_refresh.group(1).strip()
                    if novo.startswith("http"):
                        log_nrm.debug(
                            f"  🔓 MGL meta-refresh d={depth}: → {novo[:60]}"
                        )
                        return await _desencurtar_completo(
                            novo, sessao, depth + 1
                        )

                # JavaScript redirects
                for pat in (
                    r'window\.location(?:\.href)?\s*=\s*["\']([^"\']{15,})["\']',
                    r'location\.replace\s*\(\s*["\']([^"\']{15,})["\']\s*\)',
                    r'location\.href\s*=\s*["\']([^"\']{15,})["\']',
                ):
                    m_js = re.search(pat, html)
                    if m_js:
                        novo = m_js.group(1).strip()
                        if novo.startswith("http"):
                            log_nrm.debug(
                                f"  🔓 MGL JS-redirect d={depth}: → {novo[:60]}"
                            )
                            return await _desencurtar_completo(
                                novo, sessao, depth + 1
                            )

                # Não achou redirect — para aqui
                return final_url

    except asyncio.TimeoutError:
        log_nrm.warning(
            f"  ⏱ MGL desencurtar timeout d={depth} url={url[:60]}"
        )
        return None
    except Exception as e:
        log_nrm.warning(
            f"  ⚠️ MGL desencurtar (d={depth}): {e} | url={url[:60]}"
        )
        return None


# ═════════════════════════════════════════════════════════════════════
# 2. AFILIAÇÃO — troca os IDs preservando path e fragment
# ═════════════════════════════════════════════════════════════════════
def _afiliar_url_magalu(url: str) -> str:
    """
    Adiciona/substitui os IDs de afiliado na URL final Magalu.
    Preserva path original (deeplink) e remove parâmetros de outros
    afiliados (limpeza via _limpar_params_magalu).
    """
    p = urlparse(url)
    params = {
        k: v[0] for k, v in parse_qs(p.query, keep_blank_values=True).items()
    }
    params = _limpar_params_magalu(params)
    params.update({
        "partner_id":        _MGL_PARTNER,
        "promoter_id":       _MGL_PROMOTER,
        "utm_source":        "divulgador",
        "utm_medium":        "magalu",
        "utm_campaign":      _MGL_PROMOTER,
        "pid":               _MGL_PID,
        "c":                 _MGL_PROMOTER,
        "af_force_deeplink": "true",
    })
    return urlunparse(p._replace(query=urlencode(params), fragment=""))


def _validar_url_afiliada(url: str) -> bool:
    """
    Valida que a URL afiliada está completa E é uma URL FINAL Magalu.
    Garante que os IDs de afiliação vão funcionar de fato no clique.
    """
    if not _eh_url_final_magalu(url):
        log_nrm.warning(
            f"  ⚠️ MGL: URL afiliada não é URL FINAL Magalu | "
            f"netloc={_netloc(url)}"
        )
        return False
    if _MGL_PARTNER not in url:
        log_nrm.warning("  ⚠️ MGL: partner_id ausente na URL afiliada")
        return False
    if _MGL_PROMOTER not in url:
        log_nrm.warning("  ⚠️ MGL: promoter_id ausente na URL afiliada")
        return False
    return True


# ═════════════════════════════════════════════════════════════════════
# 3. ENCURTADOR PRÓPRIO leoind.com.br
#    Detecção de colisão: se code 7 chars já existir com URL diferente,
#    sobe para 8 chars. Probabilidade de colisão dupla ≈ zero.
#    Sufixo '-magalu' REMOVIDO (links ficam mais limpos).
# ═════════════════════════════════════════════════════════════════════
async def _encurtador_proprio_magalu(url_afiliada: str) -> Optional[str]:
    """
    Insere link no SQLite e retorna URL curta leoind.com.br/CODE.
    Trata colisão de hash subindo o tamanho do code (7 → 8 → 9 chars).
    """
    if not _validar_url_afiliada(url_afiliada):
        return None

    for tamanho in (7, 8, 9):
        code = _gerar_code_magalu(url_afiliada)[:tamanho]
        try:
            with _db() as db:
                row = db.execute(
                    "SELECT url FROM short_links WHERE code=?", (code,)
                ).fetchone()

                if row:
                    if row[0] == url_afiliada:
                        # Mesmo URL já gravado — reaproveita
                        short = f"{_SHORT_BASE}/{code}"
                        log_nrm.debug(f"  ♻️ MGL próprio (cache DB): {short}")
                        return short
                    # Colisão real
                    log_nrm.warning(
                        f"  ⚠️ MGL colisão hash {tamanho}c | code={code}"
                    )
                    continue

                # Code livre, grava
                db.execute(
                    "INSERT INTO short_links(code,url,ts) VALUES(?,?,?)",
                    (code, url_afiliada, time.time()),
                )
                short = f"{_SHORT_BASE}/{code}"
                log_nrm.info(f"  ✅ MGL próprio: {short}")
                return short

        except Exception as e:
            log_nrm.error(f"  ❌ MGL próprio (tamanho={tamanho}): {e}")
            return None

    log_nrm.error(
        f"  ❌ MGL próprio: 3 colisões consecutivas — descarta. "
        f"url={url_afiliada[:80]}..."
    )
    return None


# ═════════════════════════════════════════════════════════════════════
# 4. CUTTLY — aceita APENAS status 7 + valida URL retornada
# ═════════════════════════════════════════════════════════════════════
_CUTTLY_STATUS_OK = 7
_CUTTLY_STATUS_DESCRICOES = {
    1: "link inválido",
    2: "preferência incompatível",
    3: "senha incompatível",
    4: "caractere inválido na URL",
    5: "link bloqueado (spam ou limite atingido)",
    6: "URL já é cutt.ly",
    7: "OK",
}


async def _cuttly(url: str, sessao: aiohttp.ClientSession) -> Optional[str]:
    """
    Encurta via API Cuttly.
    Retorna URL curta SOMENTE se status=7 e shortLink for URL válida.
    """
    if not _CUTTLY_KEY:
        log_nrm.debug("  ⏭ Cuttly: sem CUTTLY_API_KEY configurada")
        return None

    try:
        async with config._SEM_HTTP:
            async with sessao.get(
                "https://cutt.ly/api/api.php",
                params={"key": _CUTTLY_KEY, "short": url},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status != 200:
                    log_nrm.warning(f"  ⚠️ Cuttly HTTP {r.status}")
                    return None
                try:
                    data = await r.json(content_type=None)
                except Exception:
                    log_nrm.warning("  ⚠️ Cuttly: resposta não é JSON")
                    return None

        url_obj = data.get("url") or {}
        status = url_obj.get("status")

        if status != _CUTTLY_STATUS_OK:
            descricao = _CUTTLY_STATUS_DESCRICOES.get(
                status, f"status={status}"
            )
            log_nrm.warning(f"  ⚠️ Cuttly recusou: {descricao}")
            return None

        short_link = url_obj.get("shortLink", "")
        if (not short_link
                or not short_link.startswith(("https://", "http://"))
                or short_link == url):
            log_nrm.warning(
                f"  ⚠️ Cuttly retornou URL inválida: {short_link!r}"
            )
            return None

        log_nrm.info(f"  ✅ Cuttly: {short_link}")
        return short_link

    except asyncio.TimeoutError:
        log_nrm.warning("  ⏱ Cuttly: timeout")
    except Exception as e:
        log_nrm.warning(f"  ⚠️ Cuttly: {e}")
    return None


# ═════════════════════════════════════════════════════════════════════
# 5. BACKGROUND — tenta os 2 encurtadores em paralelo
#    Quando algum responder, edita a mensagem trocando longo→curto.
# ═════════════════════════════════════════════════════════════════════
_BG_RETRY_DELAYS = (0, 5, 15, 30)   # imediato, 5s, 15s, 30s
_BG_TIMEOUT_TOTAL = 90.0            # desiste após 90s no total


async def _tentar_encurtar_paralelo(
    url_longa: str, sessao: aiohttp.ClientSession,
) -> Optional[str]:
    """
    Tenta os 2 encurtadores em paralelo. Primeiro a responder ganha.
    Cancela o outro pra economizar quota.
    """
    tarefa_proprio = asyncio.create_task(_encurtador_proprio_magalu(url_longa))
    tarefa_cuttly = asyncio.create_task(_cuttly(url_longa, sessao))

    try:
        feitos, pendentes = await asyncio.wait(
            {tarefa_proprio, tarefa_cuttly},
            timeout=20.0,
            return_when=asyncio.FIRST_COMPLETED,
        )
        resultado: Optional[str] = None
        for t in feitos:
            try:
                r = t.result()
                if r:
                    resultado = r
                    break
            except Exception:
                continue

        if not resultado and pendentes:
            try:
                feitos2, _ = await asyncio.wait(
                    pendentes, timeout=15.0,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in feitos2:
                    try:
                        r = t.result()
                        if r:
                            resultado = r
                            break
                    except Exception:
                        continue
            except Exception:
                pass

        for t in pendentes:
            if not t.done():
                t.cancel()
        return resultado

    except Exception as e:
        log_nrm.warning(f"  ⚠️ encurtar_paralelo: {e}")
        return None


def _escapar_md_url(url: str) -> str:
    """Replica a lógica de montagem.py para evitar import circular."""
    return (url.replace('\\', '\\\\')
               .replace('_',  '\\_')
               .replace('*',  '\\*')
               .replace('`',  '\\`')
               .replace('[',  '\\['))


async def _editar_msg_com_curto(
    msg_id_origem: int, url_longa: str, url_curta: str,
) -> bool:
    """
    Substitui url_longa por url_curta no texto da mensagem publicada
    e edita no grupo. Trata também a versão escapada (Markdown).
    """
    from utils.helpers import ler_mapa
    from pipeline.publicacao import editar_por_id
    from database import db_set_estado
    from config import _EXECUTOR

    loop = asyncio.get_running_loop()
    mp = await loop.run_in_executor(_EXECUTOR, ler_mapa)
    id_d = mp.get(str(msg_id_origem))
    if not id_d:
        log_nrm.debug(f"  bg edit: msg_id={msg_id_origem} não no mapa")
        return False

    try:
        with _db() as db:
            row = db.execute(
                "SELECT identity, texto, score, plat, lider, "
                "janela_fim, edit_count, shadow_reply_id "
                "FROM oferta_estado WHERE msg_id_dest=?",
                (int(id_d),),
            ).fetchone()
    except Exception as e:
        log_nrm.warning(f"  ⚠️ bg edit: erro lendo estado: {e}")
        return False

    if not row:
        log_nrm.debug(f"  bg edit: estado não encontrado para {id_d}")
        return False

    identity, texto_atual, score, plat, lider, janela_fim, edit_count, shadow_id = row
    texto_atual = texto_atual or ""

    # Tenta substituir versão crua
    texto_novo = texto_atual.replace(url_longa, url_curta)
    # Se não achou, tenta versão escapada Markdown
    if texto_novo == texto_atual:
        texto_novo = texto_atual.replace(_escapar_md_url(url_longa), url_curta)

    if texto_novo == texto_atual:
        log_nrm.debug(
            f"  bg edit: URL longa não está mais no texto | msg={msg_id_origem}"
        )
        return False

    ok = await editar_por_id(int(id_d), texto_novo)
    if ok:
        try:
            db_set_estado(
                identity, int(id_d), score, texto_novo,
                plat, lider or "", janela_fim or 0.0,
                edit_count or 0, shadow_id or 0,
            )
        except Exception as e:
            log_nrm.warning(f"  ⚠️ bg edit: db_set_estado falhou: {e}")
        log_nrm.info(f"  ✅ bg edit OK | msg={msg_id_origem} → {url_curta}")
        return True

    log_nrm.warning(f"  ⚠️ bg edit: editar_por_id falhou | msg={msg_id_origem}")
    return False


async def _cuttly_background(url_longa: str, msg_id_origem: int) -> None:
    """
    Background: tenta encurtar URL longa nos 2 encurtadores em paralelo
    com retries escalonados (0/5/15/30s). Quando algum responder, edita
    a mensagem trocando longo→curto.

    Mantém o nome '_cuttly_background' por compatibilidade com chamadas
    em outros módulos (ex: publicacao.py).
    """
    from globals import _get_session

    inicio = time.monotonic()

    try:
        sessao = await _get_session()

        for tentativa, delay in enumerate(_BG_RETRY_DELAYS, start=1):
            if delay:
                await asyncio.sleep(delay)
            if time.monotonic() - inicio > _BG_TIMEOUT_TOTAL:
                log_nrm.warning(
                    f"  ⏱ bg encurtar: timeout total | msg={msg_id_origem}"
                )
                return

            log_nrm.debug(
                f"  🔄 bg encurtar tentativa {tentativa}/{len(_BG_RETRY_DELAYS)}"
                f" | msg={msg_id_origem}"
            )
            short = await _tentar_encurtar_paralelo(url_longa, sessao)

            if short:
                # Atualiza caches pra próximas ofertas usarem direto a curta
                _set_final(url_longa, short)
                try:
                    db_set_link(url_longa, short, "magalu")
                except Exception as e:
                    log_nrm.warning(f"  ⚠️ bg cache: {e}")
                # Edita mensagem no grupo destino
                await _editar_msg_com_curto(msg_id_origem, url_longa, short)
                return

        log_nrm.warning(
            f"  ❌ bg encurtar: {len(_BG_RETRY_DELAYS)} tentativas falharam "
            f"| msg={msg_id_origem}"
        )

    except asyncio.CancelledError:
        log_nrm.debug(f"  bg encurtar cancelado | msg={msg_id_origem}")
        raise
    except Exception as e:
        log_nrm.warning(
            f"  ⚠️ bg encurtar (exceção): {e} | msg={msg_id_origem}",
            exc_info=True,
        )


# ═════════════════════════════════════════════════════════════════════
# 6. WARMUP DO CACHE RAM (no startup)
# ═════════════════════════════════════════════════════════════════════
async def warmup_cache_magalu(limite: int = 500) -> None:
    """Pré-popula o cache RAM com links Magalu já vistos no SQLite."""
    try:
        with _db() as db:
            rows = db.execute(
                "SELECT url_orig, url_conv FROM links_cache "
                "WHERE plat=? ORDER BY ts DESC LIMIT ?",
                ("magalu", limite),
            ).fetchall()
        for url_orig, url_conv in rows:
            if url_orig and url_conv:
                _set_final(url_orig, url_conv)
        log_nrm.info(f"🔥 Warmup MGL: {len(rows)} links carregados pra RAM")
    except Exception as e:
        log_nrm.warning(f"⚠️ warmup_cache_magalu: {e}")


# ═════════════════════════════════════════════════════════════════════
# 7. ENTRYPOINT — chamado pelo affiliate_router
# ═════════════════════════════════════════════════════════════════════
async def _afiliar_magalu(
    url: str, sessao: aiohttp.ClientSession, msg_id: int = 0,
) -> Optional[str]:
    """
    Pipeline completo de afiliação Magalu.

    Ordem:
      1. Cache RAM        — instantâneo
      2. Cache SQLite     — milissegundos
      3. Desencurta SEMPRE até a alma (se URL atual é encurtador)
      4. Valida que chegou em URL FINAL Magalu
      5. Aplica IDs de afiliado (troca os IDs pelos seus)
      6. Encurtador próprio leoind.com.br
      7. Cuttly (fallback)
      8. Se ambos falharem: retorna URL longa afiliada + agenda background

    Retorna None se a oferta não puder ser afiliada (Opção A: descarta).
    """
    url_entrada = _sanitizar_url(url)
    log_nrm.debug(f"▶ MGL entrada: {url_entrada[:80]}")

    # ── 1+2. Caches ──────────────────────────────────────────────
    cached = _get_final(url_entrada)
    if cached:
        log_nrm.debug(f"  ⚡ MGL cache RAM: {cached[:60]}")
        return cached
    cached = db_get_link(url_entrada)
    if cached:
        _set_final(url_entrada, cached)
        log_nrm.debug(f"  💾 MGL cache DB: {cached[:60]}")
        return cached

    # ── 3. DESENCURTA ATÉ A ALMA ─────────────────────────────────
    # Os grupos profissionais NUNCA mandam URL Magalu nativa — sempre
    # vem encurtada por algum encurtador (Magalu ou genérico). Logo,
    # forçar desencurtar é a estratégia correta.
    url_final: Optional[str] = url_entrada
    if _precisa_desencurtar(url_entrada):
        log_nrm.debug(f"  🔄 MGL desencurtando: {_netloc(url_entrada)}")
        url_final = await _desencurtar_completo(url_entrada, sessao)
        if not url_final:
            log_nrm.warning(
                f"  ❌ MGL: desencurtar falhou — descarta | {url_entrada[:60]}"
            )
            return None
        log_nrm.debug(f"  🎯 MGL desencurtado: {url_final[:80]}")

    # ── 4. VALIDA URL FINAL ──────────────────────────────────────
    # Tem que estar em magazineluiza.com.br ou magazinevoce.com.br
    if not _eh_url_final_magalu(url_final):
        log_nrm.warning(
            f"  ❌ MGL: URL final não é Magalu nativa "
            f"({_netloc(url_final)}) — descarta"
        )
        return None

    # ── 5. APLICA IDs DE AFILIADO ────────────────────────────────
    afiliado = _afiliar_url_magalu(url_final)
    if not _validar_url_afiliada(afiliado):
        log_nrm.warning("  ❌ MGL: afiliação inválida — descarta")
        return None

    log_nrm.debug(f"  🏷  MGL afiliado: {afiliado[:80]}")

    # ── 6. ENCURTADOR PRÓPRIO ────────────────────────────────────
    short = await _encurtador_proprio_magalu(afiliado)
    if short:
        # Salva nas chaves: original (encurtada), final e afiliada
        _set_final(url_entrada, short)
        _set_final(url_final, short)
        _set_final(afiliado, short)
        try:
            db_set_link(url_entrada, short, "magalu")
            db_set_link(url_final, short, "magalu")
            db_set_link(afiliado, short, "magalu")
        except Exception as e:
            log_nrm.warning(f"  ⚠️ MGL cache write: {e}")
        return short

    # ── 7. CUTTLY (fallback) ─────────────────────────────────────
    short = await _cuttly(afiliado, sessao)
    if short:
        _set_final(url_entrada, short)
        _set_final(url_final, short)
        _set_final(afiliado, short)
        try:
            db_set_link(url_entrada, short, "magalu")
            db_set_link(url_final, short, "magalu")
            db_set_link(afiliado, short, "magalu")
        except Exception as e:
            log_nrm.warning(f"  ⚠️ MGL cache write: {e}")
        return short

    # ── 8. FALLBACK: URL longa afiliada + background retry ───────
    # Manda a URL longa MAS JÁ AFILIADA (com seus IDs). Você não perde
    # a afiliação. Em background, tenta encurtar e edita a mensagem
    # trocando longo→curto quando algum encurtador responder.
    log_nrm.warning("  ⚠️ MGL: encurtadores falharam → URL longa afiliada + bg retry")
    _set_final(url_entrada, afiliado)
    _set_final(url_final, afiliado)
    try:
        db_set_link(url_entrada, afiliado, "magalu")
        db_set_link(url_final, afiliado, "magalu")
    except Exception as e:
        log_nrm.warning(f"  ⚠️ MGL cache write: {e}")

    if msg_id:
        try:
            asyncio.create_task(_cuttly_background(afiliado, msg_id))
        except Exception as e:
            log_nrm.warning(f"  ⚠️ MGL bg schedule: {e}")

    return afiliado
