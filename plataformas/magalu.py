"""
Plataforma Magalu — Afiliação, encurtamento e fallback.

Fluxo:
  1. Cache RAM (_get_final)        → instantâneo
  2. Cache SQLite (db_get_link)     → ms
  3. Desencurtar (se cutt.ly/etc)   → 1-3s
  4. Afiliar URL (troca/adiciona IDs)
  5. Encurtador próprio leoind.com.br
  6. Cuttly (fallback)
  7. Se ambos falharem: retorna URL longa afiliada + agenda background
     que tenta ENCURTADOR PRÓPRIO + CUTTLY em paralelo, primeiro a
     responder edita a mensagem trocando longo→curto

Características senior:
  - Detecção de colisão de hash (7 → 8 → 9 chars com warning)
  - Cuttly valida APENAS status 7 (sucesso real) + valida URL retornada
  - Semáforo HTTP em todas as chamadas externas
  - Cache RAM atualizado em todos os momentos chave
  - Background com retry escalonado e timeout total
  - Logs detalhados em cada decisão crítica
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import aiohttp

import config
from config import (
    _MGL_PARTNER, _MGL_PROMOTER, _MGL_PID,
    _CUTTLY_KEY, _SHORT_BASE,
)
from database import db_get_link, db_set_link, _db
from globals import _get_final, _set_final
from logger import log_nrm
from pipeline.classificacao import _ENCURTADORES, _classificar_cached, _eh_magalu_url
from pipeline.normalizacao import desencurtar
from plataformas.cleaners import _limpar_params_magalu
from utils.hashes import _gerar_code_magalu
from utils.urls import _netloc, _sanitizar_url


# ─────────────────────────────────────────────────────────────────
# 1. AFILIAÇÃO — troca/adiciona IDs sem quebrar o link
# ─────────────────────────────────────────────────────────────────
def _afiliar_url_magalu(url: str) -> str:
    """
    Adiciona/substitui os IDs de afiliado preservando o resto da URL.
    Não quebra deeplinks porque mantém path e fragment originais.
    """
    p = urlparse(url)
    params = {k: v[0] for k, v in parse_qs(p.query, keep_blank_values=True).items()}
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


# ─────────────────────────────────────────────────────────────────
# 2. ENCURTADOR PRÓPRIO leoind.com.br
#    Detecção de colisão: se o code 7-chars já existir com URL diferente,
#    sobe para 8 chars. Probabilidade de colisão dupla ≈ zero.
# ─────────────────────────────────────────────────────────────────
def _validar_url_afiliada(url: str) -> bool:
    """Verifica que a URL tem todos os IDs antes de aceitar encurtá-la."""
    if _MGL_PARTNER not in url:
        log_nrm.warning("  ⚠️ MGL: partner_id ausente"); return False
    if _MGL_PROMOTER not in url:
        log_nrm.warning("  ⚠️ MGL: promoter_id ausente"); return False
    if "magalu" not in url and "magazineluiza" not in url:
        log_nrm.warning("  ⚠️ MGL: domínio não é Magalu"); return False
    return True


async def _encurtador_proprio_magalu(url_afiliada: str) -> Optional[str]:
    """
    Insere o link no SQLite e retorna a URL curta leoind.com.br/CODE-magalu.
    Trata colisão de hash subindo o tamanho do code (7 → 8 → 9 chars).
    """
    if not _validar_url_afiliada(url_afiliada):
        return None

    # Tenta 7, 8, 9 chars até achar slot livre OU encontrar mesmo URL gravado
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
                        short = f"{_SHORT_BASE}/{code}-magalu"
                        log_nrm.debug(f"  ♻️ MGL próprio (cache DB): {short}")
                        return short
                    # Colisão real: code igual com URL diferente
                    log_nrm.warning(
                        f"  ⚠️ MGL colisão hash {tamanho}c | code={code} "
                        f"existente={row[0][:60]}... novo={url_afiliada[:60]}..."
                    )
                    continue   # tenta próximo tamanho

                # Code livre, grava
                db.execute(
                    "INSERT INTO short_links(code,url,ts) VALUES(?,?,?)",
                    (code, url_afiliada, time.time()),
                )
                short = f"{_SHORT_BASE}/{code}-magalu"
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


# ─────────────────────────────────────────────────────────────────
# 3. CUTTLY — aceita APENAS status 7 (sucesso real)
#    Demais status são erros/avisos que retornariam URL inválida.
# ─────────────────────────────────────────────────────────────────
_CUTTLY_STATUS_OK         = 7    # Link encurtado com sucesso
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
    Em qualquer outro caso, retorna None com log explicativo.
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
        status  = url_obj.get("status")

        if status != _CUTTLY_STATUS_OK:
            descricao = _CUTTLY_STATUS_DESCRICOES.get(status, f"status={status}")
            log_nrm.warning(f"  ⚠️ Cuttly recusou: {descricao}")
            return None

        short_link = url_obj.get("shortLink", "")
        # Validação rigorosa: tem que ser URL https válida e diferente da original
        if (not short_link
                or not short_link.startswith(("https://", "http://"))
                or short_link == url):
            log_nrm.warning(f"  ⚠️ Cuttly retornou URL inválida: {short_link!r}")
            return None

        log_nrm.info(f"  ✅ Cuttly: {short_link}")
        return short_link

    except asyncio.TimeoutError:
        log_nrm.warning("  ⏱ Cuttly: timeout")
    except Exception as e:
        log_nrm.warning(f"  ⚠️ Cuttly: {e}")
    return None


# ─────────────────────────────────────────────────────────────────
# 4. BACKGROUND — tenta os DOIS encurtadores em paralelo
#    Editar a mensagem com a versão curta substituindo a longa no texto.
# ─────────────────────────────────────────────────────────────────
_BG_RETRY_DELAYS = (0, 5, 15, 30)   # imediato, 5s, 15s, 30s
_BG_TIMEOUT_TOTAL = 90.0            # desiste após 90s no total


async def _tentar_encurtar_paralelo(
    url_longa: str, sessao: aiohttp.ClientSession,
) -> Optional[str]:
    """
    Tenta os dois encurtadores em paralelo, primeiro a responder ganha.
    Retorna a URL curta ou None se ambos falharam.
    """
    tarefa_proprio = asyncio.create_task(_encurtador_proprio_magalu(url_longa))
    tarefa_cuttly  = asyncio.create_task(_cuttly(url_longa, sessao))

    try:
        feitos, pendentes = await asyncio.wait(
            {tarefa_proprio, tarefa_cuttly},
            timeout=20.0,
            return_when=asyncio.FIRST_COMPLETED,
        )
        # Coleta primeiro resultado válido
        resultado: Optional[str] = None
        for t in feitos:
            try:
                r = t.result()
                if r:
                    resultado = r
                    break
            except Exception:
                continue

        # Se nenhum dos primeiros respondeu com sucesso, espera os pendentes
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

        # Cancela tarefas pendentes
        for t in pendentes:
            if not t.done():
                t.cancel()
        return resultado

    except Exception as e:
        log_nrm.warning(f"  ⚠️ encurtar_paralelo: {e}")
        return None


def _escapar_md_url(url: str) -> str:
    """
    Igual ao _proteger_url_md de montagem.py — replicado aqui para evitar
    import circular. URLs longas Magalu têm '_' nos parâmetros que o
    Telegram interpreta como itálico em parse_mode='md'.
    """
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
    e edita no grupo. Trata também a versão escapada da URL (com \\_)
    porque o texto enviado pode ter sido escapado para Markdown.
    """
    from utils.helpers import ler_mapa
    from pipeline.publicacao import editar_por_id
    from database import db_get_estado, db_set_estado
    from config import _EXECUTOR

    loop = asyncio.get_running_loop()
    mp   = await loop.run_in_executor(_EXECUTOR, ler_mapa)
    id_d = mp.get(str(msg_id_origem))
    if not id_d:
        log_nrm.debug(f"  bg edit: msg_id={msg_id_origem} não no mapa")
        return False

    # Lê estado atual da oferta
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

    # Substituição cirúrgica: tanto versão crua quanto versão escapada
    texto_novo = texto_atual.replace(url_longa, url_curta)
    if texto_novo == texto_atual:
        # Tenta a versão escapada para Markdown
        texto_novo = texto_atual.replace(_escapar_md_url(url_longa), url_curta)

    if texto_novo == texto_atual:
        # URL longa não encontrada no texto (pode ter sido editado por outro grupo)
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
        log_nrm.info(
            f"  ✅ bg edit OK | msg={msg_id_origem} → {url_curta}"
        )
        return True

    log_nrm.warning(f"  ⚠️ bg edit: editar_por_id falhou | msg={msg_id_origem}")
    return False


async def _cuttly_background(url_longa: str, msg_id_origem: int) -> None:
    """
    Background task que tenta encurtar a URL longa em paralelo (próprio +
    Cuttly) com retries escalonados. Quando algum responder com sucesso,
    atualiza caches e edita a mensagem no grupo trocando longo→curto.

    Mantém o nome '_cuttly_background' por compatibilidade com chamadas
    existentes em outros módulos (ex: publicacao.py).
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
                f"  🔄 bg encurtar tentativa {tentativa}/{len(_BG_RETRY_DELAYS)} "
                f"| msg={msg_id_origem}"
            )
            short = await _tentar_encurtar_paralelo(url_longa, sessao)

            if short:
                # Atualiza caches para próximas ofertas usarem direto a curta
                _set_final(url_longa, short)
                try:
                    db_set_link(url_longa, short, "magalu")
                except Exception as e:
                    log_nrm.warning(f"  ⚠️ bg cache: {e}")

                # Edita mensagem no grupo
                await _editar_msg_com_curto(msg_id_origem, url_longa, short)
                return

        log_nrm.warning(
            f"  ❌ bg encurtar: {len(_BG_RETRY_DELAYS)} tentativas falharam "
            f"— mantém URL longa | msg={msg_id_origem}"
        )

    except asyncio.CancelledError:
        log_nrm.debug(f"  bg encurtar cancelado | msg={msg_id_origem}")
        raise
    except Exception as e:
        log_nrm.warning(
            f"  ⚠️ bg encurtar (exceção): {e} | msg={msg_id_origem}",
            exc_info=True,
        )


# ─────────────────────────────────────────────────────────────────
# 5. WARMUP DO CACHE RAM
#    Carrega últimos N links Magalu do SQLite no startup do bot.
#    Resolve a perda de cache RAM após restart do Railway.
# ─────────────────────────────────────────────────────────────────
async def warmup_cache_magalu(limite: int = 500) -> None:
    """
    Pré-popula o cache RAM com links Magalu já vistos no SQLite.
    Chamado uma vez no startup do bot, em paralelo com outras tarefas.
    """
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


# ─────────────────────────────────────────────────────────────────
# 6. ENTRYPOINT — chamada pelo affiliate_router
# ─────────────────────────────────────────────────────────────────
async def _afiliar_magalu(
    url: str, sessao: aiohttp.ClientSession, msg_id: int = 0,
) -> Optional[str]:
    """
    Pipeline completo de afiliação Magalu.

    Ordem de operações (otimizada para latência):
      1. Cache RAM        — instantâneo
      2. Cache SQLite     — ms
      3. Sanitiza + desencurta (se necessário)
      4. Classifica + valida que é Magalu produto
      5. Aplica IDs de afiliado
      6. Tenta encurtador próprio (instant)
      7. Tenta Cuttly (rede)
      8. Fallback URL longa + agenda background
    """
    url = _sanitizar_url(url)
    log_nrm.debug(f"▶ MGL entrada: {url[:80]}")

    # 1+2. Caches
    cached = _get_final(url)
    if cached:
        log_nrm.debug(f"  ⚡ MGL cache RAM: {cached[:60]}")
        return cached
    cached = db_get_link(url)
    if cached:
        # Promove pra RAM pra próximas serem instantâneas
        _set_final(url, cached)
        log_nrm.debug(f"  💾 MGL cache DB: {cached[:60]}")
        return cached

    # 3. Desencurtar se for cutt.ly / maga.lu / outros encurtadores
    nl = _netloc(url)
    if nl == "cutt.ly" or nl == "maga.lu" or nl in _ENCURTADORES:
        try:
            async with config._SEM_HTTP:
                url_exp = await desencurtar(url, sessao)
            if not _eh_magalu_url(url_exp):
                log_nrm.warning(
                    f"  MGL pós-expand não é Magalu: {_netloc(url_exp)} — descarta"
                )
                return None
            log_nrm.debug(f"  🔓 MGL expandido: {url_exp[:60]}")
            url = url_exp
        except Exception as e:
            log_nrm.error(f"  ❌ MGL desencurtar: {e}")
            return None

    # 4. Classificar + validar
    cl = _classificar_cached(url)
    if cl.plat != "magalu" or cl.tipo == "invalido":
        log_nrm.warning(f"  MGL descartado: plat={cl.plat} tipo={cl.tipo}")
        return None

    # 5. Aplicar IDs
    afiliado = _afiliar_url_magalu(url)
    if not afiliado or not _validar_url_afiliada(afiliado):
        log_nrm.warning("  ⚠️ MGL afiliação inválida")
        return None

    # 6. Encurtador próprio
    short = await _encurtador_proprio_magalu(afiliado)
    if short:
        # Salva nas 3 chaves: original, afiliado e short
        _set_final(url, short)
        _set_final(afiliado, short)
        try:
            db_set_link(url, short, "magalu")
            db_set_link(afiliado, short, "magalu")
        except Exception as e:
            log_nrm.warning(f"  ⚠️ MGL cache write: {e}")
        return short

    # 7. Cuttly
    short = await _cuttly(afiliado, sessao)
    if short:
        _set_final(url, short)
        _set_final(afiliado, short)
        try:
            db_set_link(url, short, "magalu")
            db_set_link(afiliado, short, "magalu")
        except Exception as e:
            log_nrm.warning(f"  ⚠️ MGL cache write: {e}")
        return short

    # 8. Fallback: manda longo + tenta encurtar em background
    log_nrm.warning("  ⚠️ MGL encurtadores falharam → URL longa + bg retry")
    _set_final(url, afiliado)
    try:
        db_set_link(url, afiliado, "magalu")
    except Exception as e:
        log_nrm.warning(f"  ⚠️ MGL cache write: {e}")

    if msg_id:
        # Background não bloqueia o pipeline. Tenta próprio + Cuttly em paralelo
        # com retries escalonados. Quando algum responder, edita a mensagem.
        try:
            asyncio.create_task(_cuttly_background(afiliado, msg_id))
        except Exception as e:
            log_nrm.warning(f"  ⚠️ MGL bg schedule: {e}")

    return afiliado
    
