"""
Camada 5 — Montagem: texto formatado, imagem e dataclass MensagemMontada.

═══════════════════════════════════════════════════════════════════
v80.4 — Auditoria sênior aplicada
═══════════════════════════════════════════════════════════════════
Cirurgias incluídas:
  • Cirurgia 6 (Bug #42)  — _crases aceita cupom começando com dígito
  • Cirurgia 7 (Bug #44)  — nunca publica URL bruta sem afiliação
  • Cirurgia 19 (Bugs #43+#45) — usa _sanitizar_url em vez de rstrip
═══════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import asyncio
import io
import re
from dataclasses import dataclass
from typing import Dict, List, Optional

from logger import log_enr
from pipeline.normalizacao import MensagemNormalizada, _KW_CUPOM, _FALSO_CUPOM, _tem_emoji, _KW_EVENTO
from utils.urls import _netloc, _sanitizar_url


# ── Dataclass de saída ────────────────────────────────────────────
@dataclass
class MensagemMontada:
    msg_id:        int
    chat:          str
    plat:          str
    sku:           str
    texto:         str
    imagem:        object
    mapa:          Dict[str, str]
    msg_id_origem: int


# ── Emojis por categoria ──────────────────────────────────────────
_EMJ: Dict[str, List[str]] = {
    "titulo_oferta": ["🔥"], "titulo_cupom": ["🚨"], "titulo_evento": ["⚠️"],
    "desconto":      ["🎟"], "preco_produto": ["💵"], "resgate":       ["⭐"],
    "carrinho":      ["🛒"], "frete":         ["🚚", "📦"],
    "multi_item":    ["🔹"], "link_prod":     ["✅"],
}
_EMJ_IDX: Dict[str, int] = {k: 0 for k in _EMJ}

def _prox_emoji(cat: str) -> str:
    lst = _EMJ[cat]; idx = _EMJ_IDX[cat]; e = lst[idx % len(lst)]
    _EMJ_IDX[cat] = (idx + 1) % len(lst); return e


# ── Regex de formatação ───────────────────────────────────────────
_KW_PRECO    = re.compile(r'R\$\s?[\d.,]+', re.I)
_KW_DESCONTO = re.compile(r'\b(?:\d+\s*%\s*off|r\$\s*[\d.,]+\s*off|off\s*r\$|limite\s*r\$)\b', re.I)
_KW_FRETE    = re.compile(r'\b(?:frete\s+gr[aá]t|entrega\s+gr[aá]t|sem\s+frete|frete\s+0)\b', re.I)
_KW_RESGATE  = re.compile(r'\b(?:resgate|acesse|ative|lista|use\s+o\s+cupom)\b', re.I)
_KW_CARRINHO = re.compile(r'\b(?:carrinho|cart)\b', re.I)
_KW_LINK_PROD= re.compile(r'\b(?:link\s+produto|link\s+oferta|link\s+lista)\b', re.I)
_RE_LIXO_PREF= re.compile(r'^\s*(?:::?\s*ML|[-–]\s*ML|ML\s*:|[-:•|]\s*(?:ML|MG|AMZ)\s*[-:•]?)\s*', re.I)
_RE_ANUNCIO  = re.compile(r'^\s*[-#]?\s*(?:an[uú]ncio|publicidade|patrocinado)\s*$', re.I)
_RE_URL_RENDER = re.compile(r'https?://[^\s\)\]>,"\'<\u200b\u200c]+')


# ─────────────────────────────────────────────────────────────────
# Domínios de plataformas que REQUEREM afiliação
# Se URL desses domínios não está no mapa, a oferta NÃO publica
# essa URL (Cirurgia 7 — Bug #44).
# ─────────────────────────────────────────────────────────────────
_DOMINIOS_REQUEREM_AFILIACAO = frozenset({
    "amazon.com.br", "amazon.com", "amzn.to", "amzn.com", "a.co", "amzlink.to",
    "shopee.com.br", "s.shopee.com.br", "shope.ee", "shopee.com",
    "magazineluiza.com.br", "magazinevoce.com.br",
    "maga.lu", "divulgador.magalu.com",
})


def _eh_dominio_que_requer_afiliacao(url: str) -> bool:
    """True se URL é de plataforma que exige afiliação (Amazon/Shopee/Magalu)."""
    nl = _netloc(url)
    if nl in _DOMINIOS_REQUEREM_AFILIACAO:
        return True
    return any(nl.endswith("." + d) for d in _DOMINIOS_REQUEREM_AFILIACAO)


def _contar_produtos(texto: str) -> int:
    return sum(1 for l in texto.splitlines() if _KW_PRECO.search(l))


def _eh_linha_cupom(linha: str) -> bool:
    return bool(_KW_DESCONTO.search(linha) or _KW_CUPOM.search(linha))


def _proteger_url_md(url: str) -> str:
    """
    Protege URLs contra interpretação de Markdown do Telegram.
    URLs longas Magalu/Amazon contêm '_' em parâmetros (partner_id,
    promoter_id, utm_source) que parse_mode='md' interpreta como
    itálico, quebrando o link.
    """
    return (url.replace('\\', '\\\\')
               .replace('_', '\\_')
               .replace('*', '\\*')
               .replace('`', '\\`')
               .replace('[', '\\['))


def _emoji_linha(linha: str, eh_titulo: bool, is_multi: bool = False) -> Optional[str]:
    if _tem_emoji(linha): return None
    if eh_titulo:
        if _KW_EVENTO.search(linha): return _prox_emoji("titulo_evento")
        if _KW_CUPOM.search(linha):  return _prox_emoji("titulo_cupom")
        return _prox_emoji("titulo_oferta")
    if is_multi and _KW_PRECO.search(linha): return "🔹"
    if _KW_FRETE.search(linha):      return _prox_emoji("frete")
    if _KW_CARRINHO.search(linha):   return _prox_emoji("carrinho")
    if _KW_LINK_PROD.search(linha):  return _prox_emoji("link_prod")
    if _KW_RESGATE.search(linha):    return _prox_emoji("resgate")
    if _KW_PRECO.search(linha):
        return _prox_emoji("desconto") if _eh_linha_cupom(linha) else _prox_emoji("preco_produto")
    if _eh_linha_cupom(linha): return _prox_emoji("desconto")
    return None


def _crases(linha: str, eh_titulo: bool = False) -> str:
    """
    Envolve cupons em crases pra preservar formatação do Telegram.

    CIRURGIA 6 (Bug #42): regex `[A-Z0-9]` (era `[A-Z]`) — agora aceita
    cupons começando com dígito (5D05PRAVOCE, 5D05CHEG0U, 555H0PPR3C0).
    Antes esses cupons saíam SEM crases e o markdown podia quebrá-los.
    """
    if "http" in linha or eh_titulo or "`" in linha: return linha
    if not _KW_CUPOM.search(linha): return linha

    def _sub(m: re.Match) -> str:
        c = m.group(0)
        if c.upper() in _FALSO_CUPOM or len(c) < 4: return c
        return f"`{c}`"

    # Aceita dígito inicial — cupom como 5D05PRAVOCE agora é protegido
    return re.sub(r'\b([A-Z0-9][A-Z0-9_-]{4,20})\b', _sub, linha)


# ─────────────────────────────────────────────────────────────────
# Montagem de texto
# ─────────────────────────────────────────────────────────────────
def montar_texto(norm: MensagemNormalizada) -> str:
    """
    Monta o texto final a partir do normalizado.

    CIRURGIA 7 (Bug #44): NUNCA publica URL bruta de plataforma quando
    afiliação falhou. Antes: `mapa.get(uc, uc)` retornava URL crua se
    não havia conversão — quebrava a regra de ouro "nunca usar URL
    bruta como fallback de publicação". Agora: omite a linha (ou
    palavra) se a URL é de plataforma que requer afiliação E não está
    no mapa.

    CIRURGIA 19 (Bugs #43+#45): usa _sanitizar_url consistente com o
    resto do sistema (era rstrip('.,;)>') — perdia '!' e '?').
    """
    mapa     = {**norm.mapa, **{u: u for u in norm.preservar}}
    is_multi = _contar_produtos(norm.texto_limpo) >= 2
    saida: List[str] = []
    primeiro = True
    cupons_vistos: set = set()

    for linha in norm.texto_limpo.split("\n"):
        l = linha.strip()
        if not l: saida.append(""); continue
        if _RE_ANUNCIO.match(l): saida.append(l); continue
        l = _RE_LIXO_PREF.sub("", l).strip()
        if not l: continue

        urls_na_linha = _RE_URL_RENDER.findall(l)
        sem_urls      = _RE_URL_RENDER.sub("", l).strip()

        # ── Linha é só URL(s) ──────────────────────────────────────
        if urls_na_linha and not sem_urls:
            urls_publicaveis: List[str] = []
            for u in urls_na_linha:
                # CIRURGIA 19: sanitização consistente
                uc = _sanitizar_url(u)
                url_final = mapa.get(uc)

                # CIRURGIA 7: regra de ouro
                if url_final is None:
                    if _eh_dominio_que_requer_afiliacao(uc):
                        # URL de plataforma sem afiliação → OMITE
                        log_enr.warning(
                            f"🚫 omitindo URL bruta sem afiliação: {uc[:80]}"
                        )
                        continue
                    # URL não-plataforma — publica como veio
                    url_final = uc

                urls_publicaveis.append(_proteger_url_md(url_final))

            for u_pub in urls_publicaveis:
                saida.append(u_pub)
            continue

        # ── Linha mista — substitui URLs inline sem apagar ──────────
        # Lista de URLs que devem ser removidas (não foram afiliadas)
        urls_para_remover: List[str] = []

        def _sub_url(m: re.Match) -> str:
            u_orig = m.group(0)
            u = _sanitizar_url(u_orig)   # CIRURGIA 19
            url_final = mapa.get(u)
            if url_final is None:
                if _eh_dominio_que_requer_afiliacao(u):
                    # Marca pra remover essa URL da linha
                    urls_para_remover.append(u_orig)
                    log_enr.warning(
                        f"🚫 omitindo URL bruta inline sem afiliação: {u[:80]}"
                    )
                    return ""   # remove inline
                return _proteger_url_md(u)
            return _proteger_url_md(url_final)

        l = _RE_URL_RENDER.sub(_sub_url, l).strip()
        if not l: continue

        # Limpa espaços duplos/triplos resultantes da remoção
        l = re.sub(r'\s+', ' ', l).strip()
        if not l: continue

        # Deduplicação de cupom
        if _KW_CUPOM.search(l):
            cupons_linha = re.findall(r'\b([A-Z0-9][A-Z0-9_-]{3,19})\b', l)
            cupons_novos = [c for c in cupons_linha
                            if c.upper() not in _FALSO_CUPOM and c not in cupons_vistos]
            if cupons_linha and not cupons_novos:
                log_enr.debug(f"🔁 Cupom duplicado suprimido: {l[:60]}")
                continue
            cupons_vistos.update(cupons_novos)

        eh_titulo = primeiro
        l = _crases(l, eh_titulo=eh_titulo)
        if not _tem_emoji(l):
            e = _emoji_linha(l, eh_titulo=eh_titulo, is_multi=is_multi)
            if e: l = f"{e} {l}"
        primeiro = False
        saida.append(l)

    return "\n".join(saida).strip()


# ─────────────────────────────────────────────────────────────────
# Imagens
# ─────────────────────────────────────────────────────────────────
async def preparar_imagem_tg(media_obj) -> Optional[object]:
    """
    Baixa a imagem da mensagem original do Telegram pra buffer em memória.
    Retorna BytesIO pronto pra reenviar via Telethon, ou None se falhar.
    """
    from client import client
    import config

    try:
        buf = io.BytesIO()
        try:
            res = await asyncio.wait_for(
                client.download_media(media_obj, file=buf),
                timeout=config._TIMEOUT_DOWNLOAD_MIDIA,
            )
        except asyncio.TimeoutError:
            log_enr.warning(
                f"⏱ download_media timeout após "
                f"{config._TIMEOUT_DOWNLOAD_MIDIA}s — pulando imagem"
            )
            return None

        if res is None:
            return None
        buf.seek(0)
        if buf.getbuffer().nbytes < 500:
            return None
        buf.name = "imagem.jpg"
        return buf
    except Exception as e:
        log_enr.warning(f"⚠️ download_media: {e}")
        return None


async def _resolver_imagem(norm: MensagemNormalizada) -> object:
    """Resolve a imagem a anexar no post."""
    if norm.tem_midia:
        img = await preparar_imagem_tg(norm.media_obj)
        if img:
            return img
    return None


async def montar(norm: MensagemNormalizada) -> MensagemMontada:
    texto  = montar_texto(norm)
    imagem = await _resolver_imagem(norm)
    return MensagemMontada(
        msg_id=norm.msg_id, chat=norm.chat, plat=norm.plat,
        sku=norm.sku, texto=texto, imagem=imagem,
        mapa=norm.mapa, msg_id_origem=norm.msg_id,
    )
    
