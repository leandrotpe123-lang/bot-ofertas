"""Camada 5 — Montagem: texto formatado, imagem e dataclass MensagemMontada."""
from __future__ import annotations
import asyncio
import io
import json
import re
from dataclasses import dataclass
from typing import Dict, List, Optional

import aiohttp
from bs4 import BeautifulSoup

from config import USER_AGENTS
from globals import _get_session
from logger import log_enr
from pipeline.normalizacao import MensagemNormalizada, _KW_CUPOM, _FALSO_CUPOM, _tem_emoji, _KW_EVENTO

import random

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


def _contar_produtos(texto: str) -> int:
    return sum(1 for l in texto.splitlines() if _KW_PRECO.search(l))

def _eh_linha_cupom(linha: str) -> bool:
    return bool(_KW_DESCONTO.search(linha) or _KW_CUPOM.search(linha))

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
    if "http" in linha or eh_titulo or "`" in linha: return linha
    if not _KW_CUPOM.search(linha): return linha
    def _sub(m: re.Match) -> str:
        c = m.group(0)
        if c in _FALSO_CUPOM or len(c) < 4: return c
        return f"`{c}`"
    return re.sub(r'\b([A-Z][A-Z0-9_-]{4,20})\b', _sub, linha)

# ── Montagem de texto ─────────────────────────────────────────────
def montar_texto(norm: MensagemNormalizada) -> str:
    mapa     = {**norm.mapa, **{u: u for u in norm.preservar}}
    is_multi = _contar_produtos(norm.texto_limpo) >= 2
    saida: List[str] = []; primeiro = True
    cupons_vistos: set = set()

    for linha in norm.texto_limpo.split("\n"):
        l = linha.strip()
        if not l: saida.append(""); continue
        if _RE_ANUNCIO.match(l): saida.append(l); continue
        l = _RE_LIXO_PREF.sub("", l).strip()
        if not l: continue

        urls_na_linha = _RE_URL_RENDER.findall(l)
        sem_urls      = _RE_URL_RENDER.sub("", l).strip()

        # Linha é só URL(s)
        if urls_na_linha and not sem_urls:
            for u in urls_na_linha:
                uc = u.rstrip('.,;)>')
                saida.append(mapa.get(uc, uc))   # BUG FIX: mantém URL original se não no mapa
            continue

        # Linha mista — substitui URLs inline sem apagar
        def _sub_url(m: re.Match) -> str:
            u = m.group(0).rstrip('.,;)>')
            return mapa.get(u, m.group(0))        # BUG FIX: mantém original se não no mapa

        l = _RE_URL_RENDER.sub(_sub_url, l).strip()
        if not l: continue

        # Deduplicação de cupom
        if _KW_CUPOM.search(l):
            cupons_linha = re.findall(r'\b([A-Z][A-Z0-9_-]{3,19})\b', l)
            cupons_novos = [c for c in cupons_linha
                            if c not in _FALSO_CUPOM and c not in cupons_vistos]
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

# ── Imagens ───────────────────────────────────────────────────────
async def buscar_imagem_produto(url: str) -> Optional[str]:
    if not url or not url.startswith("http"): return None
    sessao = await _get_session()
    for t in range(1, 4):
        try:
            async with sessao.get(url, allow_redirects=True,
                                  timeout=aiohttp.ClientTimeout(total=15)) as r:
                ct = r.headers.get("content-type", "")
                if "image" in ct: return str(r.url)
                html = await r.text(errors="ignore")
                soup = BeautifulSoup(html, "html.parser")
                for attr in [{"property": "og:image"},
                             {"property": "og:image:secure_url"},
                             {"name": "twitter:image"}]:
                    tag = soup.find("meta", attrs=attr)
                    if not tag: continue
                    img_url = tag.get("content", "")
                    if not img_url.startswith("http"): continue
                    img_url = re.sub(
                        r'[?&](?:width|height|w|h|size|resize|fit|quality|q|'
                        r'maxwidth|maxheight|format|auto|compress|crop|scale)=[^&]+',
                        '', img_url).rstrip('?&')
                    return img_url
                for scr in soup.find_all("script", type="application/ld+json"):
                    try:
                        data  = json.loads(scr.string or "")
                        items = data if isinstance(data, list) else [data]
                        for item in items:
                            img = item.get("image")
                            if isinstance(img, str) and img.startswith("http"): return img
                            if isinstance(img, list) and img:
                                c = img[0]
                                if isinstance(c, str): return c
                                if isinstance(c, dict):
                                    u2 = c.get("url", "")
                                    if u2.startswith("http"): return u2
                    except Exception:
                        pass
                melhor = None; melhor_area = 0
                for img_tag in soup.find_all("img", src=True):
                    src = img_tag.get("src", "")
                    if not src.startswith("http"): continue
                    if any(x in src.lower() for x in ["icon","logo","avatar","badge","spinner"]):
                        continue
                    try:
                        w = int(img_tag.get("width", 0)); h = int(img_tag.get("height", 0))
                        area = w * h
                        if area > melhor_area: melhor_area = area; melhor = src
                    except (ValueError, TypeError):
                        if any(x in src.lower() for x in
                               ["product","produto","item","image","foto","zoom","large","xl","hd"]):
                            if not melhor: melhor = src
                if melhor: return melhor
        except asyncio.TimeoutError:
            log_enr.warning(f"⏱ Timeout buscar_img t={t}")
        except Exception as e:
            log_enr.warning(f"⚠️ buscar_img t={t}: {e}")
        if t < 3: await asyncio.sleep(1.0)
    return None

async def preparar_imagem_tg(media_obj) -> Optional[object]:
    from client import client
    try:
        buf = io.BytesIO()
        res = await client.download_media(media_obj, file=buf)
        if res is None: return None
        buf.seek(0)
        if buf.getbuffer().nbytes < 500: return None
        buf.name = "imagem.jpg"; return buf
    except Exception as e:
        log_enr.warning(f"⚠️ download_media: {e}"); return None

async def preparar_imagem_url(url: str) -> Optional[object]:
    try:
        sessao = await _get_session()
        async with sessao.get(url, timeout=aiohttp.ClientTimeout(total=20),
                              allow_redirects=True) as r:
            if r.status == 200:
                data = await r.read()
                if len(data) < 1000: return None
                buf = io.BytesIO(data); buf.name = "produto.jpg"; return buf
    except Exception as e:
        log_enr.warning(f"⚠️ preparar_img_url: {e}")
    return None

async def _resolver_imagem(norm: MensagemNormalizada) -> object:
    eh_cupom = bool(norm.cupom or _KW_CUPOM.search(norm.texto_limpo))
    if norm.tem_midia:
        img = await preparar_imagem_tg(norm.media_obj)
        if img: return img
    if eh_cupom: return None
    if norm.mapa:
        for link in norm.mapa.values():
            if not link.startswith("http"): continue
            img_url = await buscar_imagem_produto(link)
            if img_url:
                img = await preparar_imagem_url(img_url)
                if img:
                    log_enr.info(f"🖼 og:image: {img_url[:60]}"); return img
    return None

async def montar(norm: MensagemNormalizada) -> MensagemMontada:
    texto  = montar_texto(norm)
    imagem = await _resolver_imagem(norm)
    return MensagemMontada(
        msg_id=norm.msg_id, chat=norm.chat, plat=norm.plat,
        sku=norm.sku, texto=texto, imagem=imagem,
        mapa=norm.mapa, msg_id_origem=norm.msg_id,
)

