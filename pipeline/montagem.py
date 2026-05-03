"""Camada 5 — Montagem: texto formatado, imagem e dataclass MensagemMontada."""
from __future__ import annotations
import asyncio
import io
import re
from dataclasses import dataclass
from typing import Dict, List, Optional

from logger import log_enr
from pipeline.normalizacao import MensagemNormalizada, _KW_CUPOM, _FALSO_CUPOM, _tem_emoji, _KW_EVENTO

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


def _proteger_url_md(url: str) -> str:
    """
    Protege URLs contra interpretação de Markdown do Telegram.
    URLs longas Magalu/Amazon contêm '_' em parâmetros (partner_id,
    promoter_id, utm_source etc.) que o parse_mode='md' interpreta
    como itálico, quebrando o link. A solução é escapar os caracteres
    especiais do Markdown que aparecem dentro de URLs.
    """
    # Escapa apenas dentro da URL (não no texto ao redor)
    # Caracteres que o Markdown do Telegram interpreta: _ * ` [
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
                url_final = mapa.get(uc, uc)
                # Protege contra Markdown quebrar o link
                saida.append(_proteger_url_md(url_final))
            continue

        # Linha mista — substitui URLs inline sem apagar
        def _sub_url(m: re.Match) -> str:
            u = m.group(0).rstrip('.,;)>')
            url_final = mapa.get(u, m.group(0))
            return _proteger_url_md(url_final)

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
# A imagem usada é SEMPRE a que vem junto com a mensagem do Telegram
# (mais rápida, vem direto dos servidores deles, não depende de site externo).
# A busca de og:image em sites externos foi REMOVIDA porque consumia rede
# e era a parte mais cara do bot. Quando uma oferta chega sem imagem, o
# sistema publica só o texto. Se outro grupo mandar a mesma oferta com
# imagem, o sistema reenvia o post com a imagem (lógica em publicacao.py).

async def preparar_imagem_tg(media_obj) -> Optional[object]:
    """
    Baixa a imagem da mensagem original do Telegram para um buffer em memória.
    Retorna BytesIO pronto pra reenviar via Telethon, ou None se falhar.

    Protegido por timeout para não travar o worker infinitamente caso a
    conexão com servidores do Telegram engasgue.
    """
    from client import client
    import config

    try:
        buf = io.BytesIO()
        # Timeout protege contra trava infinita do download
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
            # Imagem corrompida ou muito pequena pra ser útil
            return None
        buf.name = "imagem.jpg"
        return buf
    except Exception as e:
        log_enr.warning(f"⚠️ download_media: {e}")
        return None


async def _resolver_imagem(norm: MensagemNormalizada) -> object:
    """
    Resolve a imagem a anexar no post.

    Estratégia simplificada (sem fallback HTTP):
      - Se a mensagem original tem mídia → baixa do Telegram e usa
      - Se não tem → retorna None (publica sem imagem)

    Quando outro grupo mandar a mesma oferta com imagem, o sistema de
    score/disputa em publicacao.py se encarrega de substituir o post.
    """
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
    
