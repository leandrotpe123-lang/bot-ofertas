"""Configuração central — variáveis de ambiente, constantes, grupos."""
from __future__ import annotations
import concurrent.futures
import os
from typing import Optional
import asyncio

# ── Telegram ──────────────────────────────────────────────────────
API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION", "")
GRUPOS_ORIGEM  = [
    "promotom", "fumotom", "botofera", "fadadoscupons",
    "SamuelF3lipePromo", "fadapromos",
]
GRUPO_DESTINO  = "@ofertap"

# ── Encurtador próprio ────────────────────────────────────────────
_RAILWAY_DOMAIN = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
_SHORT_BASE = f"https://{_RAILWAY_DOMAIN}" if _RAILWAY_DOMAIN else "https://leoind.com.br"

# ── HTTP ──────────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
]

# ── Executor ──────────────────────────────────────────────────────
_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# ── Semáforos (inicializados em globals._init_globals) ────────────
# Declarados aqui como None; atribuídos no loop correto.
_SEM_ENVIO: Optional[asyncio.Semaphore] = None
_SEM_HTTP:  Optional[asyncio.Semaphore] = None

# ── Persistência JSON ─────────────────────────────────────────────
ARQUIVO_MAPEAMENTO = "map_mensagens.json"

# ── DB ────────────────────────────────────────────────────────────
_DB_PATH         = "foguetao.db"
CACHE_TTL        = 86400
TTL_DEDUPE       = 86400
TTL_SCHEDULER    = 30 * 86400
TTL_LINK_INATIVO = 7 * 86400

# ── Pillow ────────────────────────────────────────────────────────
try:
    from PIL import Image  # noqa: F401
    _PIL_OK = True
except ImportError:
    _PIL_OK = False

# ── Constantes de publicação (usadas pelo orchestrator e publicacao) ─
_JANELA_DISPUTA_S = 90.0   # janela de disputa entre grupos (segundos)
_MAX_EDITS        = 1      # máximo de edições estéticas (entre grupos)
                            # Edições por mudança no original NÃO contam.

# ── Janelas por TIPO DE OFERTA (independente de plataforma) ───────
# Define por quanto tempo a mesma oferta é considerada "duplicata".
# Após esse tempo, mesma oferta pode ser publicada de novo (a fonte
# ainda está divulgando = ainda é válida).
_JANELA_CUPOM_S    = 30 * 60    # 30 min — cupom code
_JANELA_PRODUTO_S  = 60 * 60    # 60 min — produto com ASIN/SKU
_JANELA_EVENTO_S   = 20 * 60    # 20 min — campanha/evento

# ── Estratégia de imagem ─────────────────────────────────────────
# Grupos que mandam imagens de baixa qualidade — peso de mídia reduzido
# no score (vale +1 em vez de +3). Se outro grupo mandar imagem boa, ela
# vence o desempate. Se forem os ÚNICOS a mandar, usa a imagem feia mesmo.
_GRUPOS_IMG_RUIM = frozenset({"promotom", "fumotom"})

# Score de mídia
_SCORE_MIDIA_NORMAL = 3   # mídia de grupo normal
_SCORE_MIDIA_RUIM   = 1   # mídia de _GRUPOS_IMG_RUIM

# Janela para "deletar+reenviar" quando chega imagem melhor
# Se passou MAIS desse tempo desde o post original, edita só o texto
# (não reenvia pra evitar notificar o grupo 2x desnecessariamente)
_JANELA_REENVIO_MIDIA_S = 30.0
# Muro de frescor para mensagens NOVAS (NewMessage). Oferta nova com
# idade acima disso é oferta velha ressurgida (ex.: post de 117h) e é
# descartada na entrada. NÃO atrasa nada: rajada acontece em segundos,
# MUITO abaixo deste muro. Conceito distinto da janela de disputa (90s).
_MAX_IDADE_NOVA_S = 120.0   

# Timeout do download_media do Telethon (segundos)
# Protege workers de travar infinito se Telegram engasgar
_TIMEOUT_DOWNLOAD_MIDIA = 15.0

# ── Reativação de ofertas ────────────────────────────────────────
# Cooldown mínimo desde a última publicação. Posts com palavras como
# "voltou", "Ativo", "Ainda ativo", "reativou", "ativo novamente" só republicam se passou esse tempo.
# Evita reativação acidental de cupons recém-postados.
_COOLDOWN_REATIVACAO_S = 30 * 60   # 30 minutos

# ── Código morto sinalizado (NÃO remover ainda) ───────────────────
# _SHADOW_FRASES: lista de frases fixas de engajamento.
# Substituída pela captura de comentários reais dos grupos.
# Mantida aqui para referência histórica.
_SHADOW_FRASES = [
    "Precin 🔥", "Bom preço 👀", "Tá barato esse 🛒", "Queimando estoque 🚨",
    "Melhor preço do mês 💥", "Relâmpago! ⚡", "Tá voando 🔥", "Imperdível esse 🎯",
    "Preço absurdo 😱", "Que desconto 🤑",
]
