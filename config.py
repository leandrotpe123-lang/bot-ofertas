"""Configuração central — variáveis de ambiente, constantes, grupos."""
from __future__ import annotations
import os
from typing import Optional
import asyncio

# ── Telegram ──────────────────────────────────────────────────────
API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION", "")
GRUPOS_ORIGEM  = [
    "promotom", "fumotom", "ofertasconvertidas", "fadadoscupons",
    "SamuelF3lipePromo", "fadapromos",
]
    
GRUPO_DESTINO  = "fullpromotion"

# ── Encurtador próprio ────────────────────────────────────────────
_RAILWAY_DOMAIN = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
_SHORT_BASE = f"https://{_RAILWAY_DOMAIN}" if _RAILWAY_DOMAIN else "https://leoind.com.br"

# ── HTTP ──────────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
]

# ── Semáforos de throughput (recursos de runtime) ─────────────────
# Exceção consciente à pureza declarativa deste módulo: config é a
# única folha do grafo de imports alcançável por plataformas,
# pipeline e saída. Movê-los para globals criaria o ciclo
# plataformas → globals → plataformas.contrato no boot. Declarados
# None aqui; instanciados por globals._init_globals, dentro do loop.
_SEM_ENVIO: Optional[asyncio.Semaphore] = None
_SEM_HTTP:  Optional[asyncio.Semaphore] = None

# ── DB ────────────────────────────────────────────────────────────
# CAMINHO DO BANCO — declaração, não abertura.
#
# Este módulo apenas DECLARA onde o banco deve viver; quem abre,
# valida o diretório e falha é database_conexao. A separação é
# deliberada: config é folha do grafo de imports e não pode tocar
# filesystem no import, sob pena de derrubar o boot inteiro por um
# problema de disco antes de qualquer log existir.
#
# O caminho é configurável por ambiente porque o armazenamento do
# processo NÃO é o mesmo em toda parte:
#
#   Railway  → DB_PATH=/data/foguetao.db   (volume persistente)
#   local    → sem DB_PATH → "foguetao.db" no diretório corrente
#
# O default preserva byte a byte o comportamento anterior a esta
# frente: sem a variável, nada muda para quem roda local.
#
# ATENÇÃO — o banco NÃO é um arquivo só. Com journal_mode=WAL
# (ligado em database_conexao._init_db) o SQLite mantém `<db>-wal` e
# `<db>-shm` NO MESMO DIRETÓRIO, e uma transação confirmada pode
# viver apenas no `-wal` até o próximo checkpoint. Por isso o que
# precisa ser persistente é o DIRETÓRIO, não o arquivo: apontar
# DB_PATH para dentro do volume é o que garante que os três
# sobrevivam juntos a restart e deploy.
_DB_PATH         = os.environ.get("DB_PATH", "foguetao.db")

# Quando o caminho aponta para um armazenamento que deve ser
# persistente, um diretório ausente ou somente-leitura é ERRO DE
# INFRAESTRUTURA, não um caso a contornar criando o banco noutro
# lugar. Cair para o disco efêmero em silêncio seria pior que
# falhar: o bot subiria, publicaria links curtos e os perderia no
# deploy seguinte — exatamente a falha que esta frente corrige.
# database_conexao consome esta lista para decidir entre criar o
# diretório (caminho local) e exigir que ele seja um volume REALMENTE
# montado — diretório existir não prova montagem.
#
# RAILWAY_VOLUME_MOUNT_PATH é injetada automaticamente pela Railway
# quando um volume está anexado ao serviço; não precisa ser definida
# à mão. Ela entra primeiro para que a checagem acompanhe sozinha uma
# eventual mudança de mount path. "/data" fica como declaração
# explícita do caminho combinado nesta frente, válida mesmo se a
# variável não vier.
_DB_MONTAGENS_PERSISTENTES = tuple(dict.fromkeys(
    p for p in (os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", ""), "/data") if p
))
# Link convertido sem uso há 30 dias sai do cache (cada uso renova o
# prazo). Medido: 100 mil links = 22 MB e 0,03 ms por consulta.
TTL_LINK_INATIVO = 30 * 86400

# ── Pillow ────────────────────────────────────────────────────────
try:
    from PIL import Image  # noqa: F401
    _PIL_OK = True
except ImportError:
    _PIL_OK = False

# ── Constantes de publicação (usadas pelo orchestrator e publicacao) ─
_MAX_EDITS = 2   # duas evoluções por ciclo; sync/renascimento não contam

# ── Estratégia de imagem ─────────────────────────────────────────
# Grupos que mandam imagens de baixa qualidade — peso de mídia reduzido
# no score (vale +1 em vez de +3). Se outro grupo mandar imagem boa, ela
# vence o desempate. Se forem os ÚNICOS a mandar, usa a imagem feia mesmo.
_GRUPOS_IMG_RUIM = frozenset({"promotom", "fumotom"})

# Score de mídia
_SCORE_MIDIA_NORMAL = 3   # mídia de grupo normal
_SCORE_MIDIA_RUIM   = 1   # mídia de _GRUPOS_IMG_RUIM

# Janela para "deletar+reenviar" quando chega imagem melhor
# Muro de frescor para mensagens NOVAS (NewMessage). Oferta nova com
# idade acima disso é oferta velha ressurgida (ex.: post de 117h) e é
# descartada na entrada. NÃO atrasa nada: rajada acontece em segundos,
# MUITO abaixo deste muro. Conceito distinto da janela de disputa (90s).
_MAX_IDADE_NOVA_S = 120.0   

# Timeout do download_media do Telethon (segundos)
# Protege workers de travar infinito se Telegram engasgar
_TIMEOUT_DOWNLOAD_MIDIA = 15.0


# ── [F1.1] Proveniência — API privada do worker (somente leitura) ──
# Sem API_PRIVADA_PORTA E API_PRIVADA_SEGREDO válidos nada liga:
# eventos.emitir() fica no-op e nenhuma porta abre (ver main e
# eventos/). Valor inválido NUNCA derruba o boot: cai no padrão
# (desligado / limites padrão).
def _int_env(nome: str, padrao: int, minimo: int, maximo: int) -> int:
    """Inteiro de variável de ambiente dentro de [minimo, maximo];
    ausente, inválido ou fora da faixa → `padrao`."""
    bruto = os.environ.get(nome, "")
    try:
        valor = int(bruto) if bruto.strip() else padrao
    except ValueError:
        return padrao
    return valor if minimo <= valor <= maximo else padrao


_PORTA_PUBLICA         = _int_env("PORT", 8080, 1, 65535)
# Porta interna de um proxy TCP PÚBLICO da Railway, se existir (variável
# fornecida pela própria Railway). A API privada nunca liga nela.
_PORTA_TCP_PUBLICA     = _int_env("RAILWAY_TCP_APPLICATION_PORT", 0, 1, 65535)
API_PRIVADA_PORTA      = _int_env("API_PRIVADA_PORTA", 0, 1025, 65535)   # 0 = desligada
API_PRIVADA_SEGREDO    = os.environ.get("API_PRIVADA_SEGREDO", "")
EVENTOS_ANEL_MAX       = _int_env("EVENTOS_ANEL_MAX", 20_000, 1_000, 100_000)
EVENTOS_ANEL_MAX_BYTES = _int_env("EVENTOS_ANEL_MAX_BYTES", 32 << 20,
                                  1 << 20, 256 << 20)

# ── CUPOM SEM CÓDIGO: lançamento × liberação (pipeline/cupom_lancamento) ──
# Constantes de CÓDIGO, de propósito sem variável de ambiente: mudar qualquer
# uma é PR aprovado pelo dono (recalibrar = PR com dados novos). Nada se
# ajusta sozinho em produção.
#
# ATIVO desde o PR #22 (20/10/2026); regra (a+b) do PR #24; reversão = PR #23.
# "SOMBRA": a regra nova só registra o que faria (CUPOM_ENCONTRO_SOMBRA,
# CUPOM_LIBERACAO_SOMBRA, CUPOM_INCERTO); o comportamento é o de sempre.
# "ATIVO": a regra nova decide a família. Ligar exige PR do dono depois dos
# critérios escritos no PR da frente (≥ 5 dias de sombra revisada, zero erro).
CUPOM_MODO = "ATIVO"
# Fonte: todo o log disponível na Railway, 07/10 09:07 → 14/10 09:08 UTC
# (medição de 14/10/2026). Δ = da estampa do post vivo até a mensagem nova.
#   Mesmo lançamento, fontes diferentes — ML: n=18, 16 casos ≤ 5,6 min (+10,8
#   e 21,2); Shopee: n=29, 25 casos ≤ 4,4 min (+9,8, 14,9, 15,8, 20,2).
#   Lançamento diferente, fontes diferentes: ML 56,1; Shopee 19,4 e 20,1.
#   Mesma fonte, mensagem nova (liberação): ML 25,5; 55,1; 56,4; 59,7.
# RAJADA: cobre os dois aglomerados densos (41 de 47) com 2,4 min de folga e
# fica 11 min abaixo do 1º lançamento diferente (19,4). Plataforma sem
# medição fica fora da regra.
CUPOM_RAJADA_S = {"mercadolivre": 8 * 60, "shopee": 8 * 60}
# SUPERIOR: maior Δ de mesmo lançamento (21,2) + folga; metade do ciclo da
# série horária (próximas liberações chegaram a 55–60 min). Entre a RAJADA e
# o SUPERIOR o tempo sozinho não decide: zona cinzenta (CUPOM_INCERTO).
CUPOM_LIMITE_SUPERIOR_S = 30 * 60

# ── CAMPANHA POR PÁGINA DE RESGATE (pipeline/campanha_resgate*) ──
# Constante de CÓDIGO, sem variável de ambiente: ligar ou desligar é PR do
# dono. Qualquer valor diferente de "ATIVO" vale como "SOMBRA".
# "SOMBRA" (padrão): só registra o que a regra faria (🎟 [RESGATE_SOMBRA]);
# publicação, família, canal de cupons e locks são os de sempre.
# "ATIVO": a mensagem que abriria post NOVO junta no post vivo da mesma
# campanha (mesma página de resgate declarada como destino principal), e o
# post cujo único destino é essa página vai também ao canal de cupons.
# Inclui as junções "compatível" (benefício ausente de um lado): os dois
# casos reais de 28/10/2026 são desse tipo.
CAMPANHA_RESGATE_MODO = "SOMBRA"
