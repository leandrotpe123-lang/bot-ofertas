"""
CAMPANHA POR PÁGINA DE RESGATE — SOMBRA (Frente A, caso real de 28/10/2026).

28/10 00:00 BRT: Promotom 112024 "Cupons Shopee" → post 31943
(`shopee|cupb|geral`, nos dois canais); Fada 18042 "Começou! Esquenta 11.11
Shopee / Resgate todos os cupons aqui" 44 s depois → post NOVO 31945
(`shopee|url|https://shopee.com.br/m/cupom-de-desconto`, só no principal);
Samuel 123470 → DUP_SILENCIOSO no 31945. Os dois posts publicaram o MESMO
link s.shopee.com.br/AAHdNLxGSX. O dono apagou o 31943.

Todo cenário de fluxo passa pelo caminho REAL da produção a partir da
normalização (orchestrator_pipeline: normalizar → enriquecer →
deve_enviar_async → montar → publicacao.enviar), com os TEXTOS REAIS das
fontes (prévia pública t.me/s, HTML → markdown do Telethon). A única
simulação é a rede da afiliação (resolver_e_afiliar), que devolve só o que
os logs e os canais PROVAM — cada link traz a prova em RESOLUCAO. Canônica
que o ambiente não consegue observar (rede da Shopee bloqueada) é LACUNA:
vira `https://lacuna.invalid/<link>`, que de propósito não é página
nenhuma — o resultado nunca pode depender de um palpite.

O QUE ESTE ARQUIVO PROVA:
  01  contrato: só a Shopee declara página de resgate, e só
      /m/cupom-de-desconto; carteira, carrinho, live, /m/11-11 e outra
      plataforma não
  02  regra pura, caso a caso, com perfis de textos reais (pares
      sintéticos MARCADOS onde o corpus não tem o par)
  03  caso real 28/10: hoje 2 posts (inalterado); a sombra junta a Fada no
      post da Promotom e prevê os 2 canais; o Samuel segue o dedup de hoje;
      o produto do fumotom 42674 (página como 1º link) fica fora
      03b  17/10 (Promotom 111508 × Fada 17870 "VIP", o dono apagou o
           29546): a regra de cupom fica em INCERTO sem_beneficio (2
           posts); a sombra junta pela página
  04  a sombra não muda NADA: mesmas chamadas ao Telegram, texto a texto,
      e o mesmo banco, com a sombra ligada, desligada e quebrando por
      dentro; nenhuma linha escrita no SQLite durante a sombra
  05  noites reais sem junção (21/10 12h, 22/10, 26/10 com o Tech, 27/10
      15h): nenhuma linha JUNTA — e a lacuna do link do Samuel não muda o
      resultado
  06  produto com cupom: o post do fumotom que abre com a página nunca é
      candidato nem par
  07  exclusão manual: o post apagado pelo dono não é par e não renasce
  08  falha no reconhecimento do link nunca junta (sem conversão,
      canônica ainda encurtada, outra rota, link publicado diferente)
  09  canais separados: evento sozinho na página é elegível ao canal de
      cupons sem "cupom" no título (só previsto); /m/11-11 não; o canal
      de hoje não muda
  10  SIMULAÇÃO DE TESTE (não é ativação): se a família fosse a da
      decisão, os aplicadores de hoje dariam 1 post no principal e 1 cópia
      no canal de cupons
  11  só mensagem NOVA passa pela sombra; a edição não

    python tests/test_campanha_resgate.py
"""
import asyncio
import contextlib
import logging
import os
import re
import sys
import time
from dataclasses import replace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_cupom_sem_codigo as T                               # noqa: E402  (preparar())
from test_cupom_sem_codigo import FADA, PROMOTOM, SAMUEL, _db    # noqa: E402
from _telethon_fakes import afetadas                            # noqa: E402
from _harness_e5 import rodar                                   # noqa: E402

import config                                                   # noqa: E402
import client as modulo_client                                  # noqa: E402
import database_conexao                                         # noqa: E402
from database import db_get_post, db_ofertas_de_post            # noqa: E402
from database_espelho import db_espelho_get                     # noqa: E402
from logger import log_out                                      # noqa: E402
from pipeline import campanha_resgate as regra                  # noqa: E402
from pipeline import campanha_resgate_sombra as sombra          # noqa: E402
from pipeline import (cupom_lancamento, espelho_cupons, familia,  # noqa: E402
                      normalizacao, origem, post_apagado, publicacao)
from pipeline.deduplicacao import deve_enviar_async             # noqa: E402
from pipeline.enriquecimento import enriquecer, enriquecer_edicao  # noqa: E402
from pipeline.ingestao import MensagemBruta, links_de           # noqa: E402
from pipeline.montagem import montar                            # noqa: E402
from pipeline.normalizacao_texto import sem_marcacao            # noqa: E402
from pipeline.vida_oferta import VIDA_OFERTA_S                  # noqa: E402
from plataformas import registry                                # noqa: E402

FUMOTOM = -1003775401737
T.identidade._CHAT_USERNAME[FUMOTOM] = "fumotom"
CANAL_CUPONS = "FullPromotionCupons"

PAGINA = "https://shopee.com.br/m/cupom-de-desconto"
PUB_PAGINA = "https://s.shopee.com.br/AAHdNLxGSX"

# ══════════════════════════════════════════════════════════════════
# Resolução PROVADA: link da fonte → (publicado, canônica, prova).
# canônica None = LACUNA (não observável aqui). Link que não está aqui
# também é lacuna (ver _resolucao).
# ══════════════════════════════════════════════════════════════════
RESOLUCAO = {
    # Promotom
    "https://s.shopee.com.br/2B5snGi9NX": (PUB_PAGINA, PAGINA,
        "vira AAHdNLxGSX nos posts 30602, 31079, 31425 e 31451; AAHdNLxGSX é o link do "
        "31945, identidade logada url|" + PAGINA + " (cache por destino)"),
    "https://s.shopee.com.br/qaVCprDoA": ("https://s.shopee.com.br/7faIOl2FGW", None, "post 30602"),
    "https://s.shopee.com.br/9V2Q1jzast": ("https://s.shopee.com.br/LoB6msmOY", None,
        "posts 31079 e 31451 (1º link)"),
    "https://s.shopee.com.br/9APjGY25YQ": ("https://s.shopee.com.br/60SGP3K099", None, "post 31770"),
    # Fada
    "https://s.shopee.com.br/40aG8LLgaE": (PUB_PAGINA, PAGINA,
        "log 28/10 03:00:03 PASSOU shopee|url|" + PAGINA + "; post 31945"),
    "https://s.shopee.com.br/AAGNE4pfS9": (PUB_PAGINA, PAGINA, "posts 30837, 31566 e 31638"),
    "https://s.shopee.com.br/8KiBu8pls3": (PUB_PAGINA, PAGINA, "post 31463 (2º link)"),
    "https://s.shopee.com.br/5LD0k06jJ4": (PUB_PAGINA, PAGINA, "post 31785"),
    "https://s.shopee.com.br/40YmMTfcfQ": ("https://s.shopee.com.br/905ea27BKK", None,
        "posts 31463 e 31638"),
    "https://s.shopee.com.br/9Kj9SraimJ": ("https://s.shopee.com.br/60SGP3K099", None, "post 31785"),
    "https://s.shopee.com.br/3qKx8uTPsp": ("https://s.shopee.com.br/4LK2yNECtS", None, "post 30591"),
    "https://s.shopee.com.br/1W0IbEaaOB": ("https://s.shopee.com.br/4VduAnfYLs",
        "https://shopee.com.br/m/11-11", "post 31930, identidade logada url|https://shopee.com.br/m/11-11"),
    # Samuel: só o 123470 tem prova (log); os outros são lacuna
    "https://s.shopee.com.br/6L5YgZgkK8": (PUB_PAGINA, PAGINA,
        "log 28/10 03:00:17 PASSOU shopee|url|" + PAGINA + " → DUP_SILENCIOSO no 31945; "
        "mesmo destino → mesmo link publicado (cache por destino)"),
    # fumotom 42674 → post 31947
    "https://s.shopee.com.br/4LKUIxr0Wf": (PUB_PAGINA, PAGINA, "post 31947 (1º link)"),
    "https://s.shopee.com.br/6q1pHYqHxX": ("https://s.shopee.com.br/3VlNJRTRhV",
        "https://shopee.com.br/product/1930141978/58270173531",
        "post 31947, identidade logada shopee|1930141978.58270173531"),
}


def _lacuna(url):
    return "https://lacuna.invalid/" + url.rstrip("/").rsplit("/", 1)[-1]


def _resolucao(url, tabela):
    pub, can, _prova = tabela.get(url, (None, None, "lacuna"))
    if pub is None:                       # link sem prova: publicado E canônica lacuna
        return "https://s.shopee.com.br/LACUNA" + url.rsplit("/", 1)[-1], _lacuna(url)
    return pub, can or _lacuna(url)


@contextlib.contextmanager
def _afiliacao(tabela):
    """Troca SÓ a rede da afiliação. Como a de produção, só converte link
    de plataforma registrada (t.me fica de fora); valor (None, None) na
    tabela = o link não converte (a resolução falhou)."""
    async def falsa(converter, msg_id=0):
        mapa, canon, plats = {}, {}, []
        for u in converter:
            if registry.resolver(u) is None:
                continue                                    # plat "none"
            if u in tabela and tabela[u][0] is None:
                continue                                    # não converteu
            mapa[u], canon[u] = _resolucao(u, tabela)
            plats.append("shopee")
        return mapa, canon, plats
    real = normalizacao.resolver_e_afiliar
    normalizacao.resolver_e_afiliar = falsa
    try:
        yield
    finally:
        normalizacao.resolver_e_afiliar = real


# ══════════════════════════════════════════════════════════════════
# Textos REAIS (prévia pública; HTML → markdown do Telethon)
# ══════════════════════════════════════════════════════════════════
# 28/10 (UTC)
P_112024 = ("Cupons Shopee\n\n-Resgate aqui:\n1 https://s.shopee.com.br/2B5snGi9NX\n"
            "2 https://s.shopee.com.br/qaVCprDoA")                                  # 02:59:18 → 31943
F_18042 = ("**🚨 Começou! Esquenta 11.11 Shopee **\n\n**✅ Resgate todos os cupons aqui:**\n"
           "https://s.shopee.com.br/40aG8LLgaE")                                    # 03:00:02 → 31945
S_123470 = ("🔥 **Começou! Esquenta 11.11 Shopee**\n\n⭐️ **Resgate todos os cupons aqui:**\n"
            "https://s.shopee.com.br/6L5YgZgkK8\n\nanúncio")                        # 03:00:15 → DUP no 31945
FU_42674 = ("Placa-Mãe Msi A520m-a Pro, Amd Am4, Matx, Ddr4, Preto - A520m-a Pro\n\nR$ 342\n"
            "-Resgate todos os cupons desta página:\nhttps://s.shopee.com.br/4LKUIxr0Wf\n\n"
            "-Link produto:\nhttps://s.shopee.com.br/6q1pHYqHxX\n\n-Anúncio")         # 03:01:07 → 31947
# 22/10
P_111784 = ("Cupons Shopee\n\n-Resgate aqui:\n1 https://s.shopee.com.br/9V2Q1jzast\n"
            "2 https://s.shopee.com.br/2B5snGi9NX\n3 https://s.shopee.com.br/qaVCprDoA")  # 02:59:25 → 30837
F_17953 = ("**🚨 Novos Cupons Shopee\n\n🎟 R$30 OFF em R$299\n🎟 R$90 OFF em R$899\n\n"
           "✅ Resgate aqui: **\nhttps://s.shopee.com.br/AAGNE4pfS9")              # 03:00:01 → 30837 EVOLUI
S_122635 = ("🔥 **Novos Cupons Shopee**\n\n🎟 **R$ 30 OFF** em **R$ 299**\n🎟 **R$ 90 OFF** em "
            "**R$ 899**\n\n⭐️ **Resgate aqui:**\nhttps://s.shopee.com.br/60SYicbJh9\n\nanúncio")
# 26/10
P_111915 = ("Cupons Shopee\n\n-Resgate aqui:\n1 https://s.shopee.com.br/9V2Q1jzast\n"
            "2 https://s.shopee.com.br/2B5snGi9NX\n3 https://s.shopee.com.br/qaVCprDoA")  # 02:59:05 → 31451
F_17997 = "**🚨 Novos Cupons Shopee\n**\nhttps://s.shopee.com.br/AAGNE4pfS9"         # 03:01:14 → 31451
S_123091 = ("🔥 **Novos Cupons Shopee**\n\n⭐️ **Resgate aqui:**\n"
            "https://s.shopee.com.br/7VHSt1QTCW\n\nanúncio")                        # 03:01:26 → 31451
F_17998 = ("**🔥 Novos Cupons Shopee Tech\n\n🎟 R$100 OFF em R$999**\n"
           "https://s.shopee.com.br/40YmMTfcfQ\n\n**🎟 R$50 OFF em R$450**\n"
           "https://s.shopee.com.br/8KiBu8pls3")                                    # 03:02:26 → 31463
S_123093 = ("🔥 **Novos Cupons Shopee Tech**\n\n🎟 **R$ 100 OFF** em **R$ 999**\n"
            "https://s.shopee.com.br/5AtY6ogCXJ\n\n🎟 **R$ 50 OFF** em **R$ 450**\n"
            "https://s.shopee.com.br/7puJHiaaRB\n\nanúncio")                        # 03:02:48 → 31463
# 27/10
P_111992 = ("Cupons Shopee\n\n-Resgate aqui:\n1 https://s.shopee.com.br/9APjGY25YQ\n"
            "2  https://s.shopee.com.br/2B5snGi9NX")                                # 15:02:50 → 31770
F_18025 = "**🔥 Novo Cupom Shopee 30% OFF**\n\nhttps://s.shopee.com.br/9Kj9SraimJ"   # 15:03:23 → 31770
S_123345 = ("🔥 **Novo Cupom Shope 30% OFF**\n\n⭐️ **Resgate aqui:**\n"
            "https://s.shopee.com.br/60ShUkZo7V\n\nanúncio")                        # 15:03:37 → 31770
F_18028 = ("**🔥 Novos Cupons Shopee**\n\nhttps://s.shopee.com.br/5LD0k06jJ4\n"
           "https://s.shopee.com.br/9Kj9SraimJ")                                    # 15:40:08 → 31785
F_18038 = ("**__🚨 __É AMANHÃ! ESQUENTA 11.11 SHOPEE**\n\n⏰ Começa meia-noite!\n\n"
           "• Cupons confirmados nos horários:\n**00h**, **09h**, **10h**, **12h**, **16h** e "
           "**20h**\n\n• Cupom atrelado ao link **20/120**:\nhttps://t.me/fadadoscupons/18030\n\n"
           "ℹ️** Começa meia-noite:**\nhttps://s.shopee.com.br/1W0IbEaaOB")         # 22:02:54 → 31930
# 21/10
F_17927 = ("**🚨🚨 Renovados! Dia de Cupons Shopee**\n\n**👇 Resgate todos os cupons aqui:**\n"
           "https://s.shopee.com.br/3qKx8uTPsp")                                    # 12:00:03 → 30591
S_122435 = ("🔥 **Renovados! Dia de Cupons Shopee**\n\n⭐️ **Resgate todos os cupons aqui:**\n"
            "https://s.shopee.com.br/LoB637ATQ\n\nanúncio")                         # 12:00:40 → DUP 30591
P_111724 = ("Cupons Shopee\n\n-Resgate aqui:\n1 https://s.shopee.com.br/2B5snGi9NX\n"
            "2 https://s.shopee.com.br/qaVCprDoA")                                  # 12:03:37 → 30602
# 17/10 (corpus de tests/test_post_apagado: o dono apagou o 29546 da Promotom)
P_111508 = ("Cupons Shopee\n\n-Resgate aqui:\n1 https://s.shopee.com.br/2B5snGi9NX\n"
            "2 https://s.shopee.com.br/qaVCprDoA")                                  # 02:59 → 29546
F_17870 = ("🚨 Novo Cupom Shopee VIP\n\n✅ Resgate aqui : https://s.shopee.com.br/40aG8LLgaE\n\n"
           "🔝 Assine aqui (1 Mês Grátis): https://s.shopee.com.br/AKZmtpLNvr")      # +2,8 min → 29549

# Textos PUBLICADOS (prévia do canal de destino) — para os perfis da regra pura
PUB_31943 = ("🔥 Cupons Shopee\n\n📌 Resgate aqui:\n1 https://s.shopee.com.br/AAHdNLxGSX\n"
             "2 https://s.shopee.com.br/7faIOl2FGW")          # = 30602 (mesma fonte e links)
PUB_31463 = ("🔥 Novos Cupons Shopee Tech\n\n🎟 R$100 OFF em R$999\nhttps://s.shopee.com.br/905ea27BKK\n\n"
             "🎟 R$50 OFF em R$450\nhttps://s.shopee.com.br/AAHdNLxGSX")
PUB_31299 = ("🔥 Novo Cupom Shopee\n\n🎟 10% OFF acima de 0, MAXIMO DE R$10\n\n⭐️ Resgate aqui:\n"
             "https://s.shopee.com.br/AAHdNLxGSX")
PUB_31947 = ("Placa-Mãe Msi A520m-a Pro, Amd Am4, Matx, Ddr4, Preto - A520m-a Pro\n\nR$ 342\n"
             "-Resgate todos os cupons desta página:\nhttps://s.shopee.com.br/AAHdNLxGSX\n\n"
             "-Link produto:\nhttps://s.shopee.com.br/3VlNJRTRhV")


# ══════════════════════════════════════════════════════════════════
# Telegram falso por canal + registro de cada chamada
# ══════════════════════════════════════════════════════════════════
class _Msg:
    def __init__(self, i):
        self.id, self.media = i, None


class Canais:
    def __init__(self):
        self.seq = {}
        self.chamadas = []                       # (op, canal, id, texto)
        self.no_ar = {}                          # canal → {id: texto}

    def _novo(self, dest, texto):
        dest = str(dest)
        base = 61000 if dest == CANAL_CUPONS else 51000
        self.seq[dest] = self.seq.get(dest, base) + 1
        i = self.seq[dest]
        self.no_ar.setdefault(dest, {})[i] = texto
        self.chamadas.append(("send", dest, i, texto))
        return _Msg(i)

    async def send_message(self, dest, texto, *a, **k):
        return self._novo(dest, texto)

    async def send_file(self, dest, img, caption=None, *a, **k):
        return self._novo(dest, caption or "")

    async def upload_file(self, file, *a, **k):
        return file

    async def edit_message(self, dest, msg_id, texto=None, *a, **k):
        self.chamadas.append(("edit", str(dest), msg_id, texto))
        self.no_ar.setdefault(str(dest), {})[msg_id] = texto
        return _Msg(msg_id)

    async def delete_messages(self, dest, ids, *a, **k):
        lista = ids if isinstance(ids, (list, tuple)) else [ids]
        for i in lista:
            self.chamadas.append(("delete", str(dest), i, None))
            self.no_ar.get(str(dest), {}).pop(i, None)
        return afetadas(lista)

    async def get_messages(self, dest, ids=None, *a, **k):
        lista = ids if isinstance(ids, (list, tuple)) else [ids]
        return [_Msg(i) if i in self.no_ar.get(str(dest), {}) else None for i in lista]

    async def download_media(self, media, file=None, *a, **k):
        return None

    def principal(self):
        return sorted(self.no_ar.get(str(config.GRUPO_DESTINO), {}))

    def cupons(self):
        return sorted(self.no_ar.get(CANAL_CUPONS, {}))


# ══════════════════════════════════════════════════════════════════
# Linhas da sombra
# ══════════════════════════════════════════════════════════════════
_RE_LINHA = re.compile(
    r"msg=\((?P<chat>-?\d+),(?P<msg>\d+)\) acao=(?P<acao>\w+) post:(?P<post>\S+) "
    r"motivo=(?P<motivo>\S+) .* destino_hoje=(?P<hoje>\S+) "
    r"destino_previsto=(?P<previsto>\S+) .* outros=\[(?P<outros>[^\]]*)\]$")


class _Captura(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.linhas, self.erros = [], []

    def emit(self, rec):
        m = rec.getMessage()
        if "❌ [RESGATE_SOMBRA]" in m:
            self.erros.append(m)
        elif "[RESGATE_SOMBRA]" in m:
            self.linhas.append(m)


def _ler(linha):
    m = _RE_LINHA.search(linha)
    assert m, linha
    d = m.groupdict()
    d["msg"] = int(d["msg"])
    d["post"] = None if d["post"] == "-" else int(d["post"])
    d["outros"] = [int(x) for x in d["outros"].split(",") if x.strip()]
    return d


# ══════════════════════════════════════════════════════════════════
# Cenário: o caminho REAL a partir da normalização
# ══════════════════════════════════════════════════════════════════
class Noite:
    def __init__(self, cli):
        self.cli = cli
        self.norms = {}

    async def chega(self, chat, mid, texto):
        bruta = MensagemBruta(msg_id=mid, chat=str(chat), texto=texto,
                              links=links_de(texto), tem_midia=False, media_obj=None)
        norm = await normalizacao.normalizar(bruta)
        self.norms[(chat, mid)] = norm
        if norm is not None:
            enr = enriquecer(norm)
            if await deve_enviar_async(enr):
                await publicacao.enviar(await montar(norm), norm, enr=enr, is_edit=False)
        await _drenar()
        return norm

    def post(self, chat, mid):
        return origem.consultar(str(chat), mid)

    @staticmethod
    def recuar(seg):
        """O relógio anda `seg` segundos (os posts envelhecem)."""
        with _db() as db:
            db.execute("UPDATE post_estado SET janela_fim = janela_fim - ?, ts = ts - ?",
                       (seg, seg))


async def _drenar(limite=15.0):
    fim = time.monotonic() + limite
    while espelho_cupons._tarefas:
        restante = fim - time.monotonic()
        assert restante > 0, "operações do canal de cupons não terminaram"
        await asyncio.wait(set(espelho_cupons._tarefas), timeout=restante)


def cenario(corpo, *, resolucao=RESOLUCAO, modo="ATIVO"):
    """Banco vazio, Telegram falso, canal de cupons ligado, CUPOM_MODO
    fixo (produção: ATIVO desde o #22). Devolve (noite, captura)."""
    async def _run(cap):
        T._zerar_banco()
        T._isolar_loop()
        post_apagado._QUARENTENA.clear()
        T.MOTIVOS.clear()
        cli = Canais()
        modulo_client.client = cli
        os.environ["CANAL_CUPONS"] = CANAL_CUPONS
        espelho_cupons.iniciar()
        noite = Noite(cli)
        try:
            await corpo(noite)
            await _drenar()
        finally:
            await espelho_cupons.encerrar()
        return noite
    cap = _Captura()
    antigo = config.CUPOM_MODO
    config.CUPOM_MODO = modo
    log_out.addHandler(cap)
    try:
        with _afiliacao(resolucao):
            noite = asyncio.run(_run(cap))
    finally:
        log_out.removeHandler(cap)
        config.CUPOM_MODO = antigo
    return noite, cap


def _banco():
    """Todas as tabelas, sem relógio (o que muda entre execuções)."""
    def sem_tempo(v):
        if isinstance(v, (int, float)) and not isinstance(v, bool) and 1.5e9 < v < 2.5e9:
            return "T"
        return v
    with _db() as db:
        tabelas = [t for (t,) in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        return {t: sorted(repr(tuple(sem_tempo(v) for v in r))
                          for r in db.execute(f"SELECT * FROM {t}").fetchall())
                for t in tabelas}


async def _caso_real(noite):
    """28/10 00:00 BRT, na ordem real."""
    await noite.chega(PROMOTOM, 112024, P_112024)
    await noite.chega(FADA, 18042, F_18042)
    await noite.chega(SAMUEL, 123470, S_123470)
    await noite.chega(FUMOTOM, 42674, FU_42674)


# ══════════════════════════════════════════════════════════════════
# 01 — contrato
# ══════════════════════════════════════════════════════════════════
def test_01_contrato(r):
    comp = registry.compor_capacidade("paginas_resgate")
    r.check(comp == {PAGINA: frozenset({"shopee"})}, "01.so_a_shopee_e_so_a_pagina", str(comp))
    r.check(sombra.pagina_de_resgate("shopee", PAGINA) == PAGINA, "01.pagina_reconhecida")
    for nome, url in (("carteira", "https://shopee.com.br/user/voucher-wallet"),
                      ("carrinho", "https://shopee.com.br/cart"),
                      ("11-11", "https://shopee.com.br/m/11-11"),
                      ("live", "https://live.shopee.com.br/live/7605481"),
                      ("vazia", ""), ("encurtada", "https://s.shopee.com.br/40aG8LLgaE"),
                      ("outra_rota", PAGINA + "-11-11"), ("barra", PAGINA + "/")):
        r.check(sombra.pagina_de_resgate("shopee", url) == "", f"01.nao_declarada.{nome}")
    for plat in ("mercadolivre", "amazon", "magalu", "netshoes", ""):
        r.check(sombra.pagina_de_resgate(plat, PAGINA) == "", f"01.outra_plataforma.{plat or 'vazia'}")


# ══════════════════════════════════════════════════════════════════
# 02 — regra pura
# ══════════════════════════════════════════════════════════════════
def _perfil(texto):
    """Perfil como a produção o calcula (texto de análise, sem marcação)."""
    return cupom_lancamento.perfil(sem_marcacao(texto), "shopee")


AGORA = 1_800_000_000.0
SUP = 1800.0


def _cand(texto, chat=FADA, chaves=("shopee|url|" + PAGINA,), pagina=PAGINA,
          publicada=PUB_PAGINA, familia_hoje=None, espelho_hoje=False):
    return regra.Candidato("shopee", str(chat), tuple(chaves), pagina, publicada,
                           _perfil(texto), familia_hoje, espelho_hoje)


def _vivo(mid, texto, chaves=("shopee|cupb|geral",), fontes=(PROMOTOM,), idade=46.0,
          espelhado=True, plat="shopee"):
    return regra.PostVivo(mid, plat, frozenset(chaves), next(iter(links_de(texto)), ""),
                          _perfil(texto), AGORA - idade, frozenset(str(f) for f in fontes),
                          espelhado)


def test_02_regra_pura(r):
    av = lambda c, vivos: regra.avaliar(c, vivos, AGORA, SUP)       # noqa: E731
    F = F_18042
    p43 = _vivo(31943, PUB_31943)

    d = av(_cand(F), [p43])
    r.check((d.acao, d.post, d.motivo) == (regra.JUNTA, 31943, "mesma_pagina_de_resgate:compativel"),
            "02.caso_real_junta", str(d))
    r.check((d.destino_hoje, d.destino_previsto) == ("principal", "principal+cupons"),
            "02.caso_real_canais", str(d))
    r.check(abs(d.delta_s - 46.0) < 1e-6, "02.delta", str(d.delta_s))

    d = av(_cand(F), [replace(p43, espelhado=False)])
    r.check(d.acao == regra.JUNTA and d.destino_previsto == "principal",
            "02.juncao_vale_o_canal_do_post", str(d))

    # Tech 31463: a página é o 2º link → não é par (26/10 03:02, campanha própria)
    d = av(_cand(F), [_vivo(31463, PUB_31463, ("shopee|cupb|tech",), (FADA,))])
    r.check((d.acao, d.motivo) == (regra.MANTER, "sem_par"), "02.pagina_como_2o_link_nao_e_par", str(d))

    # produto do fumotom com a página como 1º link: âncora forte → nunca par
    d = av(_cand(F), [_vivo(31947, PUB_31947, ("shopee|1930141978.58270173531",), (FUMOTOM,))])
    r.check((d.acao, d.motivo) == (regra.MANTER, "sem_par"), "02.produto_nunca_e_par", str(d))
    d = av(_cand(FU_42674, chat=FUMOTOM, chaves=("shopee|1930141978.58270173531",)), [p43])
    r.check((d.acao, d.motivo) == (regra.MANTER, "fora:ancora_forte"), "02.produto_nunca_candidato", str(d))
    d = av(_cand(F, chaves=("shopee|cup|ESQUENTA10",)), [p43])
    r.check(d.motivo == "fora:ancora_forte", "02.cupom_com_codigo_fora", str(d))
    d = av(_cand(F, chaves=()), [p43])
    r.check(d.motivo == "fora:ancora_forte", "02.sem_chave_fora", str(d))

    d = av(_cand(F, pagina=""), [p43])
    r.check(d.motivo == "fora:sem_pagina_de_resgate", "02.sem_pagina", str(d))
    d = av(_cand(F, publicada=""), [p43])
    r.check(d.motivo == "fora:sem_pagina_de_resgate", "02.sem_link_publicado", str(d))

    d = av(_cand(F), [replace(p43, plat="mercadolivre")])
    r.check(d.motivo == "sem_par", "02.outra_plataforma_nao_e_par", str(d))
    d = av(_cand(F), [])
    r.check((d.acao, d.motivo, d.destino_previsto) == (regra.MANTER, "sem_par", "principal+cupons"),
            "02.sem_par_previsto_dois_canais", str(d))

    d = av(_cand(F), [_vivo(31943, PUB_31943, fontes=(PROMOTOM, FADA))])
    r.check((d.acao, d.motivo) == (regra.INCERTO, "mesma_fonte"), "02.mesma_fonte", str(d))
    d = av(_cand(F), [_vivo(31943, PUB_31943, idade=SUP + 1)])
    r.check((d.acao, d.motivo) == (regra.INCERTO, "acima_do_superior"), "02.acima_do_superior", str(d))
    d = av(_cand(F), [_vivo(31943, PUB_31943, idade=SUP)])
    r.check(d.acao == regra.JUNTA, "02.no_limite_superior_junta", str(d))

    # SINTÉTICO (textos reais, par que o corpus não tem): nome em conflito
    # (Fada 17870 "VIP" de 17/10 × Fada 18042 "Esquenta" de 28/10)
    d = av(_cand(F), [_vivo(29549, F_17870.replace("40aG8LLgaE", "AAHdNLxGSX"))])
    r.check((d.acao, d.motivo) == (regra.INCERTO, "nome_ou_beneficio_em_conflito"),
            "02.nome_em_conflito_nunca_junta", str(d))
    # SINTÉTICO: benefício em conflito (30% OFF da Fada 18025 × 10% OFF do post 31299)
    d = av(_cand(F_18025), [_vivo(31299, PUB_31299, fontes=(SAMUEL,))])
    r.check((d.acao, d.motivo) == (regra.INCERTO, "nome_ou_beneficio_em_conflito"),
            "02.beneficio_em_conflito_nunca_junta", str(d))
    # dois posts válidos → ambíguo
    d = av(_cand(F), [p43, _vivo(31566, "🚨 Novos Cupons Shopee\n\nhttps://s.shopee.com.br/AAHdNLxGSX",
                                fontes=(SAMUEL,))])
    r.check((d.acao, d.motivo) == (regra.INCERTO, "ambiguo"), "02.dois_pares_ambiguo", str(d))
    r.check(set(d.outros) == {31943, 31566}, "02.ambiguo_lista_os_dois", str(d.outros))

    # com família escolhida pelo pipeline, a regra nunca troca de post
    d = av(_cand(F, familia_hoje=31943, espelho_hoje=True), [p43])
    r.check((d.acao, d.post, d.motivo) == (regra.MANTER, 31943, "ja_no_mesmo_post"),
            "02.familia_ja_e_o_par", str(d))
    d = av(_cand(F, familia_hoje=31451), [p43])
    r.check((d.acao, d.post, d.motivo) == (regra.INCERTO, 31943, "familia_de_hoje_diferente"),
            "02.familia_diferente_incerto", str(d))
    d = av(_cand(F, familia_hoje=31451), [])
    r.check((d.acao, d.destino_previsto) == (regra.MANTER, d.destino_hoje),
            "02.familia_sem_par_nao_muda_canal", str(d))


# ══════════════════════════════════════════════════════════════════
# 03 — caso real 28/10
# ══════════════════════════════════════════════════════════════════
def test_03_caso_real_2810(r):
    noite, cap = cenario(_caso_real)
    pa, pf = noite.post(PROMOTOM, 112024), noite.post(FADA, 18042)
    ps, pp = noite.post(SAMUEL, 123470), noite.post(FUMOTOM, 42674)
    cli = noite.cli
    # hoje (inalterado): 2 posts de campanha + o produto; cópia só da Promotom
    r.check(pa is not None and pf is not None and pa != pf, "03.hoje_dois_posts", f"{pa} {pf}")
    r.check(ps == pf, "03.hoje_samuel_no_post_da_fada", f"{ps} {pf}")
    r.check("DUP_SILENCIOSO" in T.MOTIVOS, "03.hoje_samuel_dup_silencioso", str(T.MOTIVOS))
    r.check(cli.principal() == sorted([pa, pf, pp]), "03.hoje_principal", str(cli.principal()))
    r.check(len(cli.cupons()) == 1 and db_espelho_get(pa) == cli.cupons()[0]
            and not db_espelho_get(pf), "03.hoje_so_a_promotom_no_canal_de_cupons",
            f"{cli.cupons()} {db_espelho_get(pa)} {db_espelho_get(pf)}")
    r.check(db_ofertas_de_post(pa) == ["shopee|cupb|geral"], "03.ident_promotom_como_no_log",
            str(db_ofertas_de_post(pa)))
    r.check(db_ofertas_de_post(pf) == ["shopee|url|" + PAGINA], "03.ident_fada_como_no_log",
            str(db_ofertas_de_post(pf)))
    r.check("shopee|1930141978.58270173531" in db_ofertas_de_post(pp), "03.ident_produto_como_no_log",
            str(db_ofertas_de_post(pp)))
    # a sombra
    linhas = [_ler(x) for x in cap.linhas]
    r.check(not cap.erros, "03.sem_erro", str(cap.erros))
    r.check(len(linhas) == 2, "03.duas_linhas", "\n".join(cap.linhas))
    if len(linhas) == 2:
        f, s = linhas
        r.check((f["msg"], f["acao"], f["post"], f["motivo"]) ==
                (18042, "JUNTA", pa, "mesma_pagina_de_resgate:compativel"),
                "03.fada_junta_no_post_da_promotom", cap.linhas[0])
        r.check((f["hoje"], f["previsto"]) == ("principal", "principal+cupons"),
                "03.fada_iria_aos_dois_canais", cap.linhas[0])
        r.check("hoje=post_novo" in cap.linhas[0] and "Δ=0." in cap.linhas[0],
                "03.fada_hoje_post_novo_delta", cap.linhas[0])
        r.check(f"ident_atual=['shopee|url|{PAGINA}']" in cap.linhas[0]
                and f"'shopee|resgate|{PAGINA}'" in cap.linhas[0],
                "03.identidade_atual_e_proposta", cap.linhas[0])
        r.check("nome=(esquent|-) beneficio=(-|-)" in cap.linhas[0], "03.nome_e_beneficio",
                cap.linhas[0])
        r.check((s["msg"], s["acao"], s["post"], s["motivo"], s["outros"]) ==
                (123470, "MANTER", pf, "ja_no_mesmo_post", [pa]),
                "03.samuel_segue_o_dedup_e_registra_o_risco", cap.linhas[1])
    # nada de texto nem link publicado na linha
    r.check(not any(PUB_PAGINA in x or "Resgate" in x for x in cap.linhas),
            "03.linha_sem_texto_nem_link_publicado", "\n".join(cap.linhas))


def test_03b_1710_vip(r):
    async def corpo(noite):
        await noite.chega(PROMOTOM, 111508, P_111508)
        noite.recuar(168)                                       # +2,8 min
        await noite.chega(FADA, 17870, F_17870)
    noite, cap = cenario(corpo)
    pa, pf = noite.post(PROMOTOM, 111508), noite.post(FADA, 17870)
    linhas = [_ler(x) for x in cap.linhas]
    # hoje: a regra de cupom (ATIVO) fica em INCERTO sem_beneficio → 2 posts
    r.check(pa != pf and db_ofertas_de_post(pf) == ["shopee|cupb|vip"]
            and len(noite.cli.cupons()) == 2, "03b.hoje_dois_posts_nos_dois_canais",
            f"{pa} {pf} {db_ofertas_de_post(pf)} {noite.cli.cupons()}")
    # a página como link principal dos dois lados é a prova que faltava
    r.check(len(linhas) == 1 and linhas[0]["acao"] == "JUNTA" and linhas[0]["post"] == pa
            and (linhas[0]["hoje"], linhas[0]["previsto"]) == ("principal+cupons", "principal+cupons"),
            "03b.vip_junta_no_post_da_promotom", "\n".join(cap.linhas))
    r.check("nome=(vip|-)" in (cap.linhas or [""])[0], "03b.nome_de_um_lado_so", str(cap.linhas))


# ══════════════════════════════════════════════════════════════════
# 04 — a sombra não muda nada
# ══════════════════════════════════════════════════════════════════
def test_04_sombra_nao_muda_nada(r):
    def rodada(variante):
        real_reg, real_vivos = sombra.registrar, sombra._vivos
        escritas, retornos = [], []

        def espiao(*a, **k):
            con = database_conexao._db_conn
            antes = con.total_changes
            ret = real_reg(*a, **k)
            escritas.append(con.total_changes - antes)
            retornos.append(ret)
            return ret

        def quebra(_plat):
            raise RuntimeError("leitura quebrada")
        if variante == "desligada":
            sombra.registrar = lambda *a, **k: None
        else:
            sombra.registrar = espiao
        if variante == "quebrando":
            sombra._vivos = quebra
        try:
            noite, cap = cenario(_caso_real)
            return noite.cli.chamadas, _banco(), cap, escritas, retornos, list(T.MOTIVOS)
        finally:
            sombra.registrar, sombra._vivos = real_reg, real_vivos

    base = rodada("desligada")
    for v in ("ligada", "quebrando", "desligada"):
        cham, banco, cap, escritas, retornos, motivos = rodada(v)
        r.check(cham == base[0], f"04.{v}.mesmas_chamadas_ao_telegram",
                f"{len(cham)} × {len(base[0])}")
        r.check(banco == base[1], f"04.{v}.mesmo_banco",
                str([t for t in banco if banco[t] != base[1].get(t)]))
        r.check(motivos == base[5], f"04.{v}.mesmas_decisoes", f"{motivos} × {base[5]}")
        if v != "desligada":
            r.check(len(escritas) == 4 and not any(escritas), f"04.{v}.nenhuma_escrita_no_sqlite",
                    str(escritas))
            r.check(retornos == [None] * 4, f"04.{v}.sempre_none", str(retornos))
    _, _, cap_q, *_ = rodada("quebrando")
    r.check(cap_q.erros == ["❌ [RESGATE_SOMBRA] RuntimeError"] * 3 and not cap_q.linhas,
            "04.quebrando_vira_uma_linha_e_mais_nada", str(cap_q.erros))

    # nem o log que levanta escapa (log_out.info e log_out.error quebrados)
    class _Bomba(logging.Handler):
        def emit(self, rec):
            if "RESGATE_SOMBRA" in rec.getMessage():
                raise RuntimeError("log quebrado")

    def rodar_com_log_quebrado():
        b = _Bomba()
        log_out.addHandler(b)
        try:
            noite, _cap = cenario(_caso_real)
            return noite.cli.chamadas, _banco()
        finally:
            log_out.removeHandler(b)
    cham, banco = rodar_com_log_quebrado()
    r.check(cham == base[0] and banco == base[1], "04.log_que_levanta_nao_muda_nada")


# ══════════════════════════════════════════════════════════════════
# 05 — noites reais sem junção
# ══════════════════════════════════════════════════════════════════
def _sem_juncao(r, nome, corpo, posts_esperados, resolucao=RESOLUCAO):
    noite, cap = cenario(corpo, resolucao=resolucao)
    linhas = [_ler(x) for x in cap.linhas]
    r.check(not cap.erros, f"05.{nome}.sem_erro", str(cap.erros))
    r.check(not [x for x in linhas if x["acao"] == "JUNTA"], f"05.{nome}.nenhuma_juncao",
            "\n".join(cap.linhas))
    r.check(len(noite.cli.principal()) == posts_esperados, f"05.{nome}.posts_de_hoje",
            str(noite.cli.principal()))
    return noite, linhas


def test_05_noites_reais_sem_juncao(r):
    async def n2110(noite):                       # 12:00 Dia de Cupons × 12:03 Promotom
        await noite.chega(FADA, 17927, F_17927)
        await noite.chega(SAMUEL, 122435, S_122435)
        noite.recuar(214)
        await noite.chega(PROMOTOM, 111724, P_111724)
    noite, linhas = _sem_juncao(r, "2110_12h", n2110, 2)
    r.check(not linhas, "05.2110_12h.silencio", str(linhas))     # abstém: 4LK2yNECtS não é a página

    async def n2210(noite):                       # a redação da Fada entra (EVOLUI)
        await noite.chega(PROMOTOM, 111784, P_111784)
        noite.recuar(36)
        await noite.chega(FADA, 17953, F_17953)
        noite.recuar(8)
        await noite.chega(SAMUEL, 122635, S_122635)
    noite, linhas = _sem_juncao(r, "2210", n2210, 1)
    r.check(not linhas, "05.2210.silencio", str(linhas))
    # a lacuna (link do Samuel) não muda o resultado: com a página, igual
    hip = dict(RESOLUCAO)
    hip["https://s.shopee.com.br/60SYicbJh9"] = (PUB_PAGINA, PAGINA, "HIPÓTESE do teste")
    noite, linhas = _sem_juncao(r, "2210_hipotese_samuel_pagina", n2210, 1, hip)
    r.check(not linhas, "05.2210_hipotese.silencio", str(linhas))

    async def n2610(noite):                       # 3 links × Fada × Tech (página como 2º link)
        await noite.chega(PROMOTOM, 111915, P_111915)
        noite.recuar(129)
        await noite.chega(FADA, 17997, F_17997)
        noite.recuar(12)
        await noite.chega(SAMUEL, 123091, S_123091)
        noite.recuar(60)
        await noite.chega(FADA, 17998, F_17998)
        noite.recuar(22)
        await noite.chega(SAMUEL, 123093, S_123093)
    noite, linhas = _sem_juncao(r, "2610", n2610, 2)
    r.check(not linhas, "05.2610.silencio", str(linhas))

    async def n2710(noite):                       # 15:02 × 15:40 (Δ 37 min)
        await noite.chega(PROMOTOM, 111992, P_111992)
        noite.recuar(33)
        await noite.chega(FADA, 18025, F_18025)
        noite.recuar(14)
        await noite.chega(SAMUEL, 123345, S_123345)
        noite.recuar(2191)
        await noite.chega(FADA, 18028, F_18028)
    noite, linhas = _sem_juncao(r, "2710_15h", n2710, 2)
    r.check(not linhas, "05.2710_15h.silencio", str(linhas))


# ══════════════════════════════════════════════════════════════════
# 06 — produto com cupom
# ══════════════════════════════════════════════════════════════════
def test_06_produto_com_cupom(r):
    async def produto_antes(noite):               # ordem trocada (SINTÉTICO): o produto já no ar
        await noite.chega(FUMOTOM, 42674, FU_42674)
        await noite.chega(FADA, 18042, F_18042)
    noite, cap = cenario(produto_antes)
    linhas = [_ler(x) for x in cap.linhas]
    pp = noite.post(FUMOTOM, 42674)
    r.check(len(linhas) == 1 and linhas[0]["acao"] == "MANTER" and linhas[0]["motivo"] == "sem_par"
            and pp not in linhas[0]["outros"], "06.produto_no_ar_nao_e_par", "\n".join(cap.linhas))
    noite, cap = cenario(_caso_real)
    r.check(not [x for x in cap.linhas if "msg=(-1003775401737" in x], "06.produto_nunca_candidato",
            "\n".join(cap.linhas))


# ══════════════════════════════════════════════════════════════════
# 07 — exclusão manual
# ══════════════════════════════════════════════════════════════════
def test_07_exclusao_manual(r):
    out = {}

    async def corpo(noite):
        await noite.chega(PROMOTOM, 112024, P_112024)
        pa = noite.post(PROMOTOM, 112024)
        out["pa"] = pa
        out["feitos"] = await post_apagado.apagados([pa], "TELEGRAM")
        noite.cli.no_ar.get(str(config.GRUPO_DESTINO), {}).pop(pa, None)
        antes = len(noite.cli.chamadas)
        await noite.chega(FADA, 18042, F_18042)
        await noite.chega(SAMUEL, 123470, S_123470)
        out["depois"] = noite.cli.chamadas[antes:]
    noite, cap = cenario(corpo)
    pa = out["pa"]
    linhas = [_ler(x) for x in cap.linhas]
    r.check(out["feitos"] and out["feitos"][0][0] == pa, "07.dono_apagou", str(out["feitos"]))
    r.check(not [x for x in linhas if x["post"] == pa or pa in x["outros"]],
            "07.apagado_nunca_e_par", "\n".join(cap.linhas))
    r.check(linhas and linhas[0]["acao"] == "MANTER" and linhas[0]["motivo"] == "sem_par",
            "07.fada_sem_par", "\n".join(cap.linhas))
    r.check(not [c for c in out["depois"] if c[2] == pa], "07.apagado_nao_renasce",
            str(out["depois"]))
    r.check((db_get_post(pa) or {}).get("delete_status") == "ok", "07.continua_apagado",
            str(db_get_post(pa)))


# ══════════════════════════════════════════════════════════════════
# 08 — falha no reconhecimento do link nunca junta
# ══════════════════════════════════════════════════════════════════
def test_08_falha_de_reconhecimento(r):
    def com(**trocas):
        t = dict(RESOLUCAO)
        t.update(trocas)
        return t
    L_FADA, L_PROMO = "https://s.shopee.com.br/40aG8LLgaE", "https://s.shopee.com.br/2B5snGi9NX"
    variantes = {
        "fada_nao_converteu": com(**{L_FADA: (None, None, "falhou")}),
        "promotom_link1_nao_converteu": com(**{L_PROMO: (None, None, "falhou")}),
        "canonica_ainda_encurtada": com(**{L_FADA: (PUB_PAGINA, L_FADA, "expansão falhou")}),
        "outra_rota": com(**{L_FADA: (PUB_PAGINA, PAGINA + "-esquenta", "SINTÉTICO")}),
        "link_publicado_diferente": com(**{L_FADA: ("https://s.shopee.com.br/OUTRO00000", PAGINA,
                                                    "SINTÉTICO: cache por destino quebrado")}),
    }
    falhou = {"promotom_link1_nao_converteu": 112024}
    for nome, tabela in variantes.items():
        noite, cap = cenario(_caso_real, resolucao=tabela)
        linhas = [_ler(x) for x in cap.linhas]
        mid = falhou.get(nome, 18042)
        r.check(not cap.erros, f"08.{nome}.sem_erro", str(cap.erros))
        r.check(not [x for x in linhas if x["acao"] == "JUNTA" and x["msg"] == mid],
                f"08.{nome}.a_mensagem_do_link_falho_nunca_junta", "\n".join(cap.linhas))
        if nome == "fada_nao_converteu":
            # sem a Fada no ar, quem leva à página PROVADA é o Samuel: esse junta
            pa = noite.post(PROMOTOM, 112024)
            r.check([(x["msg"], x["acao"], x["post"]) for x in linhas] == [(123470, "JUNTA", pa)],
                    f"08.{nome}.samuel_com_link_provado_junta", "\n".join(cap.linhas))
        else:
            r.check(not [x for x in linhas if x["acao"] == "JUNTA"], f"08.{nome}.nenhuma_juncao",
                    "\n".join(cap.linhas))


# ══════════════════════════════════════════════════════════════════
# 09 — canais separados
# ══════════════════════════════════════════════════════════════════
def test_09_canais_separados(r):
    async def so_fada(noite):
        await noite.chega(FADA, 18042, F_18042)
    noite, cap = cenario(so_fada)
    pf = noite.post(FADA, 18042)
    linhas = [_ler(x) for x in cap.linhas]
    r.check(len(linhas) == 1 and (linhas[0]["acao"], linhas[0]["hoje"], linhas[0]["previsto"]) ==
            ("MANTER", "principal", "principal+cupons"),
            "09.evento_na_pagina_elegivel_sem_cupom_no_titulo", "\n".join(cap.linhas))
    r.check(noite.cli.cupons() == [] and not db_espelho_get(pf), "09.hoje_nao_vai_ao_canal_de_cupons",
            str(noite.cli.cupons()))

    async def so_promotom(noite):
        await noite.chega(PROMOTOM, 112024, P_112024)
    noite, cap = cenario(so_promotom)
    r.check(not cap.linhas and len(noite.cli.cupons()) == 1, "09.cupb_ja_vai_aos_dois_silencio",
            f"{cap.linhas} {noite.cli.cupons()}")

    async def esquenta_1111(noite):
        await noite.chega(FADA, 18038, F_18038)
    noite, cap = cenario(esquenta_1111)
    p = noite.post(FADA, 18038)
    r.check(db_ofertas_de_post(p) == ["shopee|url|https://shopee.com.br/m/11-11"],
            "09.1111_identidade_como_no_log", str(db_ofertas_de_post(p)))
    r.check(not cap.linhas and noite.cli.cupons() == [], "09.pagina_de_evento_nao_declarada_silencio",
            f"{cap.linhas} {noite.cli.cupons()}")


# ══════════════════════════════════════════════════════════════════
# 10 — SIMULAÇÃO DE TESTE: o que os aplicadores de hoje fariam
# ══════════════════════════════════════════════════════════════════
def test_10_simulacao_da_decisao(r):
    """NÃO é ativação: só neste teste, a família vira o post da decisão
    JUNTA (sombra.avaliar) e o resto é o pipeline de hoje."""
    capturadas = {}
    real_norm, real_lanc = normalizacao.normalizar, familia.lancamento_cupom

    async def norm_espiao(bruta):
        n = await real_norm(bruta)
        capturadas[(bruta.chat, bruta.msg_id)] = n
        return n

    def lanc(ofertas, texto, chat, msg_id, msg_id_rel, **k):
        rel, via = real_lanc(ofertas, texto, chat, msg_id, msg_id_rel, **k)
        n = capturadas.get((str(chat), msg_id))
        res = sombra.avaliar(n, ofertas, rel) if n is not None else None
        if res is not None and res[2].acao == regra.JUNTA:
            return res[2].post, via
        return rel, via
    normalizacao.normalizar, familia.lancamento_cupom = norm_espiao, lanc
    try:
        noite, _cap = cenario(_caso_real)
    finally:
        normalizacao.normalizar, familia.lancamento_cupom = real_norm, real_lanc
    pa, pf, ps = (noite.post(PROMOTOM, 112024), noite.post(FADA, 18042),
                  noite.post(SAMUEL, 123470))
    pp = noite.post(FUMOTOM, 42674)
    r.check(pa == pf == ps, "10.um_post_para_a_campanha", f"{pa} {pf} {ps}")
    r.check(noite.cli.principal() == sorted([pa, pp]), "10.principal_campanha_mais_produto",
            str(noite.cli.principal()))
    r.check(len(noite.cli.cupons()) == 1 and db_espelho_get(pa) == noite.cli.cupons()[0],
            "10.uma_copia_no_canal_de_cupons", f"{noite.cli.cupons()} {db_espelho_get(pa)}")


# ══════════════════════════════════════════════════════════════════
# 11 — só mensagem NOVA passa pela sombra
# ══════════════════════════════════════════════════════════════════
def test_11_edicao_nao_passa_pela_sombra(r):
    chamadas = []
    real = sombra.registrar

    def espiao(*a, **k):
        chamadas.append(a[0].msg_id)
        return real(*a, **k)

    async def corpo(noite):
        await noite.chega(PROMOTOM, 112024, P_112024)
        await noite.chega(FADA, 18042, F_18042)
        antes = list(chamadas)
        # a Fada edita a própria mensagem (o 18042 aparece "editado" na prévia)
        n = noite.norms[(FADA, 18042)]
        await publicacao.enviar(await montar(n), n, enr=enriquecer_edicao(n), is_edit=True)
        await _drenar()
        noite.depois_da_edicao = chamadas[len(antes):]
    sombra.registrar = espiao
    try:
        noite, cap = cenario(corpo)
    finally:
        sombra.registrar = real
    r.check(chamadas[:2] == [112024, 18042], "11.mensagens_novas_passam", str(chamadas))
    r.check(noite.depois_da_edicao == [], "11.edicao_nao_passa", str(noite.depois_da_edicao))
    r.check(len(cap.linhas) == 1, "11.uma_linha_so", "\n".join(cap.linhas))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CAMPANHA POR PÁGINA DE RESGATE · sombra · caso real 28/10"))
