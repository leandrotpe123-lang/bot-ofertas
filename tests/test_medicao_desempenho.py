"""
P0 — MEDIÇÃO DE DESEMPENHO: o worker passa a medir sem mudar NADA do que faz.

Caso real (auditoria de 05/11, produção 456afa4): o log tem resolução de 1 s
e não mede o download da mídia, a conversão por plataforma e subetapa, a
espera nos locks e na fila, o atraso do event loop, o SQLite lento nem o
motivo de "🚫 Zero links convertidos" (~850 por dia, sem causa).

Contrato verificado aqui:
  01  sanitização: nenhum valor livre chega ao log (URL, @, espaço, token
      com barra) — só número, '-', '?' e rótulo; toda linha com hora em ms
  02  a medição NUNCA levanta, nem com o log quebrado nem com lixo
  03  download_media: ok / pequeno / vazio / timeout / erro, bytes e tipo
      (foto, vídeo); o retorno da função é o de sempre
  04  Mercado Livre: etapas expandir / descoberta / createlink e o motivo
      de cada falha (expansão, descoberta, createLink, timeout, URL e
      credencial recusadas, sem credencial, link inválido)
  05  EQUIVALÊNCIA do ML: a MESMA entrada dá o MESMO resultado e as MESMAS
      chamadas com a medição ligada e com ela neutralizada (no-op)
  06  descoberta real do ML: timeout e HTTP ≠ 200 viram motivo; a
      resposta continua None
  07  Shopee e Magalu: etapas reais e motivos; equivalência com no-op
  08  [CONV_MS] pelo caminho real do core (_normalizar_medido) com etapas
  09  [LINKS_ZERO] pelo normalizar REAL: motivo nao_suportada, sem URL
  10  locks: espera medida sob contenção, uma linha por dono, aninhados na
      ficha do dono; exceção solta o lock; cancelamento na espera não
      deixa ficha nem lock preso
  11  publicacao.py: os 4 locks embrulhados, mesmo factory, mesma ordem
  12  fila: ordem por chat preservada, espera da lane medida, sem resto
  13  event loop: atraso detectado, resumo com percentis, uma instância só,
      cancelamento limpo; main cria e encerra uma vez
  14  SQLite: operação lenta vira [DB_LENTO] com o nome de quem abriu;
      rápida não; erro passa inalterado e o mutex é solto; sem SQL/valor
  15  guarda estrutural: nos módulos instrumentados, valor medido nunca
      alimenta lógica (só volta para a própria medição)

    python tests/test_medicao_desempenho.py
"""
import ast
import asyncio
import io
import logging
import os
import re
import sqlite3
import sys
import time
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _harness_e5 as harness                                   # noqa: E402

harness.preparar()

import config                                                   # noqa: E402
import globals as g                                             # noqa: E402
import medicao_desempenho as md                                 # noqa: E402
import plataformas                                              # noqa: E402
from database import _init_db                                   # noqa: E402
from database_conexao import _db                                # noqa: E402
from pipeline import montagem_imagem, normalizacao, normalizacao_links as nl  # noqa: E402
from pipeline import orchestrator_fila                          # noqa: E402
from pipeline.ingestao import MensagemBruta, links_de           # noqa: E402
from plataformas import magalu                                  # noqa: E402
from plataformas.mercadolivre import afiliacao as ml, afiliado as ml_afiliado  # noqa: E402
from plataformas.mercadolivre import cliente as ml_cliente, descoberta as ml_desc  # noqa: E402
from plataformas.mercadolivre import sessao as ml_sessao        # noqa: E402
from plataformas.contrato import AUSENTE, Afiliacao             # noqa: E402
from plataformas.shopee import afiliacao as shp                 # noqa: E402

plataformas.inicializar()
_init_db()

# CONTRATO DE FORMATO de toda linha nova.
_LINHA = re.compile(r"⏱ \[[A-Z_]+\] hora=\d\d:\d\d:\d\d\.\d{3}( [a-z][a-z0-9_]*=([A-Za-z0-9_.:,-]+|\?))*")


class _Captura(logging.Handler):
    def __init__(self, *nomes):
        super().__init__(logging.DEBUG)
        self.linhas = []
        self.lgs = [logging.getLogger(n) for n in (nomes or ("SISTEMA",))]

    def emit(self, record):
        self.linhas.append(record.getMessage())

    def __enter__(self):
        for lg in self.lgs:
            lg.addHandler(self)
        return self

    def __exit__(self, *exc):
        for lg in self.lgs:
            lg.removeHandler(self)
        return False

    def de(self, marcador):
        return [m for m in self.linhas if m.startswith(f"⏱ [{marcador}]")]


def _campos(linha):
    return dict(kv.split("=", 1) for kv in linha.split()[3:])


class _Troca:
    """Troca atributos e devolve no fim (monkeypatch da casa, sem pytest)."""
    def __init__(self, *trocas):
        self.trocas, self.antes = trocas, []

    def __enter__(self):
        for alvo, nome, valor in self.trocas:
            self.antes.append((alvo, nome, getattr(alvo, nome)))
            setattr(alvo, nome, valor)
        return self

    def __exit__(self, *exc):
        for alvo, nome, valor in reversed(self.antes):
            setattr(alvo, nome, valor)
        return False


def _rodar(coro_fn):
    async def principal():
        g._init_globals()
        return await coro_fn()
    return asyncio.run(principal())


# ─────────────────────────────────────────────────────────────────
def test_01_sanitizacao_e_formato(r):
    r.check(md._v("https://meli.la/AbC123") == "?", "url", md._v("https://meli.la/x"))
    r.check(md._v("meli.la/x") == "?", "caminho")
    r.check(md._v("@promotom") == "?", "arroba")
    r.check(md._v("dois tokens") == "?", "espaço")
    r.check(md._v("HTTP_x") == "?", "http em qualquer caixa")
    r.check(md._v(12) == "12" and md._v(3.6) == "4" and md._v(None) == "-"
            and md._v(True) == "1", "números")
    r.check(md._v("mercadolivre.falha_descoberta") == "mercadolivre.falha_descoberta",
            "rótulo passa")
    with _Captura() as cap:
        md._emitir("TESTE_X", ("a", 1), ("url", "https://x.com/segredo?token=1"))
    r.check(len(cap.linhas) == 1 and _LINHA.fullmatch(cap.linhas[0]) is not None,
            "contrato da linha", cap.linhas)
    r.check(all("segredo" not in l and "x.com" not in l for l in cap.linhas), "sem vazamento",
            cap.linhas)


def test_02_medicao_nunca_levanta(r):
    def quebra(*a, **k):
        raise RuntimeError("log quebrado")
    lixo = object()
    with _Troca((md.log_sys, "info", quebra)):
        try:
            md.midia(lixo, lixo, lixo, lixo)
            md.etapa(lixo, lixo, lixo, lixo)
            md.motivo(lixo)
            md.afiliou(lixo, lixo, lixo)
            md.links_zero(lixo, lixo, lixo, {"a": lixo})
            md.fila(lixo, lixo, lixo, lixo, lixo)
            md.db(lixo, lixo, lixo, lixo)
            md.guardar_motivos(lixo)
            md.tirar_motivos()
            md.admitida(lixo)
            md._emitir("X", ("a", 1))               # a última barreira, sozinha
            with md.cronometro("x", "y"):
                pass
            ok = True
        except Exception as e:                       # noqa: BLE001
            ok = False
            r.check(False, "levantou", repr(e))
    r.check(ok, "nenhuma chamada levanta")


# ── 03 download de mídia ─────────────────────────────────────────
class MessageMediaPhoto:
    pass


class MessageMediaDocument:
    def __init__(self, mime):
        self.document = types.SimpleNamespace(mime_type=mime)


class _Msg:
    def __init__(self, media):
        self.media = media


class _ClienteMidia:
    def __init__(self, modo, n=1000):
        self.modo, self.n = modo, n

    async def download_media(self, obj, file=None):
        if self.modo == "timeout":
            await asyncio.sleep(1)
        if self.modo == "erro":
            raise ValueError("https://cdn.exemplo/segredo-123")
        if self.modo == "vazio":
            return None
        file.write(b"x" * self.n)
        return file


def test_03_download_de_midia(r):
    import client as modulo_cliente
    casos = [("ok", 1000, MessageMediaPhoto(), "ok", "foto", True),
             ("pequeno", 100, MessageMediaPhoto(), "pequeno", "foto", False),
             ("vazio", 0, MessageMediaPhoto(), "vazio", "foto", False),
             ("timeout", 0, MessageMediaDocument("video/mp4"), "timeout", "video", False),
             ("erro", 0, MessageMediaDocument("image/webp"), "erro", "imagem", False)]
    for modo, n, media, esperado, tipo, devolve in casos:
        with _Troca((modulo_cliente, "client", _ClienteMidia(modo, n)),
                    (config, "_TIMEOUT_DOWNLOAD_MIDIA", 0.05)), _Captura("SISTEMA", "ENRICH") as cap:
            buf = _rodar(lambda: montagem_imagem.preparar_imagem_tg(_Msg(media)))
        linhas = cap.de("MIDIA_MS")
        r.check(len(linhas) == 1, f"03.{modo}.uma_linha", cap.linhas)
        if not linhas:
            continue
        c = _campos(linhas[0])
        r.check(c.get("resultado") == esperado and c.get("tipo") == tipo, f"03.{modo}.campos", c)
        r.check(int(c.get("ms", "-1")) >= 0, f"03.{modo}.ms", c)
        r.check((buf is not None) == devolve, f"03.{modo}.retorno_de_sempre", repr(buf))
        if devolve:
            r.check(c.get("bytes") == "1000" and buf.name == "imagem.jpg"
                    and buf.getbuffer().nbytes == 1000, "03.ok.bytes", c)
        r.check(all("segredo" not in l for l in cap.de("MIDIA_MS")), f"03.{modo}.sem_url")
    r.check(md._tipo_midia(_Msg(MessageMediaDocument("application/pdf"))) == "documento"
            and md._tipo_midia(_Msg(object())) == "outro", "03.tipos")


# ── 04/05 Mercado Livre ──────────────────────────────────────────
_SOCIAL = "https://www.mercadolivre.com.br/social/terceiro?ref=abc&matt_word=terceiro"
_PRODUTO = "https://www.mercadolivre.com.br/p/MLB16016316"
_SEQ = iter(range(10 ** 6))


def _meli():
    return f"https://meli.la/T{next(_SEQ)}x"


class _CenarioML:
    """Stubs nas FRONTEIRAS de rede do ML; o código do ML roda inteiro."""
    def __init__(self, expandir="social", descobrir="produto", criar="ok", sessao="ok"):
        self.cfg = dict(expandir=expandir, descobrir=descobrir, criar=criar, sessao=sessao)
        self.chamadas = []

    async def desencurtar(self, url, sessao):
        self.chamadas.append("desencurtar")
        e = self.cfg["expandir"]
        if e == "fora":
            return "https://outro-site.exemplo/x"
        return _SOCIAL

    async def descobrir(self, url, sessao):
        self.chamadas.append("descobrir")
        if self.cfg["descobrir"] is None:
            return None
        return ml_desc.Destino(tipo="produto", url=_PRODUTO)

    async def criar_link(self, alvo, credencial, geracao=0, sessao_http=None):
        self.chamadas.append(("criar_link", alvo))
        c = self.cfg["criar"]
        if c == "ok":
            return ml_cliente.LinkAfiliado(publicada="https://meli.la/NOSSO1",
                                           canonica="https://www.mercadolivre.com.br/social/nosso",
                                           tag="nosso")
        if c == "url":
            raise ml_cliente.UrlRecusada("error_code 111")
        if c == "timeout":
            raise ml_cliente.ErroCliente("inesperado: TimeoutError")
        raise ml_cliente.ErroCliente("rede: ClientOSError")

    def trocas(self):
        disp = self.cfg["sessao"] == "ok"
        return _Troca(
            (ml, "desencurtar", self.desencurtar),
            (ml_desc, "descobrir", self.descobrir),
            (ml_cliente, "criar_link", self.criar_link),
            (ml_afiliado, "configurado", lambda: True),
            (ml_sessao, "disponivel", lambda: disp),
            (ml_sessao, "expirada", lambda: False),
            (ml_sessao, "carregar", lambda: ml_sessao.Credencial(cookie="c", csrf="x")),
            (ml_sessao, "geracao", lambda: 0),
        )


async def _afilia_com_ficha(url):
    with md.link() as ficha:
        res = await ml.afilia(url, None)
    return res, ficha


def _resumo(res):
    if res is AUSENTE:
        return "AUSENTE"
    return (getattr(res, "publicada", res), getattr(res, "canonica", res))


_CENARIOS_ML = [
    ("ok", dict(), [("expandir", True), ("descoberta", True), ("createlink", True)], None),
    ("expansao_fora", dict(expandir="fora"), [("expandir", False)], "falha_expansao"),
    ("sem_destino", dict(descobrir=None), [("expandir", True), ("descoberta", False)],
     "falha_descoberta"),
    ("createlink_rede", dict(criar="rede"),
     [("expandir", True), ("descoberta", True), ("createlink", False), ("createlink_2", False)],
     "falha_afiliado"),
    ("createlink_timeout", dict(criar="timeout"),
     [("expandir", True), ("descoberta", True), ("createlink", False), ("createlink_2", False)],
     "timeout_afiliado"),
    ("url_recusada", dict(criar="url"),
     [("expandir", True), ("descoberta", True), ("createlink", False)], "url_recusada"),
    ("sem_credencial", dict(sessao="nao"), [("expandir", True), ("descoberta", True)],
     "sem_credencial"),
]


def test_04_ml_etapas_e_motivos(r):
    for nome, cfg, etapas, motivo in _CENARIOS_ML:
        cen = _CenarioML(**cfg)
        with cen.trocas():
            res, ficha = _rodar(lambda: _afilia_com_ficha(_meli()))
        r.check([(n, ok) for n, _, ok in ficha.etapas] == etapas, f"04.{nome}.etapas",
                ficha.etapas)
        r.check(ficha.motivo == motivo, f"04.{nome}.motivo", ficha.motivo)
        r.check((res is AUSENTE) == (motivo is not None), f"04.{nome}.resultado", _resumo(res))
    res, ficha = _rodar(lambda: _afilia_com_ficha("https://exemplo.com/x"))
    r.check(res is AUSENTE and ficha.motivo == "link_invalido", "04.link_invalido", ficha.motivo)


def _neutralizada():
    """A medição INTEIRA vira no-op — é o código de antes, pela prova AST."""
    class _Nulo:
        def __init__(self, *a, **k):
            self.ok = True

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False
    nada = lambda *a, **k: None                       # noqa: E731
    return _Troca((md, "etapa", nada), (md, "motivo", nada), (md, "afiliou", nada),
                  (md, "cronometro", _Nulo), (md, "agora", lambda: 0.0))


def test_05_ml_equivalencia_ligada_x_neutralizada(r):
    for nome, cfg, _e, _m in _CENARIOS_ML:
        saidas = []
        for neutra in (False, True):
            cen = _CenarioML(**cfg)
            with cen.trocas():
                if neutra:
                    with _neutralizada():
                        res, _f = _rodar(lambda: _afilia_com_ficha(_meli()))
                else:
                    res, _f = _rodar(lambda: _afilia_com_ficha(_meli()))
            saidas.append((_resumo(res), cen.chamadas))
        r.check(saidas[0] == saidas[1], f"05.{nome}.mesmo_resultado_mesmas_chamadas", saidas)
    r.check(_CenarioML().cfg["criar"] == "ok", "05.cenario_ok_existe")


# ── 06 descoberta real ───────────────────────────────────────────
class _RespHTTP:
    def __init__(self, status, corpo="", erro=None):
        self.status, self.corpo, self.erro = status, corpo, erro

    async def __aenter__(self):
        if self.erro:
            raise self.erro
        return self

    async def __aexit__(self, *exc):
        return False

    async def text(self, errors=None):
        return self.corpo


class _SessaoHTTP:
    def __init__(self, resp):
        self.resp, self.metodos = resp, []

    def get(self, url, **k):
        self.metodos.append("get")
        return self.resp

    def head(self, url, **k):
        self.metodos.append("head")
        return self.resp


def test_06_descoberta_real_motivos(r):
    for nome, resp, motivo in (
            ("timeout", _RespHTTP(200, erro=asyncio.TimeoutError()), "timeout_descoberta"),
            ("conexao", _RespHTTP(200, erro=OSError("x")), "falha_descoberta"),
            ("http_403", _RespHTTP(403), "descoberta_http")):
        async def corpo(resp=resp):
            with md.link() as f:
                d = await ml_desc.descobrir(_SOCIAL, _SessaoHTTP(resp))
            return d, f
        d, f = _rodar(corpo)
        r.check(d is None and f.motivo == motivo, f"06.{nome}", (d, f.motivo))

    # O PRIMEIRO motivo vence: o timeout da descoberta (camada funda) chega
    # antes da falha genérica que _resolver_alvo registra em seguida.
    async def resolver_com_timeout():
        with md.link() as f:
            alvo = await ml._resolver_alvo(_SOCIAL, _SessaoHTTP(_RespHTTP(200, erro=asyncio.TimeoutError())))
        return alvo, f
    alvo, f = _rodar(resolver_com_timeout)
    r.check(alvo is None and f.motivo == "timeout_descoberta"
            and [(n, ok) for n, _, ok in f.etapas] == [("descoberta", False)],
            "06.primeiro_motivo_vence", (alvo, f.motivo, f.etapas))


# ── 07 Shopee e Magalu ───────────────────────────────────────────
def test_07_shopee_e_magalu(r):
    _PROD_SHP = "https://shopee.com.br/Produto-X-i.123456.7891011"

    async def desenc_ok(url, sessao):
        return _PROD_SHP

    async def desenc_curto(url, sessao):
        return url

    async def desenc_erro(url, sessao):
        raise OSError("caiu")

    async def api_ok(url, sessao):
        return "https://s.shopee.com.br/NOSSO7"

    async def api_nada(url, sessao):
        return None

    casos = [("ok", desenc_ok, api_ok, [("expandir", True), ("api", True)], None),
             ("expansao_erro", desenc_erro, api_ok, [("expandir", False)], "falha_expansao"),
             ("nao_resolveu", desenc_curto, api_ok, [("expandir", False)], "falha_expansao"),
             ("api_falha", desenc_ok, api_nada, [("expandir", True), ("api", False)],
              "falha_afiliado")]
    for nome, desenc, api, etapas, motivo in casos:
        saidas = []
        for neutra in (False, True):
            curta = f"https://s.shopee.com.br/c{next(_SEQ)}"

            async def corpo(curta=curta):
                with md.link() as f:
                    res = await shp.afilia(curta, None)
                return res, f
            with _Troca((shp, "desencurtar", desenc), (shp, "_chamar_servico_afiliados", api)):
                if neutra:
                    with _neutralizada():
                        res, f = _rodar(corpo)
                else:
                    res, f = _rodar(corpo)
                    r.check([(n, ok) for n, _, ok in f.etapas][:len(etapas)] == etapas,
                            f"07.shopee.{nome}.etapas", f.etapas)
                    r.check(f.motivo == motivo, f"07.shopee.{nome}.motivo", f.motivo)
            saidas.append(_resumo(res))
            for u in (curta, _PROD_SHP, shp.limpa_url(_PROD_SHP)):   # cada rodada do zero
                shp.descartar_link(u)
        r.check(saidas[0] == saidas[1], f"07.shopee.{nome}.equivalencia", saidas)

    # Timeout real dentro do serviço de afiliados → motivo timeout_afiliado.
    class _SessTimeout:
        def post(self, *a, **k):
            return _RespHTTP(200, erro=asyncio.TimeoutError())

    async def corpo_t():
        with md.link() as f:
            link = await shp._chamar_servico_afiliados("https://shopee.com.br/x-i.1.2", _SessTimeout())
        return link, f
    with _Troca((shp, "_ESPERA_ENTRE_TENTATIVAS", 0)):
        link, f = _rodar(corpo_t)
    r.check(link is None and f.motivo == "timeout_afiliado", "07.shopee.timeout", f.motivo)

    for nome, desenc, etapas, motivo in (
            ("expansao_erro", desenc_erro, [("expandir", False)], "falha_expansao"),
            ("fora", desenc_ok, [("expandir", False)], "expansao_fora_do_dominio")):
        saidas = []
        for neutra in (False, True):
            async def corpo():
                with md.link() as f:
                    res = await magalu.afilia(f"https://maga.lu/m{next(_SEQ)}", None)
                return res, f
            with _Troca((magalu, "desencurtar", desenc)):
                if neutra:
                    with _neutralizada():
                        res, f = _rodar(corpo)
                else:
                    res, f = _rodar(corpo)
                    r.check([(n, ok) for n, _, ok in f.etapas] == etapas,
                            f"07.magalu.{nome}.etapas", f.etapas)
                    r.check(f.motivo == motivo, f"07.magalu.{nome}.motivo", f.motivo)
            saidas.append(_resumo(res))
        r.check(saidas == ["AUSENTE", "AUSENTE"], f"07.magalu.{nome}.equivalencia", saidas)


# ── 08 [CONV_MS] pelo core ───────────────────────────────────────
def test_08_conv_ms_pelo_core(r):
    from collections import Counter
    cen = _CenarioML(criar="rede")
    motivos = Counter()
    with cen.trocas(), _Captura() as cap:
        res = _rodar(lambda: nl._normalizar_medido(_meli(), None, 1, motivos))
    linhas = cap.de("CONV_MS")
    r.check(res[1] is None and res[2] == "mercadolivre", "08.resultado_de_sempre", res)
    r.check(len(linhas) == 1 and _LINHA.fullmatch(linhas[0]) is not None, "08.uma_linha", linhas)
    if linhas:
        c = _campos(linhas[0])
        r.check(c.get("plat") == "mercadolivre" and c.get("resultado") == "ausente"
                and c.get("motivo") == "falha_afiliado", "08.campos", c)
        r.check(all(k in c for k in ("expandir", "descoberta", "createlink", "createlink_2")),
                "08.etapas_na_linha", c)
        r.check(re.fullmatch(r"\d+:ok", c.get("expandir", "")) is not None
                and c.get("createlink", "").endswith(":falha"), "08.formato_etapa", c)
    r.check(dict(motivos) == {"mercadolivre.falha_afiliado": 1}, "08.motivo_contado",
            dict(motivos))
    r.check(md._CONV == {}, "08.ficha_fechada", md._CONV)
    with cen.trocas(), _Captura() as cap:
        _rodar(lambda: nl._normalizar_medido("https://www.lojadesconhecida.com.br/p/1", None, 1,
                                              motivos))
    r.check(cap.de("CONV_MS") == [], "08.sem_plataforma_sem_conv", cap.linhas)


# ── 09 [LINKS_ZERO] pelo normalizar real ─────────────────────────
def test_09_links_zero_normalizar_real(r):
    texto = ("🔥 Fone Bluetooth Bom Demais\n\n💰 R$ 99,90 à vista\n\n"
             "https://www.lojadesconhecida.com.br/p/123?token=SEGREDO")
    bruta = MensagemBruta(msg_id=777001, chat="-1001825680721", texto=texto,
                          links=links_de(texto), tem_midia=False, media_obj=None)
    with _Captura("SISTEMA", "NORMAL") as cap:
        norm = _rodar(lambda: normalizacao.normalizar(bruta))
    r.check(norm is None, "09.descarta_como_sempre", norm)
    r.check(any("Zero links convertidos" in l for l in cap.linhas), "09.aviso_de_sempre",
            cap.linhas)
    linhas = cap.de("LINKS_ZERO")
    r.check(len(linhas) == 1, "09.uma_linha", cap.linhas)
    if linhas:
        c = _campos(linhas[0])
        r.check(c.get("id") == "777001" and c.get("links") == "1"
                and c.get("motivos") == "nao_suportada:1", "09.campos", c)
        r.check("SEGREDO" not in linhas[0] and "lojadesconhecida" not in linhas[0],
                "09.sem_url", linhas[0])
    r.check(len(md._MOTIVOS) == 0, "09.motivos_consumidos", len(md._MOTIVOS))


# ── 10/11 locks ──────────────────────────────────────────────────
def test_10_locks_espera_e_seguranca(r):
    async def corpo():
        lk, lk_id, lk_post = asyncio.Lock(), asyncio.Lock(), asyncio.Lock()
        ordem = []

        async def a():
            async with md.lock_medido("origem", lk):
                ordem.append("a")
                async with md.lock_medido("identidade", lk_id):
                    async with md.lock_medido("post", lk_post):
                        await asyncio.sleep(0.06)

        async def b():
            await asyncio.sleep(0.01)
            async with md.lock_medido("origem", lk) as x:
                ordem.append("b")
                return x

        _, x = await asyncio.wait_for(asyncio.gather(a(), b()), 3)   # lock preso reprova, não trava
        return ordem, x, lk.locked(), lk_id.locked(), lk_post.locked()
    with _Captura() as cap:
        ordem, x, *presos = _rodar(corpo)
    linhas = cap.de("LOCK_MS")
    r.check(ordem == ["a", "b"] and x is None and not any(presos), "10.mesma_semantica",
            (ordem, x, presos))
    r.check(len(linhas) == 2, "10.uma_linha_por_dono", linhas)
    if len(linhas) == 2:
        la, lb = (_campos(l) for l in linhas)
        r.check(la.get("dono") == "origem" and la.get("ident_n") == "1"
                and la.get("post_n") == "1" and int(la.get("protegido")) >= 50,
                "10.aninhados_na_ficha_do_dono", la)
        r.check(int(lb.get("origem")) >= 30 and lb.get("ident") == "-", "10.espera_medida", lb)
    r.check(md._FICHAS == {}, "10.sem_ficha_sobrando", md._FICHAS)

    async def com_erro():
        lk = asyncio.Lock()
        try:
            async with md.lock_medido("post", lk):
                raise KeyError("x")
        except KeyError:
            pass
        await asyncio.wait_for(lk.acquire(), 1)     # solto de verdade: dá para pegar de novo
        lk.release()
        return lk.locked()
    r.check(_rodar(com_erro) is False and md._FICHAS == {}, "10.excecao_solta_o_lock")

    async def cancelado():
        lk = asyncio.Lock()
        await lk.acquire()

        async def espera():
            async with md.lock_medido("origem", lk):
                return "nunca"
        t = asyncio.create_task(espera())
        await asyncio.sleep(0.01)
        t.cancel()
        res = await asyncio.gather(t, return_exceptions=True)
        lk.release()
        return isinstance(res[0], asyncio.CancelledError), lk.locked()
    cancelou, preso = _rodar(cancelado)
    r.check(cancelou and not preso and md._FICHAS == {}, "10.cancelado_na_espera",
            (cancelou, preso, md._FICHAS))


def test_11_publicacao_embrulha_os_quatro_locks(r):
    arv = ast.parse(harness.ler_fonte("pipeline/publicacao.py"))
    chamadas = [n for n in ast.walk(arv) if isinstance(n, ast.Call)
                and ast.unparse(n.func) == "medicao_desempenho.lock_medido"]
    chamadas.sort(key=lambda n: n.lineno)
    vistos = [(ast.unparse(c.args[0]), ast.unparse(c.args[1])) for c in chamadas]
    r.check(vistos == [("'origem'", "await origem.lock_origem(norm.chat, norm.msg_id)"),
                       ("'identidade'", "await exclusao.lock_identidade(chave)"),
                       ("'identidade'", "await exclusao.lock_identidade(chave)"),
                       ("'post'", "post_lock")], "11.quatro_locks", vistos)
    crus = [ast.unparse(n) for n in ast.walk(arv) if isinstance(n, (ast.AsyncWith,))
            for it in n.items if "lock_" in ast.unparse(it.context_expr)
            and "lock_medido" not in ast.unparse(it.context_expr)]
    r.check(crus == [], "11.nenhum_lock_sem_medicao_no_caminho", crus)


# ── 12 fila ──────────────────────────────────────────────────────
class _Evento:
    def __init__(self, chat, mid):
        self.chat_id = chat
        self.message = types.SimpleNamespace(id=mid)


def test_12_fila_ordem_e_espera(r):
    feitos = []

    consumida = []

    async def pipeline_falso(event, is_edit=False):
        feitos.append(event.message.id)
        consumida.append(asyncio.current_task() not in md._ADMISSAO)
        await asyncio.sleep(0.05)

    async def corpo():
        for mid in (1, 2, 3):
            await orchestrator_fila._enfileirar(_Evento(-100777, mid), False)
        await asyncio.sleep(0)
        while g._buf:
            await asyncio.gather(*list(g._buf), return_exceptions=True)
    with _Troca((orchestrator_fila, "_pipeline", pipeline_falso)), _Captura() as cap:
        _rodar(corpo)
    linhas = [_campos(l) for l in cap.de("FILA_MS")]
    r.check(feitos == [1, 2, 3], "12.ordem_por_chat", feitos)
    r.check([c.get("id") for c in linhas] == ["1", "2", "3"], "12.uma_linha_por_evento", linhas)
    if len(linhas) == 3:
        r.check(int(linhas[2]["lane"]) >= 80 and int(linhas[2]["espera"]) >= 80
                and linhas[0]["edit"] == "0" and linhas[0]["chat"] == "-100777",
                "12.espera_da_lane", linhas)
    r.check(len(md._ADMISSAO) == 0 and consumida == [True, True, True],
            "12.admissao_consumida_no_inicio", (len(md._ADMISSAO), consumida))


# ── 13 event loop ────────────────────────────────────────────────
def test_13_event_loop(r):
    async def corpo():
        t = asyncio.create_task(md.amostrar_loop(intervalo=0.02, limiar_ms=30, resumo_s=0.15))
        await asyncio.sleep(0.03)
        segunda = asyncio.create_task(md.amostrar_loop(intervalo=0.02))
        await asyncio.sleep(0)
        time.sleep(0.08)                            # bloqueia o loop de propósito
        await asyncio.sleep(0.25)
        segunda_saiu = segunda.done()
        t.cancel()
        res = await asyncio.gather(t, return_exceptions=True)
        return segunda_saiu, isinstance(res[0], asyncio.CancelledError), md._LOOP_ATIVO
    with _Captura() as cap:
        segunda_saiu, cancelou, ativo = _rodar(corpo)
    atrasos = [_campos(l) for l in cap.de("LOOP_ATRASO")]
    resumos = [_campos(l) for l in cap.de("LOOP_RESUMO")]
    r.check(any(int(a["atraso"]) >= 50 for a in atrasos), "13.atraso_detectado", atrasos)
    r.check(resumos and all(k in resumos[0] for k in ("n", "p50", "p90", "p95", "p99", "max")),
            "13.resumo_com_percentis", resumos)
    r.check(segunda_saiu and cancelou and not ativo, "13.instancia_unica_e_cancelamento",
            (segunda_saiu, cancelou, ativo))
    o = sorted([1, 2, 3, 4, 100])
    r.check(md._percentil(o, 50) == 3 and md._percentil(o, 99) == 100
            and md._percentil([], 50) == 0.0, "13.percentil_nearest_rank")

    arv = ast.parse(harness.ler_fonte("main.py"))
    cria = [ast.unparse(n) for n in ast.walk(arv) if isinstance(n, ast.Assign)
            and ast.unparse(n.targets[0]) == "_TASKS_FUNDO['loop']"]
    r.check(cria == ["_TASKS_FUNDO['loop'] = asyncio.create_task(medicao_desempenho.amostrar_loop())"],
            "13.main_cria_uma_vez", cria)
    enc = next(n for n in ast.walk(arv) if isinstance(n, ast.AsyncFunctionDef)
               and n.name == "_encerrar")
    corpo = enc.body
    i = next((k for k, st in enumerate(corpo)
              if ast.unparse(st) == "t = _TASKS_FUNDO.get('loop')"), None)
    bloco = [ast.unparse(x) for x in corpo[i + 1].body] if i is not None else []
    r.check(bloco == ["t.cancel()", "await asyncio.gather(t, return_exceptions=True)"],
            "13.main_cancela_e_aguarda", bloco)


# ── 14 SQLite ────────────────────────────────────────────────────
def _consulta_lenta(erro=None):
    with _db() as db:
        db.execute("SELECT ?", ("segredo-param-9",)).fetchone()
        time.sleep(0.06)
        if erro == "sql":
            db.execute("SELECT * FROM tabela_que_nao_existe")
        if erro == "valor":
            raise ValueError("de fora")


def test_14_sqlite(r):
    with _Captura("SISTEMA", "DB") as cap:
        with _db() as db:
            db.execute("SELECT 1").fetchone()
    r.check(cap.de("DB_LENTO") == [], "14.rapida_nao_registra", cap.linhas)
    with _Captura("SISTEMA", "DB") as cap:
        _consulta_lenta()
    linhas = [_campos(l) for l in cap.de("DB_LENTO")]
    r.check(len(linhas) == 1 and linhas[0].get("op") == "test_medicao_desempenho._consulta_lenta"
            and linhas[0].get("resultado") == "ok" and int(linhas[0]["ms"]) >= 50,
            "14.lenta_com_nome", linhas)
    r.check(all("segredo" not in l and "SELECT" not in l for l in cap.de("DB_LENTO")),
            "14.sem_sql_nem_valor", cap.de("DB_LENTO"))
    for erro, tipo in (("sql", sqlite3.OperationalError), ("valor", ValueError)):
        with _Captura("SISTEMA", "DB") as cap:
            try:
                _consulta_lenta(erro)
                passou = None
            except Exception as e:                   # noqa: BLE001
                passou = e
        r.check(type(passou) is tipo, f"14.{erro}.excecao_inalterada", repr(passou))
        linhas = [_campos(l) for l in cap.de("DB_LENTO")]
        r.check(len(linhas) == 1 and linhas[0].get("resultado") == "erro", f"14.{erro}.erro",
                linhas)
        r.check((erro == "sql") == any("❌ DB:" in l for l in cap.linhas),
                f"14.{erro}.log_de_sempre", cap.linhas)
        with _db() as db:
            r.check(db.execute("SELECT 2").fetchone()[0] == 2, f"14.{erro}.mutex_solto")


# ── 15 guarda estrutural ─────────────────────────────────────────
_INSTRUMENTADOS = ("pipeline/montagem_imagem.py", "pipeline/normalizacao.py",
                   "pipeline/normalizacao_links.py", "pipeline/publicacao.py",
                   "pipeline/orchestrator_fila.py", "plataformas/mercadolivre/afiliacao.py",
                   "plataformas/mercadolivre/descoberta.py", "plataformas/shopee/afiliacao.py",
                   "plataformas/magalu.py", "utils/url_resolver.py", "main.py",
                   "database_conexao.py")


def test_15_valor_medido_nunca_alimenta_logica(r):
    problemas = []
    for rel in _INSTRUMENTADOS:
        arv = ast.parse(harness.ler_fonte(rel))
        for fn in ast.walk(arv):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if fn.name == "_normalizar_medido":
                continue                            # a própria ficha (só medição)
            medidos = set()
            for n in ast.walk(fn):
                if (isinstance(n, ast.Assign) and isinstance(n.value, ast.Call)
                        and ast.unparse(n.value.func).startswith("medicao_desempenho.")):
                    medidos |= {t.id for t in n.targets if isinstance(t, ast.Name)}
            if not medidos:
                continue
            dentro = set()
            for n in ast.walk(fn):
                if isinstance(n, ast.Call) and ast.unparse(n.func).startswith("medicao_desempenho."):
                    dentro |= {id(x) for x in ast.walk(n)}
            for n in ast.walk(fn):
                if (isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
                        and n.id in medidos and id(n) not in dentro):
                    problemas.append(f"{rel}:{fn.name}:{n.id}")
    r.check(problemas == [], "15.so_volta_para_a_medicao", problemas)
    for rel in _INSTRUMENTADOS:
        src = harness.ler_fonte(rel)
        r.check("medicao_desempenho" in src, f"15.{rel}.instrumentado")


if __name__ == "__main__":
    sys.exit(harness.rodar(globals(), "P0 — MEDIÇÃO DE DESEMPENHO (só observa)"))
