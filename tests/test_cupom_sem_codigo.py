"""
CUPOM SEM CÓDIGO — nome declarado › vocabulário legado › assinatura › geral.

Incidente 01/10 00:00 (Shopee): Fidelidade, Tech e "R$30/R$90" — três
campanhas distintas, sem código — viraram a MESMA identidade
`shopee|cupb|geral`, porque o nome delas não estava no vocabulário fechado
de `tema_da_campanha`. Fidelidade e Tech foram descartadas como
SCORE_NAO_EVOLUI dentro do post do "Cupons Shopee" do Promotom.

O QUE ESTE ARQUIVO PROVA:
  01  corpus obrigatório — as 13 linhas, chave a chave
  02  os 6 furos que o corpus real (Shopee, 31/08–01/10) revelou
  03  classes do título: plataforma/moldura/reativação/valor/percentual
      nunca viram nome; > 3 palavras e título sem plataforma não declaram
  04  vocabulário legado: mesmas chaves de sempre; sem `plat` = antes
  05  resolvedor: níveis EXCLUSIVOS por mensagem (nome nunca leva
      assinatura); código e cashback intocados
  06  assinatura: canônica ("R$30" = "R$ 30"), N chaves, sempre FRACA
  07  família (caminho real): A=B na mesma assinatura → mesmo post
  08  família: Tech v:30-299 × genérico v:30-299 → posts diferentes
  09  família: Fidelidade × Clube Fidelidade; Black Friday × Black Week
  10  família: nome × assinatura; nome × geral → posts diferentes
  11  composição {v:30-299} × {v:30-299, v:90-899}: mesma família pelo
      overlap fraco, relacao_composicao None, decisão pelo SCORE
  12  replay real de 01/10 00:00 (textos e links reais, na ordem)

    python tests/test_cupom_sem_codigo.py
"""
import asyncio
import os
import sys
import types
from dataclasses import replace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar  # noqa: E402

preparar()

try:
    import yarl  # noqa: F401
except ImportError:
    _yarl = types.ModuleType("yarl")

    class _URL(str):
        def __new__(cls, valor="", **_kw):
            return str.__new__(cls, valor)
    _yarl.URL = _URL
    sys.modules["yarl"] = _yarl

import globals as g                                            # noqa: E402
import client as modulo_client                                 # noqa: E402
import plataformas                                             # noqa: E402
from database import _init_db                                   # noqa: E402
from database_conexao import _db                               # noqa: E402
from database_posts import db_ofertas_de_post                  # noqa: E402
from pipeline import identidade, exclusao, origem, familia     # noqa: E402
from pipeline import publicacao                                # noqa: E402
from pipeline.assunto_oferta import (                           # noqa: E402
    assinatura_do_beneficio, nome_declarado, tema_da_campanha)
from pipeline.enriquecimento import enriquecer                  # noqa: E402
from pipeline.identidade_oferta import identidades              # noqa: E402
from pipeline.montagem import montar                            # noqa: E402
from pipeline.normalizacao import MensagemNormalizada           # noqa: E402
from pipeline.normalizacao_identidade import (                  # noqa: E402
    derivar_produto, derivar_campanha, derivar_ancora_url,
    remover_cupons_da_entidade)
from pipeline.resolucao_identidade import (                     # noqa: E402
    Evidencias, eh_chave_forte, resolver)

plataformas.inicializar()
_init_db()

FADA, PROMOTOM, SAMUEL = -1002050488946, -1001825680721, -1001768101197
identidade._CHAT_USERNAME.update({
    FADA: "fadadoscupons", PROMOTOM: "promotom", SAMUEL: "samuelf3lipepromo"})


def chaves(texto, plat="shopee"):
    """Chaves cupb que o resolvedor emite para um cupom sem código."""
    return tuple(e.chave for e in resolver(Evidencias(
        plataforma=plat, entidade_cupom=True,
        tema_campanha=tema_da_campanha(texto, plat),
        assinaturas_beneficio=assinatura_do_beneficio(texto))))


# ─────────────────────────────────────────────────────────────────
# Telegram (I/O externo) e cenário — mesmo molde de test_familia_destino
# ─────────────────────────────────────────────────────────────────
class _Msg:
    def __init__(self, i):
        self.id, self.media = i, None


_ID_DESTINO = {"n": 51000}


def _proximo_id():
    _ID_DESTINO["n"] += 1
    return _ID_DESTINO["n"]


class Cliente:
    def __init__(self):
        self.novos, self.edits, self.deletes = 0, [], []
        self.ids = []

    async def send_message(self, dest, texto, parse_mode=None, link_preview=None):
        self.novos += 1
        m = _Msg(_proximo_id())
        self.ids.append(m.id)
        return m

    async def send_file(self, dest, img, caption=None, parse_mode=None,
                        force_document=False):
        self.novos += 1
        m = _Msg(_proximo_id())
        self.ids.append(m.id)
        return m

    async def edit_message(self, dest, msg_id, texto, parse_mode=None, file=None):
        self.edits.append((msg_id, texto))
        return _Msg(msg_id)

    async def delete_messages(self, dest, msg_id):
        self.deletes.append(msg_id)
        return True

    async def download_media(self, media, file=None):
        return None


MOTIVOS = []
_log_real = publicacao._log_decisao


def _espiao(d, *a, **k):
    MOTIVOS.append(d.motivo)
    return _log_real(d, *a, **k)


publicacao._log_decisao = _espiao


def _isolar_loop():
    g._init_globals()
    for pool in (exclusao._IDENTITY_LOCKS, exclusao._IDENTITY_LOCKS_TS,
                 exclusao._POST_LOCKS, exclusao._POST_LOCKS_TS,
                 origem._LOCKS, origem._LOCKS_TS):
        pool.clear()
    origem._LOCKS_LCK = asyncio.Lock()


def _zerar_banco():
    """Cada cenário começa sem posts vivos: aqui as assinaturas se
    repetem DE PROPÓSITO entre testes, e um post vivo de um cenário
    viraria família do seguinte."""
    with _db() as db:
        tabelas = [t for (t,) in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'")]
        for t in tabelas:
            db.execute(f"DELETE FROM {t}")


def cenario(corpo):
    async def _run():
        _zerar_banco()
        _isolar_loop()
        c = Cliente()
        modulo_client.client = c
        MOTIVOS.clear()
        await corpo(c)
        return c
    return asyncio.run(_run())


_SEQ = {"n": 870000}


def msg(chat, texto, urls_longas=(), cupons=(), plat="shopee", msg_id=None):
    """Espelha normalizar() a partir das URLs afiliadas LONGAS."""
    _SEQ["n"] += 1
    urls_longas = list(urls_longas)
    ids, idents, sku = derivar_produto(urls_longas)
    cupons = remover_cupons_da_entidade(list(cupons), ids)
    c = derivar_campanha(urls_longas, texto)
    mapa = {f"https://orig/{_SEQ['n']}/{i}": u for i, u in enumerate(urls_longas)}
    return MensagemNormalizada(
        msg_id=msg_id or _SEQ["n"], chat=str(chat), texto_limpo=texto,
        texto_analise=texto, mapa=mapa, preservar=[], plat=plat, sku=sku,
        tem_midia=False, media_obj=None, ids_globais=ids, idents=idents,
        cupons=cupons, chave_campanha=c.chave_campanha,
        chaves_campanha=c.chaves_campanha,
        tem_host_campanha=c.tem_host_campanha,
        tem_sinal_cashback=c.tem_sinal_cashback,
        destinos_declarados=list(c.destinos_declarados),
        ancora_url=derivar_ancora_url(urls_longas))


async def publicar(n, score=None):
    enr = enriquecer(n)
    if score is not None:
        enr = replace(enr, score=score)
    montada = await montar(n)
    await publicacao.enviar(montada, n, enr=enr, is_edit=False)
    return enr, montada


def m_url(slug, pid):
    """Página de campanha Shopee como chega DEPOIS de expandida."""
    return (f"https://shopee.com.br/m/{slug}?mmp_pid={pid}"
            "&utm_medium=affiliates&utm_source=" + pid)


# ══════════════════════════════════════════════════════════════════
# 01 — corpus obrigatório
# ══════════════════════════════════════════════════════════════════
CORPUS = [
    ("Cupons Shopee",                         ("shopee|cupb|geral",)),
    ("Novos Cupons Shopee",                   ("shopee|cupb|geral",)),
    ("Novos Cupons Shopee Fidelidade (APP)",  ("shopee|cupb|fidelidade",)),
    ("Novo Cupom Shopee Tech",                ("shopee|cupb|tech",)),
    ("Cupom Shopee Moda Voltando!",           ("shopee|cupb|moda",)),
    ("Cupons Shopee Black Friday",            ("shopee|cupb|black-friday",)),
    ("Cupons Shopee Black Week",              ("shopee|cupb|black-week",)),
    ("Cupons Shopee Clube de Ofertas",        ("shopee|cupb|clube-ofertas",)),
    ("Cupons Shopee Clube Fidelidade",        ("shopee|cupb|clube-fidelidade",)),
    ("Cupons Shopee R$30 OFF em R$299",       ("shopee|cupb|v:30-299",)),
    ("Cupons Shopee R$90 OFF em R$899",       ("shopee|cupb|v:90-899",)),
    ("Cupons Shopee Frete Grátis",            ("shopee|cupb|frete",)),
    ("Cupons Shopee Primeira Compra",         ("shopee|cupb|primeira_compra",)),
]


def test_01_corpus_obrigatorio(r):
    for texto, esperado in CORPUS:
        r.check(chaves(texto) == esperado, f"01.{texto}", str(chaves(texto)))
    k = {t: chaves(t) for t, _ in CORPUS}
    r.check(not set(k["Novos Cupons Shopee Fidelidade (APP)"])
            & set(k["Novo Cupom Shopee Tech"]), "01.fidelidade_nao_colide_com_tech")
    r.check("moda" in k["Cupom Shopee Moda Voltando!"][0]
            and "voltando" not in k["Cupom Shopee Moda Voltando!"][0],
            "01.moda_nao_vira_voltando")
    r.check(k["Cupons Shopee Black Friday"] != k["Cupons Shopee Black Week"],
            "01.black_friday_diferente_de_black_week")
    r.check(k["Cupons Shopee Clube Fidelidade"]
            != k["Novos Cupons Shopee Fidelidade (APP)"],
            "01.clube_fidelidade_diferente_de_fidelidade")
    for t in ("Cupons Shopee R$30 OFF em R$299", "Cupons Shopee R$90 OFF em R$899"):
        r.check(tema_da_campanha(t, "shopee") == "geral",
                f"01.valor_nao_vira_nome.{t}", tema_da_campanha(t, "shopee"))


# ══════════════════════════════════════════════════════════════════
# 02 — os 6 furos do corpus real
# ══════════════════════════════════════════════════════════════════
def test_02_furos_do_corpus(r):
    casos = [
        # 1. horário / aviso / chamada viravam nome
        ("🚨 CUPONS SHOPEE A PARTIR DAS 19H", ("shopee|cupb|geral",)),
        ("🚨 Lembrando! Cupom Shopee 60/249\n\n🎟 R$60 OFF em compras acima de R$249",
         ("shopee|cupb|v:60-249",)),
        ("🔥 ESQUENTA 10.10 SHOPEE - Começa Meia Noite", ("shopee|cupb|esquenta",)),
        # 2. "limite" é valor, não nome
        ("🚨 Cupom Shopee 50% OFF, limite R$10", ("shopee|cupb|p:50-10",)),
        # 3. data de evento é o nome quando nada mais sobra
        ("🚨 CUPONS RENOVADOS! 9.9 SHOPEE", ("shopee|cupb|9.9",)),
        ("Cupons Shopee 10.10", ("shopee|cupb|10.10",)),
        ("🚨 Começou! Esquenta 10.10 Shopee", ("shopee|cupb|esquenta",)),
        # 4. valor abreviado no título; data não é valor
        ("🚨 Cupom Shopee 20/69\n\nℹ️ Resgate agora", ("shopee|cupb|v:20-69",)),
        ("Cupom Shopee 30/09 ativo", ("shopee|cupb|geral",)),
        # 5. corpo colado no título (sem quebra de linha)
        ("🚨 SHOPEE FIGURINHAS   -Colecione figurinhas e ganhe cupons de até R$100 OFF",
         ("shopee|cupb|figurinhas",)),
        # 6. título longo (prosa / produto) não declara nome
        ("Controle para Xbox Sem Fio Carbon Black Shopee R$ 266 cupom",
         ("shopee|cupb|geral",)),
    ]
    for texto, esperado in casos:
        r.check(chaves(texto) == esperado, f"02.{texto[:40]}", str(chaves(texto)))
    r.check(chaves("🚨 CUPONS RENOVADOS! 9.9 SHOPEE")
            != chaves("Cupons Shopee 10.10"), "02.eventos_distintos")
    r.check(chaves("🔥 ESQUENTA 10.10 SHOPEE - Começa Meia Noite")
            == chaves("🚨 Cupons Renovados! Esquenta Shopee"),
            "02.esquenta_e_o_mesmo_nas_duas_formas")
    r.check(chaves("Cupom Shopee R$ 9.90 OFF") == ("shopee|cupb|v:9.9",),
            "02.valor_decimal_nao_e_evento", str(chaves("Cupom Shopee R$ 9.90 OFF")))


# ══════════════════════════════════════════════════════════════════
# 03 — classes do título
# ══════════════════════════════════════════════════════════════════
def test_03_classes_do_titulo(r):
    nunca_nome = [
        "Cupom Shopee",                                  # plataforma + cupom
        "🚨 CUPOM SHOPEE 🚨",                             # emoji
        "Novos Cupons Shopee Liberados Hoje!",           # moldura
        "Cupom Shopee Voltando",                          # reativação canônica
        "Cupom Shopee Voltou de volta",                   # reativação canônica
        "Cupons Shopee Renovados",                        # família de renovar
        "Cupom Shopee 30% OFF",                           # percentual
        "Cupom Shopee R$ 50 OFF",                         # valor
        "Cupom Shopee desconto acima mínimo",             # condição
        "Cupom Shopee Exclusivo em nossos links!",        # autorreferência
        "Cupons Shopee (APP)",                            # qualificador
        "Cupom Shopee geral",                             # "geral" não é nome
    ]
    for t in nunca_nome:
        r.check(nome_declarado(t, "shopee") == "", f"03.sem_nome.{t}",
                nome_declarado(t, "shopee"))
    r.check(nome_declarado("Cupom Fidelidade", "shopee") == "",
            "03.sem_plataforma_nao_declara")
    r.check(nome_declarado("Novos Cupons Shopee Fidelidade", "") == "",
            "03.sem_plat_nao_declara")
    r.check(nome_declarado("Cupons Shopee Black Friday Week Mega", "shopee") == "",
            "03.mais_de_3_palavras_nao_declara")
    r.check(nome_declarado("Cupons Shopee Black Friday Mega", "shopee")
            == "black-friday-mega", "03.tres_palavras_declara")
    r.check(nome_declarado("Cupom Mercado Livre Moda", "mercadolivre") == "moda",
            "03.plataforma_separada_reconhecida")
    r.check(nome_declarado("Cupons Shopee Fidelidade\nVOLTANDO amanhã Tech", "shopee")
            == "fidelidade", "03.so_o_titulo_conta")


# ══════════════════════════════════════════════════════════════════
# 04 — vocabulário legado
# ══════════════════════════════════════════════════════════════════
def test_04_legado(r):
    legados = {
        "Novo Cupom Shopee VIP": "vip",
        "Cupom Shopee Moedas": "moeda",
        "Cupom Shopee Frete Grátis": "frete",
        "Cupom Shopee Aniversário": "aniversario",
        "Cupom Relâmpago Shopee": "relampago",
        "Cupom Shopee Primeira Compra": "primeira_compra",
        "Cupom Shopee Assinantes": "assinante",
    }
    for t, k in legados.items():
        r.check(tema_da_campanha(t, "shopee") == k, f"04.titulo.{t}",
                tema_da_campanha(t, "shopee"))
        r.check(tema_da_campanha(t) == k, f"04.sem_plat.{t}", tema_da_campanha(t))
    # sem nome declarado, o legado no corpo segue valendo
    r.check(tema_da_campanha("🚨 CUPOM DE FRETE GRÁTIS\nsem mínimo", "shopee")
            == "frete", "04.legado_no_corpo")
    # sem `plat`, exatamente o comportamento anterior (nível 1 desligado)
    r.check(tema_da_campanha("Novos Cupons Shopee Tech") == "geral",
            "04.sem_plat_igual_ao_anterior")


# ══════════════════════════════════════════════════════════════════
# 05 — resolvedor: níveis exclusivos; código e cashback intocados
# ══════════════════════════════════════════════════════════════════
def test_05_resolvedor(r):
    tech = chaves("Cupons Shopee Tech\n🎟 R$30 OFF em R$299")
    gen = chaves("Cupons Shopee\n🎟 R$30 OFF em R$299")
    r.check(tech == ("shopee|cupb|tech",), "05.nomeado_so_nome", str(tech))
    r.check(gen == ("shopee|cupb|v:30-299",), "05.generico_so_assinatura", str(gen))
    r.check(not set(tech) & set(gen), "05.nome_e_assinatura_sem_chave_comum")
    cod = resolver(Evidencias(
        plataforma="shopee", entidade_cupom=True, codigos=("MODA15SHO",),
        tema_campanha="geral", assinaturas_beneficio=("v:15-89",)))
    r.check([e.chave for e in cod] == ["shopee|cup|MODA15SHO"], "05.codigo_intocado",
            str(cod))
    cash = resolver(Evidencias(
        plataforma="shopee", entidade_cupom=True, natureza_cash=True,
        percentual="100", tema_campanha="geral", assinaturas_beneficio=("p:100",)))
    r.check([e.chave for e in cash] == ["shopee|cash|100"], "05.cashback_intocado",
            str(cash))
    leg = resolver(Evidencias(plataforma="shopee", entidade_cupom=True,
                              tema_campanha="geral"))
    r.check([e.chave for e in leg] == ["shopee|cupb|geral"],
            "05.sem_assinatura_igual_ao_anterior", str(leg))


# ══════════════════════════════════════════════════════════════════
# 06 — assinatura: canônica, N chaves, sempre FRACA
# ══════════════════════════════════════════════════════════════════
def test_06_assinatura(r):
    a = assinatura_do_beneficio("🎟 R$30 OFF em R$299\n🎟 R$90 OFF em R$899")
    b = assinatura_do_beneficio("🎟 R$ 30 OFF em R$ 299\n🎟 R$ 90 OFF em R$ 899")
    r.check(a == b == ("v:30-299", "v:90-899"), "06.formatacao_nao_muda", f"{a} {b}")
    r.check(assinatura_do_beneficio("🎟 20% OFF limitado a R$20")
            == assinatura_do_beneficio("🎟 20% OFF limitado a R$ 20") == ("p:20-20",),
            "06.percentual_com_limite")
    r.check(assinatura_do_beneficio("🎟 R$500 OFF em compras acima de R$2700")
            == ("v:500-2700",), "06.acima_de")
    r.check(assinatura_do_beneficio("🎟 R$ 1.299 OFF em R$ 5.000") == ("v:1299-5000",),
            "06.milhar")
    r.check(assinatura_do_beneficio("VAI RENOVAR! CUPOM SHOPEE R$500 OFF\n"
                                    "🎟 R$500 OFF em compras acima de R$2700")
            == ("v:500-2700",), "06.valor_solto_so_sem_par")
    r.check(assinatura_do_beneficio("Novos Cupons Shopee") == (), "06.sem_beneficio")
    ks = chaves("Novos Cupons Shopee\n🎟 R$30 OFF em R$299\n🎟 R$90 OFF em R$899")
    r.check(ks == ("shopee|cupb|v:30-299", "shopee|cupb|v:90-899"), "06.n_chaves",
            str(ks))
    r.check(not any(eh_chave_forte(k) for k in ks), "06.assinatura_e_fraca")
    r.check(familia.relacao_composicao(list(ks), list(ks)) is None,
            "06.fraca_fora_da_composicao")


# ══════════════════════════════════════════════════════════════════
# 07–11 — família, caminho real (publicação, banco SQLite real)
# ══════════════════════════════════════════════════════════════════
def _dois(t1, t2, c1=FADA, c2=SAMUEL):
    a, b = msg(c1, t1), msg(c2, t2)
    saida = {}

    async def corpo(c):
        await publicar(a)
        await publicar(b)
        saida["ofertas"] = [identidades(a), identidades(b)]
    cli = cenario(corpo)
    return cli, saida["ofertas"]


def test_07_A_igual_B_mesma_assinatura_mesma_familia(r):
    t = "🔥 Cupons Shopee\n\n🎟 R$30 OFF em R$299\n\n✅ Resgate aqui:\nhttps://s.shopee.com.br/a"
    cli, of = _dois(t, t.replace("/a", "/b"))
    r.check(cli.novos == 1, "07.um_post", f"novos={cli.novos} ofertas={of}")


def test_08_tech_x_generico_mesma_assinatura(r):
    cli, of = _dois(
        "🔥 Cupons Shopee Tech\n\n🎟 R$30 OFF em R$299\nhttps://s.shopee.com.br/t",
        "🔥 Cupons Shopee\n\n🎟 R$30 OFF em R$299\nhttps://s.shopee.com.br/g")
    r.check(of[0] == ["shopee|cupb|tech"] and of[1] == ["shopee|cupb|v:30-299"],
            "08.chaves", str(of))
    r.check(cli.novos == 2, "08.dois_posts", f"novos={cli.novos}")
    cli2, _ = _dois(
        "🔥 Cupons Shopee\n\n🎟 R$30 OFF em R$299\nhttps://s.shopee.com.br/g2",
        "🔥 Cupons Shopee Tech\n\n🎟 R$30 OFF em R$299\nhttps://s.shopee.com.br/t2")
    r.check(cli2.novos == 2, "08.dois_posts_ordem_inversa", f"novos={cli2.novos}")


def test_09_nomes_parecidos_nao_colapsam(r):
    for t1, t2 in (("Cupons Shopee Fidelidade", "Cupons Shopee Clube Fidelidade"),
                   ("Cupons Shopee Black Friday", "Cupons Shopee Black Week")):
        cli, of = _dois(f"🔥 {t1}\n\nResgate os cupons\nhttps://s.shopee.com.br/x",
                        f"🔥 {t2}\n\nResgate os cupons\nhttps://s.shopee.com.br/y")
        r.check(cli.novos == 2, f"09.{t1}×{t2}", f"novos={cli.novos} {of}")


def test_10_nome_x_assinatura_e_nome_x_geral(r):
    cli, of = _dois("🔥 Novos Cupons Shopee Fidelidade (APP)\n\nVários cupons\n"
                    "https://s.shopee.com.br/f",
                    "🔥 Novos Cupons Shopee\n\n🎟 R$30 OFF em R$299\n"
                    "🎟 R$90 OFF em R$899\nhttps://s.shopee.com.br/v")
    r.check(cli.novos == 2, "10.nome_x_assinatura", f"novos={cli.novos} {of}")
    cli, of = _dois("🔥 Novo Cupom Shopee Tech\n\nResgate\nhttps://s.shopee.com.br/t",
                    "Cupons Shopee\n\n-Resgate aqui:\nhttps://s.shopee.com.br/g",
                    PROMOTOM, FADA)
    r.check(cli.novos == 2, "10.nome_x_geral", f"novos={cli.novos} {of}")


def test_11_composicao_overlap_fraco_decidida_por_score(r):
    pobre = "🔥 Cupom Shopee\n\n🎟 R$30 OFF em R$299\nhttps://s.shopee.com.br/p"
    rico = ("🔥 Novos Cupons Shopee\n\n🎟 R$30 OFF em R$299\n🎟 R$90 OFF em R$899\n\n"
            "✅ Resgate aqui:\nhttps://s.shopee.com.br/r")
    a, b = msg(PROMOTOM, pobre), msg(FADA, rico)
    ka, kb = identidades(a), identidades(b)
    r.check(ka == ["shopee|cupb|v:30-299"]
            and kb == ["shopee|cupb|v:30-299", "shopee|cupb|v:90-899"],
            "11.chaves", f"{ka} {kb}")
    r.check(familia.relacao_composicao(kb, ka) is None
            and familia.relacao_composicao(ka, kb) is None,
            "11.relacao_composicao_none")
    for score_rico, esperado in ((10, "EVOLUI"), (2, "SCORE_NAO_EVOLUI")):
        a2, b2 = msg(PROMOTOM, pobre), msg(FADA, rico)

        async def corpo(c, a2=a2, b2=b2, s=score_rico):
            await publicar(a2, score=3)
            await publicar(b2, score=s)
        cli = cenario(corpo)
        r.check(cli.novos == 1, f"11.mesma_familia.score{score_rico}",
                f"novos={cli.novos}")
        r.check(esperado in MOTIVOS
                and not any(m.startswith("COMPOSICAO") for m in MOTIVOS),
                f"11.decide_por_score.{esperado}", str(MOTIVOS))
        dest = cli.ids[0] if cli.ids else None
        if esperado == "EVOLUI" and dest:
            r.check(set(db_ofertas_de_post(dest)) >= set(kb),
                    "11.familia_cresce_por_uniao", str(db_ofertas_de_post(dest)))


# ══════════════════════════════════════════════════════════════════
# 12 — replay real de 01/10 00:00 (textos e links expandidos reais)
# ══════════════════════════════════════════════════════════════════
P_PROM, P_SAM, P_FADA = "an_18382610042", "an_18315670131", "an_18105130010"
REPLAY = [
    ("PROM 110370", PROMOTOM, 110370,
     "Cupons Shopee\n\n-Resgate aqui:\n1 https://s.shopee.com.br/2B5snGi9NX\n"
     "2 https://s.shopee.com.br/qaVCprDoA",
     [m_url("cupom-de-desconto", P_PROM), m_url("espaco-tecnologia", P_PROM)],
     "shopee|cupb|geral"),
    ("FADA 17524", FADA, 17524,
     "🚨 Novos Cupons Shopee Fidelidade (APP)\n\n👉 Vários cupons liberados no "
     "Programa de Fidelidade Shopee\n\n🎁 Clique em \"Confira seu nível\" e resgate "
     "todos: https://s.shopee.com.br/9KbUJJoYEQ\n\n🚛 Novos cupons de frete:\n"
     "https://s.shopee.com.br/2LTDWcTbgH",
     [m_url("fidelidade-shopee", P_FADA),
      "https://shopee.com.br/user/voucher-wallet?sort=1&type=0"],
     "shopee|cupb|fidelidade"),
    ("FADA 17525", FADA, 17525,
     "🚨 Novos Cupons Shopee\n\n🎟 R$30 OFF em R$299\n🎟 R$90 OFF em R$899\n\n"
     "✅ Resgate aqui: \nhttps://s.shopee.com.br/AAGNE4pfS9",
     [m_url("cupom-de-desconto", P_FADA)],
     "shopee|cupb|v:30-299"),
    ("SAM 118699", SAMUEL, 118699,
     "🔥 Novos Cupons Shopee\n\n🎟 R$ 30 OFF em R$ 299\n🎟 R$ 90 OFF em R$ 899\n\n"
     "⭐️ Resgate aqui:\nhttps://s.shopee.com.br/AUuQaBLv6B\n\nanúncio",
     [m_url("cupom-de-desconto", P_SAM)],
     "shopee|cupb|v:30-299"),
    ("SAM 118700", SAMUEL, 118700,
     "🔥 Novos Cupons Shopee Fidelidade (APP)\n\n-Vários cupons liberados no "
     "Programa de Fidelidade Shopee\n\n👉 Clique em \"Confira seu nível\" e resgate "
     "todos: \nhttps://s.shopee.com.br/9zy9zFxM84\n\n🚚 Novos cupons de frete:\n"
     "https://s.shopee.com.br/8fSmOo2IJI\n\nanúncio",
     [m_url("fidelidade-shopee", P_SAM),
      "https://shopee.com.br/user/voucher-wallet?sort=1&type=0"],
     "shopee|cupb|fidelidade"),
    ("FADA 17526", FADA, 17526,
     "🔥 Novo Cupom Shopee Tech\n\n🎟 R$100 OFF em R$999\n"
     "https://s.shopee.com.br/40YmMTfcfQ",
     [m_url("espaco-tecnologia", P_FADA)],
     "shopee|cupb|tech"),
    ("SAM 118705", SAMUEL, 118705,
     "🔥 Novo Cupom Shopee Tech\n\n🎟 R$ 100 OFF em R$ 999\n\n"
     "https://s.shopee.com.br/1Ao572yAc\n\nanúncio",
     [m_url("espaco-tecnologia", P_SAM)],
     "shopee|cupb|tech"),
]


def test_12_replay_01_10(r):
    msgs = [(rot, msg(chat, txt, urls, msg_id=mid), chave)
            for rot, chat, mid, txt, urls, chave in REPLAY]
    for rot, n, chave in msgs:
        r.check(chave in identidades(n), f"12.chave.{rot}", str(identidades(n)))
    # o link /m/<slug> NÃO influencia a identidade (frente própria)
    r.check(identidades(msgs[2][1]) == identidades(msgs[3][1]),
            "12.link_nao_muda_identidade")
    posts = {}

    async def corpo(c):
        for rot, n, _chave in msgs:
            antes = c.novos
            await publicar(n)
            posts[rot] = c.ids[-1] if c.novos > antes else None
    cli = cenario(corpo)
    # 4 posts: Promotom (geral), Fidelidade, 30/90, Tech
    r.check(cli.novos == 4, "12.quatro_posts", f"novos={cli.novos} {posts}")
    for rot in ("PROM 110370", "FADA 17524", "FADA 17525", "FADA 17526"):
        r.check(posts.get(rot) is not None, f"12.publicou.{rot}", str(posts))
    for rot in ("SAM 118699", "SAM 118700", "SAM 118705"):
        r.check(posts.get(rot) is None, f"12.duplicata_nao_publica.{rot}", str(posts))
    # nenhuma campanha caiu na família de outra
    for rot, chave in (("FADA 17524", "shopee|cupb|fidelidade"),
                       ("FADA 17525", "shopee|cupb|v:30-299"),
                       ("FADA 17526", "shopee|cupb|tech")):
        dest = posts.get(rot)
        of = db_ofertas_de_post(dest) if dest else []
        r.check(chave in of and "shopee|cupb|geral" not in of,
                f"12.familia_propria.{rot}", str(of))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CUPOM SEM CÓDIGO · nome › legado › assinatura › geral"))
