"""
FRENTE 8 — CONVERGÊNCIA / FUSÃO DE FAMÍLIAS JÁ NASCIDAS.

Caminho REAL: enriquecer → montar → publicacao.enviar → família →
decidir → aplicador → convergência → remoção em task própria. Banco
SQLite REAL em diretório temporário (harness); só o Telegram é falso.

Os replays usam os TEXTOS REAIS dos canais em 29/09:
  cupom   — Fada 17505 (1010SHO → EDIT +MODA15SHO), Promotom 110334
            (MODA15SHO → EDIT +1010SHO), Samuel 118531/118533;
            produção: 24133 e 24135 vivos; o operador apagou o 24135.
  produto — fumotom 34935/34936 (CV450 e CV700L, mesma loja, produtos
            DIFERENTES), Samuel 118535 com os dois;
            produção: 24137 e 24138 vivos; o operador apagou o 24137.

    python tests/test_fusao_familias.py
"""
import asyncio
import io
import os
import sys
import time
import types
from dataclasses import replace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar  # noqa: E402

preparar()
os.environ.setdefault("ML_TAG", "leoofertas8270")

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
from database import _init_db, db_get_post                     # noqa: E402
from database_conexao import _db                               # noqa: E402
from database_posts import (db_registrar_post, db_fundir_posts,  # noqa: E402
                            db_vizinhos, db_composicoes)
from pipeline import exclusao, origem, familia                 # noqa: E402
from pipeline import publicacao, convergencia                  # noqa: E402
from pipeline.publicacao_estado import destino_vivo_de_origem  # noqa: E402
from pipeline.publicacao_aplicadores import _aplicar_evolucao  # noqa: E402
from pipeline.montagem import montar                            # noqa: E402
from pipeline.enriquecimento import enriquecer, enriquecer_edicao  # noqa: E402
from pipeline.normalizacao import MensagemNormalizada           # noqa: E402
from pipeline.normalizacao_identidade import (                  # noqa: E402
    derivar_produto, derivar_campanha, derivar_ancora_url,
    remover_cupons_da_entidade)
from pipeline.decisao import Decisao, EVOLUIR                   # noqa: E402
from pipeline.resolucao_identidade import (                     # noqa: E402
    Evidencias, resolver, eh_chave_forte)
from pipeline.vida_oferta import VIDA_OFERTA_S                  # noqa: E402

plataformas.inicializar()
_init_db()

# Canais reais (ids de produção) + neutros para os cenários sintéticos.
FADA, PROMOTOM, SAMUEL = "-1002050488946", "-1001825680721", "-1001768101197"
FUMOTOM = "-1003775401737"
CA, CB, CC, CD = "-1009000000001", "-1009000000002", "-1009000000003", "-1009000000004"

# Convergência: remoção sem espera entre tentativas nos testes.
convergencia._ESPERA_BASE_S = 0.0


# ─────────────────────────────────────────────────────────────────
# Telegram falso — conta envios, edições e remoções
# ─────────────────────────────────────────────────────────────────
class _Msg:
    def __init__(self, i, com_midia=False):
        self.id = i
        self.media = object() if com_midia else None
        self.photo = None


_ID_DESTINO = {"n": 50000}


def _proximo_id():
    _ID_DESTINO["n"] += 1
    return _ID_DESTINO["n"]


class Cliente:
    def __init__(self, falha_delete=False, falha_edit_com_file=False):
        self.criados, self.edits, self.deletes = [], [], []
        self.falha_delete = falha_delete
        self.falha_edit_com_file = falha_edit_com_file
        self.tentativas_delete = 0
        self.no_delete = []           # estado observado DURANTE o delete

    async def send_message(self, dest, texto, parse_mode=None, link_preview=None):
        i = _proximo_id()
        self.criados.append(i)
        return _Msg(i)

    async def send_file(self, dest, img, caption=None, parse_mode=None,
                        force_document=False):
        i = _proximo_id()
        self.criados.append(i)
        return _Msg(i, com_midia=True)

    async def edit_message(self, dest, msg_id, texto, parse_mode=None, file=None):
        if file is not None and self.falha_edit_com_file:
            raise RuntimeError("post sem mídia: edit com file recusado")
        self.edits.append((msg_id, texto))
        return _Msg(msg_id, com_midia=file is not None)

    async def delete_messages(self, dest, msg_id):
        self.tentativas_delete += 1
        self.no_delete.append({
            "posts_travados": [k for k, lk in exclusao._POST_LOCKS.items()
                               if lk.locked()],
            "identidades_travadas": [k for k, lk in
                                     exclusao._IDENTITY_LOCKS.items()
                                     if lk.locked()],
            "enviar_em_curso": _EM_ENVIO["n"],
        })
        if self.falha_delete:
            raise RuntimeError("delete recusado")
        self.deletes.append(msg_id)
        return True

    async def download_media(self, media, file=None):
        if file is not None:
            file.write(b"x" * 4096)
        return "arquivo"

    def vivos_fisicos(self):
        return [i for i in self.criados if i not in self.deletes]


_EM_ENVIO = {"n": 0}


def _isolar_loop():
    g._init_globals()
    g._encerrando = False
    for pool in (exclusao._IDENTITY_LOCKS, exclusao._IDENTITY_LOCKS_TS,
                 exclusao._POST_LOCKS, exclusao._POST_LOCKS_TS,
                 origem._LOCKS, origem._LOCKS_TS):
        pool.clear()
    origem._LOCKS_LCK = asyncio.Lock()
    convergencia._EM_CURSO.clear()


async def drenar():
    """Espera as remoções em segundo plano (tasks próprias)."""
    for _ in range(100):
        ts = [t for t in list(convergencia._EM_CURSO.values()) if not t.done()]
        if not ts:
            return
        await asyncio.gather(*ts, return_exceptions=True)


def cenario(corpo, cliente=None):
    async def _run():
        _isolar_loop()
        c = cliente or Cliente()
        modulo_client.client = c
        await corpo(c)
        await drenar()
        return c
    return asyncio.run(_run())


# ─────────────────────────────────────────────────────────────────
# Mensagens
# ─────────────────────────────────────────────────────────────────
_SEQ = {"n": 900000}


def _mid():
    _SEQ["n"] += 1
    return _SEQ["n"]


def shopee(chat, texto, pids=(), cupons=(), msg_id=None, midia=False,
           midia_key=""):
    """Mensagem Shopee normalizada: produtos pelo id EXATO do adaptador."""
    pids = list(pids)
    return MensagemNormalizada(
        msg_id=msg_id or _mid(), chat=str(chat), texto_limpo=texto,
        texto_analise=texto, mapa={"http://o": "http://a"}, preservar=[],
        plat="shopee", sku=(pids[0] if pids else ""),
        tem_midia=midia, media_obj=(object() if midia else None),
        ids_globais=pids, idents=[("shopee", p, "produto") for p in pids],
        cupons=list(cupons), midia_key=midia_key)


def ml(chat, texto, urls_longas, cupons, msg_id=None):
    """Mercado Livre com identidade derivada pelas funções REAIS."""
    ids, idents, sku = derivar_produto(urls_longas)
    cupons = remover_cupons_da_entidade(list(cupons), ids)
    c = derivar_campanha(urls_longas, texto)
    mapa = {f"https://orig/{i}": u for i, u in enumerate(urls_longas)}
    return MensagemNormalizada(
        msg_id=msg_id or _mid(), chat=str(chat), texto_limpo=texto,
        texto_analise=texto, mapa=mapa, preservar=[], plat="mercadolivre",
        sku=sku, tem_midia=False, media_obj=None, ids_globais=ids,
        idents=idents, cupons=cupons, chave_campanha=c.chave_campanha,
        chaves_campanha=c.chaves_campanha,
        tem_host_campanha=c.tem_host_campanha,
        tem_sinal_cashback=c.tem_sinal_cashback,
        destinos_declarados=list(c.destinos_declarados),
        ancora_url=derivar_ancora_url(urls_longas))


def lista(container, campanha):
    return (f"https://lista.mercadolivre.com.br/{container}"
            f"?coupon_campaign_id={campanha}")


async def publicar(n, score=None):
    """Mensagem NOVA pelo caminho real."""
    enr = enriquecer(n)
    if score is not None:
        enr = replace(enr, score=score)
    _EM_ENVIO["n"] += 1
    try:
        await publicacao.enviar(await montar(n), n, enr=enr, is_edit=False)
    finally:
        _EM_ENVIO["n"] -= 1
    return enr


async def editar(n, score=None):
    """EDIÇÃO da mesma mensagem de origem pelo caminho real."""
    enr = enriquecer_edicao(n)
    if score is not None:
        enr = replace(enr, score=score)
    _EM_ENVIO["n"] += 1
    try:
        await publicacao.enviar(await montar(n), n, enr=enr, is_edit=True)
    finally:
        _EM_ENVIO["n"] -= 1
    return enr


def edicao(n, texto, pids=None, cupons=None):
    """A MESMA mensagem de origem, editada."""
    return replace(n, texto_limpo=texto, texto_analise=texto,
                   ids_globais=list(pids if pids is not None else n.ids_globais),
                   idents=[("shopee", p, "produto") for p in
                           (pids if pids is not None else n.ids_globais)]
                   if n.plat == "shopee" else n.idents,
                   sku=((pids or n.ids_globais or [""])[0]
                        if n.plat == "shopee" else n.sku),
                   cupons=list(cupons if cupons is not None else n.cupons))


# ─────────────────────────────────────────────────────────────────
# Inspeção do banco (somente leitura)
# ─────────────────────────────────────────────────────────────────
def dono(chave):
    with _db() as db:
        r = db.execute("SELECT msg_id_dest FROM oferta_index WHERE identity=?",
                       (chave,)).fetchone()
    return r[0] if r else None


def exibida(mid):
    with _db() as db:
        return {r[0] for r in db.execute(
            "SELECT identity FROM post_exibida WHERE msg_id_dest=?",
            (mid,)).fetchall()}


def vivo(mid):
    e = db_get_post(mid)
    return bool(e) and not e.get("fused_into") and e["janela_fim"] > time.time()


def vivos_exibindo(chave):
    with _db() as db:
        return sorted(r[0] for r in db.execute(
            "SELECT px.msg_id_dest FROM post_exibida px"
            " JOIN post_estado pe ON pe.msg_id_dest=px.msg_id_dest"
            " WHERE px.identity=? AND pe.janela_fim>? AND pe.fused_into IS NULL",
            (chave, time.time())).fetchall())


def conflitos(alvo):
    with _db() as db:
        return [dict(zip(("chat", "msg_id", "motivo", "duplicadas", "exclusivas",
                          "pendentes"), r)) for r in db.execute(
            "SELECT chat,msg_id,motivo,duplicadas,exclusivas,pendentes"
            " FROM conflito_estrutural WHERE alvo=? ORDER BY ts", (alvo,)).fetchall()]


def origem_de(chat, msg_id):
    return origem.consultar(str(chat), msg_id)


def p(pid):
    return f"shopee|{pid}"


def cup(cod):
    return f"shopee|cup|{cod}"


class Espiao:
    """Conta chamadas reais de convergencia.consolidar (sem alterar nada)."""

    def __init__(self):
        self.chamadas = []
        self._real = convergencia.consolidar

    async def __call__(self, escrito):
        self.chamadas.append(escrito)
        return await self._real(escrito)

    def __enter__(self):
        convergencia.consolidar = self
        return self

    def __exit__(self, *a):
        convergencia.consolidar = self._real


# ══════════════════════════════════════════════════════════════════
# PARTE 1 — Regra pura e gramática
# ══════════════════════════════════════════════════════════════════
def test_01_gramatica_forte_sobre_chaves_reais_do_resolvedor(r):
    """Forte = produto exato, cupom com código, destino. O resto é fraco.
    As chaves vêm do PRÓPRIO resolvedor (nada escrito à mão)."""
    def chaves(**kw):
        return [e.chave for e in resolver(Evidencias(**kw))]
    prod = chaves(plataforma="shopee", entidade_cupom=False,
                  produtos=(("shopee", "627750190.22199037859", "produto"),),
                  tem_produto=True)
    cupom = chaves(plataforma="shopee", entidade_cupom=True, codigos=("MODA15SHO",))
    dest = chaves(plataforma="mercadolivre", entidade_cupom=True, codigos=("X1",),
                  destinos_declarados=("mercadolivre:lista:_C:1",))
    camp = chaves(plataforma="amazon", entidade_cupom=False,
                  chaves_campanha=("amazon.com.br/promotion/psp/A1",))
    cash = chaves(plataforma="shopee", entidade_cupom=False, natureza_cash=True)
    cashp = chaves(plataforma="shopee", entidade_cupom=True, natureza_cash=True,
                   percentual="100")
    cupb = chaves(plataforma="mercadolivre", entidade_cupom=True,
                  tema_campanha="relampago")
    url = chaves(plataforma="shopee", entidade_cupom=False,
                 link_canonico="https://live.shopee.com.br/live/1")
    txt = chaves(plataforma="shopee", entidade_cupom=False, fingerprint="ab12")
    r.check(prod == ["shopee|627750190.22199037859"] and all(map(eh_chave_forte, prod)),
            "01.produto_forte", str(prod))
    r.check(cupom == ["shopee|cup|MODA15SHO"] and eh_chave_forte(cupom[0]),
            "01.cupom_forte", str(cupom))
    r.check(any(k.startswith("mercadolivre|dest|") for k in dest)
            and all(map(eh_chave_forte, dest)), "01.destino_forte", str(dest))
    fracas = camp + cash + cashp + cupb + url + txt
    r.check(fracas and not any(map(eh_chave_forte, fracas)), "01.fracas",
            str([(k, eh_chave_forte(k)) for k in fracas]))
    r.check(not eh_chave_forte("") and not eh_chave_forte("shopee|")
            and not eh_chave_forte("shopee|cup|"), "01.vazios_fracos")


def _resumo(pl):
    """Plano → (fusões {perdedor: principal}, conflitos {(x, y)})."""
    return ({m: p_ for m, p_, _ in pl.fusoes}, {(x, y) for x, y, _ in pl.conflitos})


def test_02_plano_fusao_regra_pura(r):
    """Cobertura de fortes EXIBIDAS; interseção não basta; fracas não
    provam; estrutura antes do score; score só entre composições iguais."""
    P1, P2, P3 = p("A1"), p("A2"), p("A3")
    pf = familia.plano_fusao
    r.check(_resumo(pf(10, {10: {P1, P2}, 5: {P1}})) == ({5: 10}, set()),
            "02.escrito_cobre_vizinho")
    r.check(_resumo(pf(10, {10: {P1}, 5: {P1, P2}})) == ({10: 5}, set()),
            "02.escrito_contido_perde")
    # mesma composição: score decide; empate de score → fica o escrito
    r.check(_resumo(pf(10, {10: {P1, P2}, 5: {P1, P2}})) == ({5: 10}, set()),
            "02.empate_exato_fica_escrito")
    r.check(_resumo(pf(10, {10: {P1}, 5: {P1}}, {10: 8, 5: 15})) == ({10: 5}, set()),
            "02.mesma_composicao_score_maior_fica")
    r.check(_resumo(pf(10, {10: {P1}, 5: {P1}}, {10: 15, 5: 8})) == ({5: 10}, set()),
            "02.mesma_composicao_score_maior_fica_inverso")
    # estrutura antes do score: container de score menor não perde
    r.check(_resumo(pf(10, {10: {P1}, 5: {P1, P2}}, {10: 99, 5: 1})) == ({10: 5}, set()),
            "02.estrutura_antes_do_score")
    # sobreposição parcial → conflito, nada funde
    r.check(_resumo(pf(10, {10: {P1, P2}, 5: {P2, P3}})) == ({}, {(5, 10)}),
            "02.parcial_vira_conflito")
    fr = {"shopee|cash", "shopee|camp|x", "shopee|url|u"}
    r.check(_resumo(pf(10, {10: set(fr), 5: set(fr)})) == ({}, set()),
            "02.so_fracas_nao_funde")
    r.check(_resumo(pf(10, {10: {P1, P2}, 5: {P1, "shopee|cash"}})) == ({5: 10}, set()),
            "02.fraca_nao_conta")
    D1, D2, X = "ml|dest|L1", "ml|dest|L2", "ml|cup|X"
    r.check(_resumo(pf(10, {10: {D2, X}, 5: {D1, X}})) == ({}, set()),
            "02.destinos_distintos_mesmo_codigo_nao_conflitam")
    r.check(_resumo(pf(10, {10: {D1, X}, 5: {X}})) == ({5: 10}, set()),
            "02.mecanismo_contido_na_lista")
    # cobertura por UNIÃO de sobreviventes; donos por quem exibe
    pl = pf(10, {10: {P1, P2}, 7: {P1}, 4: {P2}, 3: {P1, P2}})
    r.check(_resumo(pl)[1] == set() and len(_resumo(pl)[0]) == 3,
            "02.um_sobrevivente", str(_resumo(pl)))
    # 7={P1,P2} sai coberto pela UNIÃO (P1 em 10, P2 em 4); 10×4 seguem
    # duplicando P3 com exclusivos dos dois lados → conflito, ninguém some
    pl = pf(10, {10: {P1, P3}, 7: {P1, P2}, 4: {P2, P3}, 2: {P2}})
    r.check(_resumo(pl) == ({2: 4, 7: 10}, {(4, 10)}) and pl.envolve(10),
            "02.coberto_pela_uniao_e_conflito_residual", str(_resumo(pl)))
    donos7 = [d for m, _, d in pl.fusoes if m == 7][0]
    r.check(donos7 == {P1: 10, P2: 4}, "02.donos_por_quem_exibe", str(donos7))
    # vários cobridores: maior interseção; empate → prioridade
    r.check(_resumo(pf(10, {10: {P1}, 7: {P1, P2}, 4: {P1, P3}}))[0] == {10: 4},
            "02.cobridor_deterministico")
    r.check(_resumo(pf(10, {10: {"shopee|cash"}, 5: {P1}})) == ({}, set()),
            "02.escrito_sem_forte")
    pl = pf(10, {10: {P1, P2}, 5: {P1}, 6: {P2}})
    r.check(pl.envolve(10) is False and not pl.conflitos, "02.envolve_sem_conflito")
    # relação de composição
    rc = familia.relacao_composicao
    r.check((rc({P1}, {P1}), rc({P1, P2}, {P1}), rc({P1}, {P1, P2}),
             rc({P1, P2}, {P2, P3}), rc({"shopee|cash"}, {P1}), rc({P1}, set()))
            == ("IGUAL", "AMPLIA", "REDUZ", "PARCIAL", None, None),
            "02.relacao_composicao")
    r.check(rc({P1, "shopee|cash"}, {P1}) == "IGUAL", "02.relacao_ignora_fracas")
    # lista: código é ATRIBUTO (7B) — mesma lista com outro código é IGUAL
    # (score decide); lista sobre o /sec/ do mesmo código AMPLIA; o /sec/
    # sobre a lista REDUZ; cupons soltos seguem conjuntos
    Y = "ml|cup|Y"
    r.check((rc({D1, Y}, {D1, X}), rc({D1, X, Y}, {D1, X}), rc({D1, X}, {X}),
             rc({X}, {D1, X}), rc({X, Y}, {X}), rc({Y}, {X}))
            == ("IGUAL", "IGUAL", "AMPLIA", "REDUZ", "AMPLIA", "PARCIAL"),
            "02.relacao_lista_codigo_atributo")
    # mesma lista, códigos diferentes: um post só (o código não é estrutura)
    r.check(_resumo(pf(10, {10: {D1, X}, 5: {D1, Y}}))[1] == set()
            and len(_resumo(pf(10, {10: {D1, X}, 5: {D1, Y}}))[0]) == 1,
            "02.mesma_lista_codigos_diferentes_funde")
    # estrutura ANTES do score: {P1,P2} (score 1) fica; {P1} e {P2} (10) saem
    r.check(_resumo(pf(10, {10: {P1, P2}, 7: {P1}, 4: {P2}}, {10: 1, 7: 10, 4: 10}))
            == ({7: 10, 4: 10}, set()), "02.estrutura_antes_do_score_uniao")
    # duplicidade nos DOIS sentidos: lista {D1,X} × mecanismo {X,Z} sem destino
    Z = "ml|cup|Z"
    r.check(_resumo(pf(10, {5: {D1, X}, 10: {X, Z}})) == ({}, {(5, 10)}),
            "02.duplicidade_simetrica")


# ══════════════════════════════════════════════════════════════════
# PARTE 2 — Posse: gravação normal nunca rouba âncora forte
# ══════════════════════════════════════════════════════════════════
def test_03_gravacao_normal_nao_rouba_forte(r):
    agora = time.time()
    P1, W = p("NR1"), "shopee|camp|nr-w"
    db_registrar_post(40001, [P1, W], 5, "a", "shopee", CA, agora + 900, 0,
                      exibidas=[P1, W])
    db_registrar_post(40002, [P1, W], 5, "b", "shopee", CB, agora + 900, 0,
                      exibidas=[P1, W])
    r.check(dono(P1) == 40001, "03.forte_fica_com_o_dono_vivo", str(dono(P1)))
    r.check(dono(W) == 40002, "03.fraca_segue_politica_antiga", str(dono(W)))
    r.check(exibida(40002) == {P1, W}, "03.exibicao_registrada_mesmo_sem_posse")
    # dono morto → a posse é livre
    db_registrar_post(40001, [P1], 5, "a", "shopee", CA, agora - 1, 0)
    db_registrar_post(40002, [P1], 5, "b", "shopee", CB, agora + 900, 0)
    r.check(dono(P1) == 40002, "03.dono_morto_libera", str(dono(P1)))
    # renascimento (superar) é a única gravação que toma posse viva
    db_registrar_post(40003, [P1], 5, "c", "shopee", CC, agora + 900, 0,
                      exibidas=[P1], superar=40002)
    r.check(dono(P1) == 40003 and exibida(40002) == set(),
            "03.superar_transfere_e_arquiva", f"{dono(P1)} {exibida(40002)}")


def test_04_fusao_atomica_e_tombstone(r):
    agora = time.time()
    P1, P2 = p("AT1"), p("AT2")
    db_registrar_post(40011, [P1, P2], 5, "s", "shopee", CA, agora + 900, 0,
                      chat_origem=CA, msg_id_origem=1, exibidas=[P1, P2])
    db_registrar_post(40012, [P1], 5, "l", "shopee", CB, agora + 900, 0,
                      chat_origem=CB, msg_id_origem=2, exibidas=[P1])
    db_registrar_post(40013, [p("AT3")], 5, "x", "shopee", CC, agora + 900, 0,
                      exibidas=[p("AT3")])
    # sobrevivente morto → nada acontece
    db_registrar_post(40014, [p("AT4")], 5, "m", "shopee", CD, agora - 1, 0)
    r.check(db_fundir_posts(40014, [40013], agora) == [] and vivo(40013),
            "04.sobrevivente_morto_nao_funde")
    feitos = db_fundir_posts(40011, [40012], agora)
    e = db_get_post(40012)
    r.check(feitos == [40012], "04.fundiu", str(feitos))
    r.check(e["fused_into"] == 40011 and e["delete_status"] == "pendente"
            and e["janela_fim"] <= agora, "04.tombstone", str(e))
    r.check(exibida(40012) == set() and origem_de(CB, 2) == 40011,
            "04.exibicao_limpa_e_origem_redirecionada")
    # refundir é no-op (idempotente)
    r.check(db_fundir_posts(40011, [40012], agora) == [], "04.idempotente")
    # nenhuma gravação de conteúdo desfaz a fusão
    db_registrar_post(40012, [P1], 9, "tardio", "shopee", CB, agora + 900, 0)
    r.check(db_get_post(40012)["fused_into"] == 40011, "04.gravacao_nao_desfunde")


# ══════════════════════════════════════════════════════════════════
# PARTE 3 — Cupom (incidente 24133 / 24135)
# ══════════════════════════════════════════════════════════════════
T_FADA_1 = ("🚨 Cupons Shopee em Selecionados\n\n"
            "🎟 R$25 OFF em R$199: 1010SHO (Lojas Oficiais)\n\n"
            "✅ Resgate aqui:\nhttps://s.shopee.com.br/1qU7Zs67MB\n\n"
            "🛒 Carrinho: https://s.shopee.com.br/7plKiu5H62")
T_FADA_2 = ("🚨 Cupons Shopee em Selecionados\n\n"
            "🎟 R$25 OFF em R$199: 1010SHO (Lojas Oficiais)\n"
            "🎟 R$15 OFF em R$89: MODA15SHO (Moda)\n\n"
            "✅ Resgate aqui:\nhttps://s.shopee.com.br/1qU7Zs67MB\n\n"
            "🛒 Carrinho: https://s.shopee.com.br/7plKiu5H62")
T_PROM_1 = ("Cupom Shopee\n\nR$ 15 OFF em R$ 89: MODA15SHO\n\n"
            "-Resgate aqui: \nhttps://s.shopee.com.br/70GSVaUJxC\n\n"
            "-Link carrinho:\nhttps://s.shopee.com.br/7fW9IpEnZG\n\n-Anúncio")
T_PROM_2 = ("Cupom Shopee\n\nR$ 15 OFF em R$ 89: MODA15SHO\n"
            "R$ 25 OFF em R$ 199: 1010SHO\n\n"
            "-Resgate aqui: \nhttps://s.shopee.com.br/70GSVaUJxC\n\n"
            "-Link carrinho:\nhttps://s.shopee.com.br/7fW9IpEnZG\n\n-Anúncio")
T_SAM_LOJ = ("🔥 Cupom Shopee Lojas Oficiais\n\n🎟 R$ 25 OFF em R$ 199: 1010SHO\n\n"
             "⭐️ Resgate aqui:\nhttps://s.shopee.com.br/10stAzGFob\n\n"
             "🛒 Link Carrinho:\nhttps://s.shopee.com.br/8ATePR5hlA\n\nanúncio")
T_SAM_MOD = ("🔥 Cupom Shopee Moda\n\n🎟 R$ 15 OFF em R$ 89: MODA15SHO\n\n"
             "⭐️ Resgate aqui:\nhttps://s.shopee.com.br/4fwbUjODdP\n\n"
             "🛒 Link carrinho:\nhttps://s.shopee.com.br/AAHY2pysiP\n\nanúncio")


def _codigos(tag):
    """Códigos com sufixo por teste (banco compartilhado entre testes)."""
    return f"1010SHO{tag}", f"MODA15SHO{tag}"


def _t(texto, tag):
    return texto.replace("1010SHO", f"1010SHO{tag}").replace(
        "MODA15SHO", f"MODA15SHO{tag}")


def test_05_A_x_B_y_sao_dois_posts(r):
    """Sem prova de equivalência, cada código é um post (B no nascimento)."""
    X, Y = _codigos("T5")
    s = {}

    async def corpo(c):
        s["a"] = await publicar(shopee(FADA, _t(T_FADA_1, "T5"), cupons=[X]))
        s["b"] = await publicar(shopee(PROMOTOM, _t(T_PROM_1, "T5"), cupons=[Y]))
    c = cenario(corpo)
    r.check(s["a"].ofertas == [cup(X)] and s["b"].ofertas == [cup(Y)],
            "05.ancoras_reais", f"{s['a'].ofertas} {s['b'].ofertas}")
    r.check(len(c.criados) == 2 and not c.deletes, "05.dois_posts",
            f"criados={c.criados} deletes={c.deletes}")
    r.check(dono(cup(X)) != dono(cup(Y)), "05.donos_distintos")


def test_06_replay_producao_cupom_A_sobrevive(r):
    """29/09: Fada 1010 → Samuel 1010 (DUP) → Promotom MODA15 → Samuel
    MODA15 (DUP) → EDIT Fada {1010, MODA15} → EDIT Promotom {ambos}.
    Esperado (gesto do operador): 24133 (Fada) vive, 24135 é apagado."""
    X, Y = _codigos("T6")
    s = {}

    async def corpo(c):
        fada = shopee(FADA, _t(T_FADA_1, "T6"), cupons=[X])
        prom = shopee(PROMOTOM, _t(T_PROM_1, "T6"), cupons=[Y])
        with Espiao() as esp:
            await publicar(fada)
            await publicar(shopee(SAMUEL, _t(T_SAM_LOJ, "T6"), cupons=[X],
                                  midia=True, midia_key="k-s1"))
            await publicar(prom)
            await publicar(shopee(SAMUEL, _t(T_SAM_MOD, "T6"), cupons=[Y],
                                  midia=True, midia_key="k-s2"))
            s["a"], s["b"] = c.criados[0], c.criados[1]
            s["fada"], s["prom"] = fada, prom
            s["esp_antes_edit"] = list(esp.chamadas)
            await editar(edicao(fada, _t(T_FADA_2, "T6"), cupons=[X, Y]))
            s["esp_pos_edit_fada"] = list(esp.chamadas)
            await drenar()
            s["deletes_pos_fada"] = list(c.deletes)
            await editar(edicao(prom, _t(T_PROM_2, "T6"), cupons=[Y, X]))
            s["esp_fim"] = list(esp.chamadas)
    c = cenario(corpo)
    a, b = s["a"], s["b"]
    r.check(len(c.criados) == 2, "06.nenhum_post_novo_alem_dos_dois",
            str(c.criados))
    r.check(s["esp_antes_edit"] == [], "06.primeiras_publicacoes_sem_convergencia",
            str(s["esp_antes_edit"]))
    r.check(a in s["esp_pos_edit_fada"], "06.edit_sincronizado_dispara",
            str(s["esp_pos_edit_fada"]))
    r.check(s["deletes_pos_fada"] == [b], "06.B_apagado_pela_prova_da_fada",
            str(s["deletes_pos_fada"]))
    r.check(c.vivos_fisicos() == [a], "06.canal_final_so_A", str(c.vivos_fisicos()))
    e = db_get_post(b)
    r.check(e["fused_into"] == a and e["delete_status"] == "ok",
            "06.B_tombstone", str(e))
    r.check(dono(cup(X)) == a and dono(cup(Y)) == a, "06.posse_unica_em_A",
            f"{dono(cup(X))} {dono(cup(Y))}")
    r.check(vivos_exibindo(cup(Y)) == [a] and vivos_exibindo(cup(X)) == [a],
            "06.um_vivo_por_codigo")
    r.check(exibida(a) == {cup(X), cup(Y)}, "06.A_exibe_o_pacote", str(exibida(a)))
    r.check(origem_de(PROMOTOM, s["prom"].msg_id) == a
            and destino_vivo_de_origem(PROMOTOM, s["prom"].msg_id) == a,
            "06.origem_do_promotom_redirecionada_para_A")
    r.check(destino_vivo_de_origem(FADA, s["fada"].msg_id) == a,
            "06.origem_da_fada_segue_em_A")
    r.check(c.deletes.count(b) == 1, "06.delete_unico", str(c.deletes))


def test_07_A_x_B_y_EDIT_B_B_sobrevive(r):
    """Ordem inversa da prova: quem edita e passa a cobrir sobrevive."""
    X, Y = _codigos("T7")
    s = {}

    async def corpo(c):
        fada = shopee(FADA, _t(T_FADA_1, "T7"), cupons=[X])
        prom = shopee(PROMOTOM, _t(T_PROM_1, "T7"), cupons=[Y])
        await publicar(fada)
        await publicar(prom)
        s["a"], s["b"] = c.criados
        s["fada"] = fada
        await editar(edicao(prom, _t(T_PROM_2, "T7"), cupons=[Y, X]))
    c = cenario(corpo)
    a, b = s["a"], s["b"]
    r.check(c.vivos_fisicos() == [b] and c.deletes == [a], "07.B_vive_A_apagado",
            f"vivos={c.vivos_fisicos()} deletes={c.deletes}")
    r.check(db_get_post(a)["fused_into"] == b, "07.A_fundido_em_B")
    r.check(dono(cup(X)) == b and dono(cup(Y)) == b, "07.posse_em_B")
    r.check(destino_vivo_de_origem(FADA, s["fada"].msg_id) == b,
            "07.origem_da_fada_resolve_B")


def test_08_B_x_A_y_ordem_de_nascimento_inversa(r):
    """B→A (nascimento invertido) + EDIT de quem nasceu primeiro."""
    X, Y = _codigos("T8")
    s = {}

    async def corpo(c):
        prom = shopee(PROMOTOM, _t(T_PROM_1, "T8"), cupons=[Y])
        fada = shopee(FADA, _t(T_FADA_1, "T8"), cupons=[X])
        await publicar(prom)
        await publicar(fada)
        s["b"], s["a"] = c.criados
        await editar(edicao(prom, _t(T_PROM_2, "T8"), cupons=[Y, X]))
    c = cenario(corpo)
    r.check(c.vivos_fisicos() == [s["b"]] and c.deletes == [s["a"]],
            "08.um_post_vivo", f"vivos={c.vivos_fisicos()} del={c.deletes}")


def test_09_evento_tardio_do_perdedor_redireciona(r):
    """Depois da fusão, TODO evento que aponte para o perdedor vai ao
    sobrevivente e nunca vira post novo — pela origem (banco) e pela
    corrida (alvo fixado no perdedor antes do lock)."""
    X, Y = _codigos("T9")
    s = {}

    async def corpo(c):
        fada = shopee(FADA, _t(T_FADA_1, "T9"), cupons=[X])
        prom = shopee(PROMOTOM, _t(T_PROM_1, "T9"), cupons=[Y])
        await publicar(fada)
        await publicar(prom)
        s["a"], s["b"] = c.criados
        await editar(edicao(fada, _t(T_FADA_2, "T9"), cupons=[X, Y]))
        await drenar()
        s["dest_origem_perdedor"] = destino_vivo_de_origem(PROMOTOM, prom.msg_id)
        s["dest_origem_sobrevivente"] = destino_vivo_de_origem(FADA, fada.msg_id)
        # 1) edição tardia da origem do perdedor
        await editar(edicao(prom, _t(T_PROM_2, "T9") + "\nPREÇO NOVO", cupons=[Y, X]))
        # 2) NEW tardio só com o código do perdedor (outra fonte)
        await publicar(shopee(SAMUEL, _t(T_SAM_MOD, "T9"), cupons=[Y]))
        # 3) corrida: alvo resolvido como o PERDEDOR antes do lock
        n = shopee(CD, _t(T_PROM_2, "T9"), cupons=[Y, X])
        enr = enriquecer_edicao(n)
        await publicacao._enviar_resolvido(await montar(n), n, enr.ofertas,
                                           enr.score, True, s["b"])
        s["n_redir"] = n
    c = cenario(corpo)
    a, b = s["a"], s["b"]
    r.check(s["dest_origem_perdedor"] == a, "09.origem_perdedor→sobrevivente",
            str(s["dest_origem_perdedor"]))
    r.check(s["dest_origem_sobrevivente"] == a, "09.origem_sobrevivente_intacta")
    r.check(len(c.criados) == 2, "09.nenhum_post_novo_por_evento_tardio",
            str(c.criados))
    r.check(c.deletes == [b], "09.nenhum_delete_extra", str(c.deletes))
    r.check(all(mid == a for mid, _ in c.edits), "09.edicoes_so_no_sobrevivente",
            str([m for m, _ in c.edits]))
    r.check(origem_de(CD, s["n_redir"].msg_id) == a, "09.corrida_registra_no_sobrevivente",
            str(origem_de(CD, s["n_redir"].msg_id)))
    r.check(db_get_post(b)["fused_into"] == a, "09.perdedor_segue_fundido")


# ══════════════════════════════════════════════════════════════════
# PARTE 4 — Produtos (incidente 24137 / 24138) — INV-PRODUTO
# ══════════════════════════════════════════════════════════════════
T_FUMO_450 = ("Gabinete Gamer Mancer Cv450, Mini-Tower, Lateral de Vidro, Com 9 Fans, "
              "Preto, Mcr-Cv4509f-Bk\n\nR$ 336\n-CUPOM: 1010SHO\n\n"
              "https://s.shopee.com.br/4LJl6XAaO4\n\n-Anúncio")
T_FUMO_700 = ("Gabinete Gamer Aquário Mancer Cv700l, Mid Tower, Lateral De Vidro, "
              "Preto, Mcr-Cv700l-Bk\n\nR$ 224\n-CUPOM: 1010SHO\n\n"
              "https://s.shopee.com.br/9zy7qvl18x\n\n-Anúncio")
T_SAM_GAB = ("🔥 Gabinetes Gamer\n\n🎟 Cupom: 1010SHO\n\n"
             "🔹 Mancer CV700L Aquário, Mid Tower, Vidro - R$ 224\n"
             "https://s.shopee.com.br/6L4pUDky6z\n\n"
             "🔹 Mancer CV450 Mini-Tower, Vidro + 9 Fans - R$ 336\n"
             "https://s.shopee.com.br/1Alwa99EZ\n\nanúncio")


def _pids(tag):
    return f"627750190.22199037859{tag}", f"627750190.23892996530{tag}", f"627750190.99{tag}"


def test_10_replay_producao_produto_B_sobrevive(r):
    """P1 → A, P2 → B, NEW P1+P2 (Samuel, score maior) evolui um deles e
    o outro, coberto, é fundido e apagado. Nunca P1 em dois vivos."""
    P1, P2, _ = _pids("T10")
    s = {}

    async def corpo(c):
        e1 = await publicar(shopee(FUMOTOM, T_FUMO_450, [P1], ["1010SHO"], midia=True,
                                   midia_key="k1"), score=8)
        await publicar(shopee(FUMOTOM, T_FUMO_700, [P2], ["1010SHO"], midia=True,
                              midia_key="k2"), score=8)
        s["e1"] = e1
        s["a"], s["b"] = c.criados
        await publicar(shopee(SAMUEL, T_SAM_GAB, [P2, P1], ["1010SHO"], midia=True,
                              midia_key="k3"), score=10)
    c = cenario(corpo)
    a, b = s["a"], s["b"]
    r.check(s["e1"].ofertas == [p(P1)] and s["e1"].tipo == "produto",
            "10.cupom_e_atributo_nao_ancora", str(s["e1"].ofertas))
    vivos = c.vivos_fisicos()
    r.check(len(c.criados) == 2 and len(vivos) == 1, "10.um_post_no_canal",
            f"criados={c.criados} vivos={vivos}")
    sv = vivos[0]
    perdedor = a if sv == b else b
    r.check(c.deletes == [perdedor], "10.perdedor_apagado", str(c.deletes))
    r.check(exibida(sv) == {p(P1), p(P2)}, "10.sobrevivente_exibe_os_dois")
    r.check(dono(p(P1)) == sv and dono(p(P2)) == sv, "10.posse_unica")
    r.check(vivos_exibindo(p(P1)) == [sv] and vivos_exibindo(p(P2)) == [sv],
            "10.INV_PRODUTO")
    r.check(db_get_post(perdedor)["fused_into"] == sv, "10.tombstone")


def test_11_P1P2_depois_P1_nao_cria_B(r):
    """P1+P2 → A; P1 → (B): a mensagem casa com A; P1 nunca em dois."""
    P1, P2, _ = _pids("T11")

    async def corpo(c):
        await publicar(shopee(SAMUEL, T_SAM_GAB, [P2, P1], ["1010SHO"]), score=10)
        await publicar(shopee(FUMOTOM, T_FUMO_450, [P1], ["1010SHO"]), score=8)
    c = cenario(corpo)
    r.check(len(c.criados) == 1 and not c.deletes, "11.um_post", str(c.criados))
    r.check(vivos_exibindo(p(P1)) == c.criados, "11.P1_um_vivo")


def test_12_P1_depois_NEW_P1P2_evolui_sem_post_novo(r):
    P1, P2, _ = _pids("T12")

    async def corpo(c):
        await publicar(shopee(FUMOTOM, T_FUMO_450, [P1], ["1010SHO"]), score=8)
        await publicar(shopee(SAMUEL, T_SAM_GAB, [P2, P1], ["1010SHO"]), score=10)
    c = cenario(corpo)
    a = c.criados[0]
    r.check(len(c.criados) == 1 and not c.deletes, "12.sem_post_novo", str(c.criados))
    r.check(exibida(a) == {p(P1), p(P2)} and dono(p(P2)) == a, "12.A_evoluiu_rico")


def test_13_EDIT_B_para_P1P2_funde_A(r):
    """P1 → A; B=P3 vivo; EDIT de B → P1+P2 (sincronização): B cobre A."""
    P1, P2, P3 = _pids("T13")
    s = {}

    async def corpo(c):
        await publicar(shopee(CA, "Produto um R$ 10", [P1]))
        nb = shopee(CB, "Produto três R$ 30", [P3])
        await publicar(nb)
        s["a"], s["b"] = c.criados
        with Espiao() as esp:
            await editar(edicao(nb, "Kit um e dois R$ 10 e R$ 20", pids=[P1, P2]))
            s["esp"] = list(esp.chamadas)
    c = cenario(corpo)
    a, b = s["a"], s["b"]
    r.check(s["esp"] == [b], "13.sincronizar_dispara_com_id_de_B", str(s["esp"]))
    r.check(c.vivos_fisicos() == [b] and c.deletes == [a], "13.B_vive_A_apagado",
            f"{c.vivos_fisicos()} {c.deletes}")
    r.check(exibida(b) == {p(P1), p(P2)} and dono(p(P1)) == b, "13.posse_via_fusao")


def test_13b_ordens_exatas_P1_P2_com_edicao_de_cada_lado(r):
    """P1→A, P2→B, EDIT A→P1+P2  ⇒ A vive, B apagado.
       P1→A, P2→B, EDIT B→P1+P2  ⇒ B vive, A apagado."""
    for lado in ("A", "B"):
        P1, P2, _ = _pids(f"T13b{lado}")
        s = {}

        async def corpo(c, lado=lado, P1=P1, P2=P2, s=s):
            na = shopee(CA, "Produto um R$ 10", [P1])
            nb = shopee(CB, "Produto dois R$ 20", [P2])
            await publicar(na)
            await publicar(nb)
            s["a"], s["b"] = c.criados
            alvo = na if lado == "A" else nb
            await editar(edicao(alvo, "Kit um e dois R$ 10 e R$ 20", pids=[P1, P2]))
        c = cenario(corpo)
        sv, pd = (s["a"], s["b"]) if lado == "A" else (s["b"], s["a"])
        r.check(c.vivos_fisicos() == [sv] and c.deletes == [pd],
                f"13b.edit_{lado}_sobrevive", f"{c.vivos_fisicos()} {c.deletes}")
        r.check(dono(p(P1)) == sv and dono(p(P2)) == sv
                and vivos_exibindo(p(P1)) == [sv] and vivos_exibindo(p(P2)) == [sv],
                f"13b.edit_{lado}_INV_PRODUTO")
        r.check(db_get_post(pd)["fused_into"] == sv, f"13b.edit_{lado}_tombstone")


def test_14_EDIT_B_para_P1_contido_em_A(r):
    """A = P1+P2; B = P3; EDIT de B → P1: B fica contido em A → B perde."""
    P1, P2, P3 = _pids("T14")
    s = {}

    async def corpo(c):
        await publicar(shopee(CA, "Kit um e dois R$ 10 e R$ 20", [P1, P2]))
        nb = shopee(CB, "Produto três R$ 30", [P3])
        await publicar(nb)
        s["a"], s["b"], s["nb"] = c.criados[0], c.criados[1], nb
        await editar(edicao(nb, "Produto um R$ 10", pids=[P1]))
    c = cenario(corpo)
    a, b = s["a"], s["b"]
    r.check(c.vivos_fisicos() == [a] and c.deletes == [b], "14.A_vive_B_apagado",
            f"{c.vivos_fisicos()} {c.deletes}")
    r.check(dono(p(P1)) == a and vivos_exibindo(p(P1)) == [a], "14.P1_sem_duplicata")
    r.check(destino_vivo_de_origem(CB, s["nb"].msg_id) == a,
            "14.origem_de_B_resolve_A")


def test_15_P1_EDIT_A_para_P1P2_evolui_proprio(r):
    P1, P2, _ = _pids("T15")
    s = {}

    async def corpo(c):
        n = shopee(CA, "Produto um R$ 10", [P1])
        await publicar(n)
        await editar(edicao(n, "Kit um e dois R$ 10 e R$ 20", pids=[P1, P2]))
        s["a"] = c.criados[0]
    c = cenario(corpo)
    a = s["a"]
    r.check(len(c.criados) == 1 and not c.deletes, "15.sem_post_novo")
    r.check(exibida(a) == {p(P1), p(P2)} and dono(p(P2)) == a, "15.A_passa_a_P1P2")


def test_16_edit_idempotente(r):
    """P1+P2 → A; EDIT mantendo P1+P2 (P1 que já é do próprio post não é
    tratado como novo): nada muda, nenhum post, nenhum roubo."""
    P1, P2, _ = _pids("T16")
    s = {}

    async def corpo(c):
        n = shopee(CA, "Kit um e dois R$ 10 e R$ 20", [P1, P2])
        await publicar(n)
        s["antes"] = (exibida(c.criados[0]), dono(p(P1)), dono(p(P2)))
        await editar(edicao(n, "Kit um e dois R$ 10 e R$ 20", pids=[P1, P2]))
        await editar(edicao(n, "Kit um e dois R$ 10 e R$ 20", pids=[P2, P1]))
        s["a"] = c.criados[0]
    c = cenario(corpo)
    a = s["a"]
    r.check(len(c.criados) == 1 and not c.deletes, "16.nada_novo")
    r.check((exibida(a), dono(p(P1)), dono(p(P2))) == s["antes"], "16.estado_identico")


def test_17_P1_EDIT_A_para_P1_depois_de_P1P2(r):
    """P1+P2 → A; EDIT de A → só P1: A continua o único, sem duplicata."""
    P1, P2, _ = _pids("T17")

    async def corpo(c):
        n = shopee(CA, "Kit um e dois R$ 10 e R$ 20", [P1, P2])
        await publicar(n)
        await editar(edicao(n, "Produto um R$ 10", pids=[P1]))
    c = cenario(corpo)
    a = c.criados[0]
    r.check(len(c.criados) == 1 and not c.deletes, "17.um_post")
    r.check(exibida(a) == {p(P1)} and dono(p(P2)) == a,
            "17.exibe_P1_e_memoria_de_P2_preservada")


def test_18_sobreposicao_parcial_bloqueada_estado_preservado(r):
    """A = P1+P2; B = P3; EDIT do LÍDER de B → P2+P3: aplicar criaria P2
    em dois vivos sem ninguém redundante (P1 exclusivo de A, P3 de B).
    BLOQUEADO: B continua exibindo P3, ninguém é apagado, P2 segue só em
    A, conflito registrado com duplicadas/exclusivas/pendentes."""
    P1, P2, P3 = _pids("T18")
    s = {}

    async def corpo(c):
        await publicar(shopee(CA, "Kit um e dois R$ 10 e R$ 20", [P1, P2]))
        nb = shopee(CB, "Produto três R$ 30", [P3])
        await publicar(nb)
        s["a"], s["b"], s["nb"] = c.criados[0], c.criados[1], nb
        s["edits_antes"] = len(c.edits)
        with Espiao() as esp:
            await editar(edicao(nb, "Kit dois e três R$ 20 e R$ 30", pids=[P2, P3]))
            s["esp"] = list(esp.chamadas)
        s["edits_depois"] = len(c.edits)
    c = cenario(corpo)
    a, b = s["a"], s["b"]
    r.check(not c.deletes and sorted(c.vivos_fisicos()) == sorted([a, b]),
            "18.ninguem_apagado", str(c.deletes))
    r.check(s["edits_depois"] == s["edits_antes"], "18.texto_nao_aplicado")
    r.check(exibida(b) == {p(P3)} and exibida(a) == {p(P1), p(P2)},
            "18.estado_anterior_preservado", f"{exibida(a)} {exibida(b)}")
    r.check(dono(p(P2)) == a and vivos_exibindo(p(P2)) == [a], "18.P2_um_vivo")
    r.check(s["esp"] == [], "18.sem_consolidacao_quando_bloqueado", str(s["esp"]))
    cf = conflitos(b)
    r.check(len(cf) == 1 and cf[0]["duplicadas"] == p(P2)
            and cf[0]["exclusivas"] == p(P1) and cf[0]["pendentes"] == ""
            and cf[0]["chat"] == CB and cf[0]["msg_id"] == s["nb"].msg_id
            and cf[0]["motivo"] == "CONFLITO_ESTRUTURAL", "18.conflito_registrado", str(cf))
    r.check(destino_vivo_de_origem(CB, s["nb"].msg_id) == b, "18.origem_intacta")


def test_19_ancora_aprendida_nao_prova_cobertura(r):
    """A exibe P1+P9 e APRENDE P2 (candidato PARCIAL {P1,P2} → IGNORAR +
    absorver). B passa a exibir só P2. Pela posse A 'teria' P2 — mas A não
    EXIBE P2: nada funde, nada bloqueia."""
    P1, P2, P3 = _pids("T19")
    P9 = P1 + "9"
    s = {}

    async def corpo(c):
        na = shopee(CA, "Kit um e nove R$ 10", [P1, P9])
        await publicar(na, score=20)
        await publicar(shopee(CC, "Kit um e dois R$ 10 e R$ 20", [P1, P2]),
                       score=5)                   # PARCIAL → IGNORAR, absorve P2
        s["a"] = c.criados[0]
        s["dono_p2_apos_absorver"] = dono(p(P2))
        nb = shopee(CB, "Produto três R$ 30", [P3])
        await publicar(nb)
        s["b"] = c.criados[1]
        await editar(edicao(nb, "Produto dois R$ 20", pids=[P2]))
        await editar(edicao(na, "Kit um e nove R$ 10 PREÇO NOVO", pids=[P1, P9]))
    c = cenario(corpo)
    a, b = s["a"], s["b"]
    r.check(s["dono_p2_apos_absorver"] == a, "19.P2_aprendido_por_A")
    r.check(exibida(a) == {p(P1), p(P9)}, "19.A_nao_exibe_P2", str(exibida(a)))
    r.check(exibida(b) == {p(P2)}, "19.B_sincronizou", str(exibida(b)))
    r.check(not c.deletes and sorted(c.vivos_fisicos()) == sorted([a, b]),
            "19.sem_falsa_fusao", str(c.deletes))
    # o PARCIAL {P1,P2} fica auditado (P2 sem representação); a âncora
    # APRENDIDA nunca bloqueia a sincronização de B
    r.check(conflitos(b) == []
            and [x["motivo"] for x in conflitos(a)] == ["COMPOSICAO_PERDERIA_IDENTIDADE"]
            and conflitos(a)[0]["pendentes"] == p(P2), "19.aprendida_nao_bloqueia",
            str(conflitos(a)))


def test_20_edit_ignorada_e_upgrade_de_midia_nao_disparam(r):
    """IGNORAR (edição de não-líder sem score) e UPGRADE_MIDIA não mudam
    o conteúdo exibido: convergência nem é chamada."""
    P1, P2, _ = _pids("T20")
    s = {}

    async def corpo(c):
        await publicar(shopee(CA, "Produto um R$ 10", [P1]), score=10)
        await publicar(shopee(CB, "Produto dois R$ 20", [P2]), score=10)
        s["a"], s["b"] = c.criados
        n3 = shopee(CC, "Produto um R$ 10", [P1], midia=True, midia_key="kx")
        with Espiao() as esp:
            await publicar(n3, score=10)          # DUP + UPGRADE_MIDIA
            s["esp_upgrade"] = list(esp.chamadas)
            await editar(edicao(n3, "Produto um R$ 10 hoje", pids=[P1]),
                         score=3)                 # não-líder, MESMA composição, score menor
            s["esp_ignorar"] = list(esp.chamadas)
        s["midia_chat"] = db_get_post(s["a"])["midia_chat"]
    c = cenario(corpo)
    r.check(s["midia_chat"] == CC, "20.upgrade_aconteceu", str(s["midia_chat"]))
    r.check(s["esp_upgrade"] == [], "20.upgrade_nao_converge", str(s["esp_upgrade"]))
    r.check(s["esp_ignorar"] == [], "20.ignorar_nao_converge", str(s["esp_ignorar"]))
    r.check(not c.deletes and len(c.criados) == 2, "20.nada_apagado")


def test_21_evoluir_dispara_e_substituicao_usa_msg_id_novo(r):
    """EVOLUIR bem-sucedido dispara com o id do post; no fallback de
    SUBSTITUIÇÃO o id é o NOVO — e a fusão usa o novo."""
    P1, P2, _ = _pids("T21")
    s = {}

    async def corpo(c):
        await publicar(shopee(CA, "Produto um R$ 10", [P1]))
        ns = shopee(CB, "Produto dois R$ 20", [P2])
        await publicar(ns)
        s["l"], s["velho"] = c.criados
        c.falha_edit_com_file = True
        n2 = shopee(CC, "Kit um e dois R$ 10 e R$ 20", [P1, P2], midia=True,
                    midia_key="kz")
        montada = replace(await montar(n2), imagem=io.BytesIO(b"x" * 4096))
        d = Decisao(EVOLUIR, "EVOLUI", novo_score=20, exigir_imagem=True,
                    permite_substituir=True, trocar_midia=True, na_janela=True)
        escritas = []
        await _aplicar_evolucao(montada, n2, d, db_get_post(s["velho"]),
                                s["velho"], 0, [p(P2), p(P1)], "t21",
                                exibidas=[p(P1), p(P2)],
                                pos_escrita=escritas.append)
        s["escritas"] = escritas
        s["novo"] = c.criados[-1]
        s["fundidos"] = await convergencia.consolidar(escritas[0])
    c = cenario(corpo)
    r.check(s["escritas"] == [s["novo"]] and s["novo"] != s["velho"],
            "21.pos_escrita_recebe_id_novo", f"{s['escritas']} velho={s['velho']}")
    r.check(db_get_post(s["velho"]) is None and exibida(s["velho"]) == set(),
            "21.velho_removido")
    r.check(exibida(s["novo"]) == {p(P1), p(P2)}, "21.composicao_no_novo")
    r.check(s["fundidos"] == [s["l"]] and db_get_post(s["l"])["fused_into"] == s["novo"],
            "21.fusao_no_id_novo", str(s["fundidos"]))
    r.check(set(c.deletes) == {s["velho"], s["l"]}, "21.deletes", str(c.deletes))


# ══════════════════════════════════════════════════════════════════
# PARTE 5 — Concorrência, restart, renascimento
# ══════════════════════════════════════════════════════════════════
def test_22_concorrencia_mesmo_produto(r):
    """Dois NEW do mesmo P1 ao mesmo tempo: no máximo um dono vivo."""
    P1, _, _ = _pids("T22")

    async def corpo(c):
        await asyncio.gather(publicar(shopee(CA, "Produto um R$ 10", [P1])),
                             publicar(shopee(CB, "Produto um R$ 10 hoje", [P1])))
    c = cenario(corpo)
    r.check(len(c.criados) == 1 and vivos_exibindo(p(P1)) == c.criados,
            "22.um_vivo", str(c.criados))


def test_23_concorrencia_edicoes_cruzadas(r):
    """A=P1, B=P2; EDIT A→P1+P2 e EDIT B→P1+P2 simultâneas: resultado
    determinístico — um vivo, um fundido, origens no sobrevivente."""
    P1, P2, _ = _pids("T23")
    s = {}

    async def corpo(c):
        na = shopee(CA, "Produto um R$ 10", [P1])
        nb = shopee(CB, "Produto dois R$ 20", [P2])
        await publicar(na)
        await publicar(nb)
        s["a"], s["b"], s["na"], s["nb"] = c.criados[0], c.criados[1], na, nb
        await asyncio.gather(
            editar(edicao(na, "Kit um e dois R$ 10 e R$ 20", pids=[P1, P2])),
            editar(edicao(nb, "Kit dois e um R$ 20 e R$ 10", pids=[P2, P1])))
    c = cenario(corpo)
    a, b = s["a"], s["b"]
    vivos = c.vivos_fisicos()
    r.check(len(c.criados) == 2 and len(vivos) == 1 and len(c.deletes) == 1,
            "23.um_vivo_um_apagado", f"vivos={vivos} del={c.deletes}")
    sv = vivos[0] if vivos else None
    r.check(sv == a, "23.deterministico_primeiro_a_provar_sobrevive", str(sv))
    r.check(vivos_exibindo(p(P1)) == [sv] and vivos_exibindo(p(P2)) == [sv],
            "23.INV_PRODUTO")
    r.check(destino_vivo_de_origem(CA, s["na"].msg_id) == sv
            and destino_vivo_de_origem(CB, s["nb"].msg_id) == sv, "23.origens")


def test_24_restart_com_remocao_falhou_e_pendente(r):
    """Falha do delete NÃO desfaz a fusão; no boot, UMA consulta local
    reagenda — sem varredura periódica."""
    X, Y = _codigos("T24")
    s = {}
    convergencia._TENTATIVAS = 2

    async def corpo(c):
        fada = shopee(FADA, _t(T_FADA_1, "T24"), cupons=[X])
        prom = shopee(PROMOTOM, _t(T_PROM_1, "T24"), cupons=[Y])
        await publicar(fada)
        await publicar(prom)
        s["a"], s["b"] = c.criados
        await editar(edicao(fada, _t(T_FADA_2, "T24"), cupons=[X, Y]))
    try:
        c1 = cenario(corpo, Cliente(falha_delete=True))
    finally:
        convergencia._TENTATIVAS = 3
    a, b = s["a"], s["b"]
    e = db_get_post(b)
    r.check(c1.tentativas_delete == 2 and not c1.deletes, "24.tentou_e_falhou",
            str(c1.tentativas_delete))
    r.check(e["fused_into"] == a and e["delete_status"] == "falhou",
            "24.fusao_permanece_apos_falha", str(e))

    # "pendente": o processo encerra antes da remoção
    X2, Y2 = _codigos("T24b")

    async def corpo2(c):
        fada = shopee(FADA, _t(T_FADA_1, "T24b"), cupons=[X2])
        prom = shopee(PROMOTOM, _t(T_PROM_1, "T24b"), cupons=[Y2])
        await publicar(fada)
        await publicar(prom)
        s["b2"] = c.criados[1]
        g._encerrando = True
        await editar(edicao(fada, _t(T_FADA_2, "T24b"), cupons=[X2, Y2]))
    c2 = cenario(corpo2)
    r.check(db_get_post(s["b2"])["delete_status"] == "pendente" and not c2.deletes,
            "24.pendente_ao_encerrar")

    # BOOT: novo processo, novo loop, memória vazia
    async def boot(c):
        s["agendadas"] = convergencia.retomar_remocoes()
    c3 = cenario(boot)
    r.check(b in c3.deletes and s["b2"] in c3.deletes, "24.boot_retoma_as_duas",
            f"deletes={c3.deletes} agendadas={s['agendadas']}")
    r.check(db_get_post(b)["delete_status"] == "ok"
            and db_get_post(s["b2"])["delete_status"] == "ok", "24.status_ok")
    r.check(db_get_post(b)["fused_into"] == a, "24.fusao_persistiu_ao_restart")

    async def boot2(c):
        s["agendadas2"] = convergencia.retomar_remocoes()
    c4 = cenario(boot2)
    r.check(not c4.deletes, "24.segundo_boot_nada_a_fazer", str(c4.deletes))
    # sem polling: nenhuma task viva depois do boot
    r.check(not convergencia._EM_CURSO, "24.sem_task_residual")


def test_25_renascimento_depois_da_fusao(r):
    """Fundido nunca volta. RENASCER no sobrevivente cria o ciclo novo
    com TODAS as âncoras (inclusive as do perdedor); o antigo vira
    histórico (não é apagado); o perdedor continua fundido."""
    X, Y = _codigos("T25")
    s = {}

    async def corpo(c):
        fada = shopee(FADA, _t(T_FADA_1, "T25"), cupons=[X])
        prom = shopee(PROMOTOM, _t(T_PROM_1, "T25"), cupons=[Y])
        await publicar(fada)
        await publicar(prom)
        s["a"], s["b"] = c.criados
        await editar(edicao(fada, _t(T_FADA_2, "T25"), cupons=[X, Y]))
        await drenar()
        await publicar(shopee(SAMUEL, "🔥 VOLTOU! " + _t(T_SAM_MOD, "T25"),
                              cupons=[Y]))
        s["n"] = c.criados[-1]
    c = cenario(corpo)
    a, b, n = s["a"], s["b"], s["n"]
    r.check(n not in (a, b) and len(c.criados) == 3, "25.renasceu_post_novo",
            str(c.criados))
    r.check(dono(cup(X)) == n and dono(cup(Y)) == n, "25.ciclo_novo_com_todas")
    r.check(c.deletes == [b], "25.historico_nao_apagado", str(c.deletes))
    r.check(db_get_post(b)["fused_into"] == a and not vivo(b), "25.perdedor_nao_volta")
    r.check(vivos_exibindo(cup(Y)) == [n], "25.um_vivo_exibindo")

    # fim do ciclo do novo: nova mensagem nasce num post NOVO, nunca no fundido
    with _db() as db:
        db.execute("UPDATE post_estado SET janela_fim=? WHERE msg_id_dest IN (?,?)",
                   (time.time() - 1, a, n))

    async def corpo2(c):
        await publicar(shopee(CD, _t(T_PROM_1, "T25"), cupons=[Y]))
        s["m"] = c.criados[-1]
    c2 = cenario(corpo2)
    r.check(s["m"] not in (a, b, n) and db_get_post(b)["fused_into"] == a,
            "25.ciclo_seguinte_nao_ressuscita", str(s["m"]))


# ══════════════════════════════════════════════════════════════════
# PARTE 6 — O que NÃO pode fundir
# ══════════════════════════════════════════════════════════════════
def test_26_listas_distintas_mesmo_cupom(r):
    """Duas campanhas (destinos) com o MESMO código: dois posts, sempre —
    inclusive depois de uma edição que acrescenta outro código."""
    cod = "FULL3009F8"
    txt = f"🔥 Cupom Mercado Livre em Selecionados\n🎟 R$ 50 OFF em R$ 300: {cod}\nhttps://meli.la/x"

    async def corpo(c):
        a = ml(CA, txt, [lista("_Container_promo-f8", "83657213")], [cod])
        await publicar(a)
        await publicar(ml(CB, txt, [lista("_Container_promo-f8", "84194174")], [cod]))
        await editar(replace(a, texto_limpo=txt + "\n🎟 extra: OUTROF8",
                             texto_analise=txt + "\n🎟 extra: OUTROF8",
                             cupons=[cod, "OUTROF8"]))
    c = cenario(corpo)
    r.check(len(c.criados) == 2 and not c.deletes, "26.dois_posts_sempre",
            f"{c.criados} {c.deletes}")


def test_27_mecanismo_e_lista_viram_um(r):
    """/sec/ {X} e lista L1 {Y} nascem separados; a lista L1 com {X, Y}
    evolui a lista, que passa a cobrir o /sec/ → um post (a lista)."""
    x, y = "SECF8X", "LSTF8Y"
    sec = "https://mercadolivre.com/sec/2U6U32Q"
    ls = lista("_Container_f8-lista", "91112222")
    s = {}

    async def corpo(c):
        await publicar(ml(CA, f"Cupom Mercado Livre\n\n10% OFF: {x}\n\n-Resgate: {sec}",
                          [sec], [x]))
        await publicar(ml(CB, f"🔥 Cupom Mercado Livre em Selecionados\n🎟 20% OFF: {y}\n"
                              f"Lista: https://meli.la/1", [ls], [y]))
        s["sec"], s["lista"] = c.criados
        await publicar(ml(CC, f"🔥 Cupons Mercado Livre em Selecionados\n🎟 10% OFF: {x}\n"
                              f"🎟 20% OFF: {y}\nLista: https://meli.la/2", [ls], [x, y]),
                       score=30)
    c = cenario(corpo)
    r.check(c.vivos_fisicos() == [s["lista"]] and c.deletes == [s["sec"]],
            "27.lista_sobrevive_sec_apagado", f"{c.vivos_fisicos()} {c.deletes}")


def test_28_produto_com_cupom_atributo_nao_funde_com_cupom(r):
    """Produto com cupom 1010SHO (atributo) × cupom-entidade 1010SHO:
    ofertas diferentes; nenhuma edição posterior funde as duas."""
    X, _ = _codigos("T28")
    P1, _, _ = _pids("T28")

    async def corpo(c):
        n = shopee(FUMOTOM, T_FUMO_450.replace("1010SHO", X), [P1], [X])
        await publicar(n)
        f = shopee(FADA, _t(T_FADA_1, "T28"), cupons=[X])
        await publicar(f)
        await editar(edicao(f, _t(T_FADA_2, "T28"), cupons=[X, "MODA15SHOT28"]))
        await editar(edicao(n, T_FUMO_450.replace("1010SHO", X) + "\nPREÇO NOVO"))
    c = cenario(corpo)
    r.check(len(c.criados) == 2 and not c.deletes, "28.duas_ofertas_distintas",
            f"{c.criados} {c.deletes}")


def test_29_fracas_nunca_provam(r):
    """Dois posts vivos que só compartilham âncoras FRACAS não fundem,
    ainda que uma composição fraca contenha a outra."""
    agora = time.time()
    W1, W2 = "shopee|camp|f8-w1", "shopee|cash"
    db_registrar_post(40021, [W1, W2], 5, "x", "shopee", CA, agora + 900, 0,
                      exibidas=[W1, W2])
    db_registrar_post(40022, [W1], 5, "y", "shopee", CB, agora + 900, 0,
                      exibidas=[W1])

    async def corpo(c):
        s["f"] = await convergencia.consolidar(40021)
    s = {}
    c = cenario(corpo)
    r.check(s["f"] == [] and vivo(40022) and not c.deletes, "29.nada_funde")
    r.check(familia.fortes([W1, W2]) == frozenset()
            and db_vizinhos(sorted(familia.fortes([W1, W2])), {40021}, time.time()) == {},
            "29.sem_forte_nem_consulta_vizinhos")


# ══════════════════════════════════════════════════════════════════
# PARTE 7 — Garantias de desempenho e isolamento
# ══════════════════════════════════════════════════════════════════
def test_30_primeira_publicacao_nao_converge(r):
    """Oferta nova sem família viva: convergência NÃO é chamada e nenhuma
    consulta de composição de vizinhos acontece."""
    P1, _, _ = _pids("T30")
    X, _ = _codigos("T30")
    sqls = []
    s = {}

    async def corpo(c):
        with _db() as db:
            db.set_trace_callback(sqls.append)
        try:
            with Espiao() as esp:
                await publicar(shopee(CA, "Produto um R$ 10", [P1]))
                await publicar(shopee(FADA, _t(T_FADA_1, "T30"), cupons=[X]))
                s["esp"] = list(esp.chamadas)
        finally:
            with _db() as db:
                db.set_trace_callback(None)
    c = cenario(corpo)
    r.check(len(c.criados) == 2 and s["esp"] == [], "30.sem_convergencia",
            str(s["esp"]))
    vizinhos = [q for q in sqls if "SELECT px.msg_id_dest" in q or "BEGIN" in q]
    r.check(vizinhos == [], "30.sem_consulta_de_vizinhos_nem_transacao",
            str(vizinhos[:2]))


def test_31_delete_fora_de_lock_e_fora_do_pipeline(r):
    """A remoção física acontece em task PRÓPRIA: nenhum lock de post ou
    identidade travado e nenhuma publicação em curso quando o Telegram
    é chamado — e o evento que provou a fusão retorna antes do delete."""
    X, Y = _codigos("T31")
    s = {}

    async def corpo(c):
        fada = shopee(FADA, _t(T_FADA_1, "T31"), cupons=[X])
        await publicar(fada)
        await publicar(shopee(PROMOTOM, _t(T_PROM_1, "T31"), cupons=[Y]))
        await editar(edicao(fada, _t(T_FADA_2, "T31"), cupons=[X, Y]))
        s["deletes_ao_retornar"] = list(c.deletes)
        await drenar()
    c = cenario(corpo)
    r.check(s["deletes_ao_retornar"] == [], "31.edicao_retorna_antes_do_delete",
            str(s["deletes_ao_retornar"]))
    r.check(len(c.no_delete) == 1, "31.um_delete", str(c.no_delete))
    obs = c.no_delete[0] if c.no_delete else {}
    r.check(obs.get("posts_travados") == [] and obs.get("identidades_travadas") == []
            and obs.get("enviar_em_curso") == 0, "31.sem_lock_sem_pipeline", str(obs))


# ══════════════════════════════════════════════════════════════════
# PARTE 8 — Casos de borda que o mutation testing exigiu provar
# ══════════════════════════════════════════════════════════════════
def test_32_aprendida_que_passa_a_ser_exibida_funde(r):
    """A exibe P1 e APRENDE P2. B passa a exibir só P2 (sem ser dono).
    Quando a FONTE de A edita e A passa a EXIBIR P1+P2, B fica coberto:
    o vizinho é achado pela EXIBIÇÃO, não pela posse."""
    P1, P2, P3 = _pids("T32")
    s = {}

    async def corpo(c):
        na = shopee(CA, "Produto um R$ 10", [P1])
        await publicar(na, score=20)
        await publicar(shopee(CC, "Kit um e dois R$ 10 e R$ 20", [P1, P2]), score=5)
        nb = shopee(CB, "Produto três R$ 30", [P3])
        await publicar(nb)
        s["a"], s["b"] = c.criados
        await editar(edicao(nb, "Produto dois R$ 20", pids=[P2]))
        s["del_antes"] = list(c.deletes)
        await editar(edicao(na, "Kit um e dois R$ 10 e R$ 20", pids=[P1, P2]))
    c = cenario(corpo)
    a, b = s["a"], s["b"]
    r.check(s["del_antes"] == [], "32.antes_da_prova_nada")
    r.check(c.vivos_fisicos() == [a] and c.deletes == [b], "32.B_coberto_pela_edicao_de_A",
            f"{c.vivos_fisicos()} {c.deletes}")
    r.check(vivos_exibindo(p(P2)) == [a], "32.P2_um_vivo")


def test_33_evolucao_exibe_a_mensagem_nao_a_familia(r):
    """Na EVOLUÇÃO o post passa a exibir a mensagem vencedora — não a
    união da família. Com A aprendendo P2 e B exibindo P2, uma evolução
    de A por MESMA composição (score maior) NÃO pode apagar B."""
    P1, P2, P3 = _pids("T33")
    P9 = P1 + "9"
    s = {}

    async def corpo(c):
        await publicar(shopee(CA, "Kit um e nove R$ 10", [P1, P9]), score=10)
        await publicar(shopee(CC, "Kit um e dois R$ 10 e R$ 20", [P1, P2]), score=5)
        nb = shopee(CB, "Produto três R$ 30", [P3])
        await publicar(nb)
        s["a"], s["b"] = c.criados
        await editar(edicao(nb, "Produto dois R$ 20", pids=[P2]))
        await publicar(shopee(CD, "Kit um e nove R$ 9 MENOR PREÇO", [P1, P9]), score=30)
    c = cenario(corpo)
    a, b = s["a"], s["b"]
    r.check(exibida(a) == {p(P1), p(P9)}, "33.A_exibe_so_a_mensagem", str(exibida(a)))
    r.check(any("MENOR PREÇO" in t for m, t in c.edits if m == a), "33.A_evoluiu")
    r.check(not c.deletes and vivo(b), "33.B_intacto", str(c.deletes))


def test_34_revalidacao_sob_lock(r):
    """O plano otimista nunca é aplicado sem prova atual: se L muda entre
    o plano e o lock (deixa de estar contido), nada funde."""
    agora = time.time()
    P1, P2, P3 = p("RV1"), p("RV2"), p("RV3")
    db_registrar_post(40031, [P1, P2], 5, "s", "shopee", CA, agora + 900, 0,
                      exibidas=[P1, P2])
    db_registrar_post(40032, [P1], 5, "l", "shopee", CB, agora + 900, 0,
                      exibidas=[P1])
    s = {}

    async def corpo(c):
        lk = await exclusao.lock_post(40032)
        await lk.acquire()
        t = asyncio.get_running_loop().create_task(convergencia.consolidar(40031))
        for _ in range(20):
            await asyncio.sleep(0)
        s["bloqueada"] = not t.done()
        db_registrar_post(40032, [P3], 5, "l2", "shopee", CB, agora + 900, 0,
                          exibidas=[P1, P3])
        lk.release()
        s["fundidos"] = await t
    c = cenario(corpo)
    r.check(s["bloqueada"], "34.convergencia_esperou_o_lock")
    r.check(s["fundidos"] == [] and vivo(40032) and not c.deletes,
            "34.nada_fundido_com_prova_vencida", str(s["fundidos"]))


def test_35_origem_legada_segue_a_cadeia(r):
    """Vínculo gravado apontando para um post fundido (inclusive cadeia de
    dois saltos) resolve no sobrevivente."""
    agora = time.time()
    P1, P2 = p("CH1"), p("CH2")
    db_registrar_post(40041, [P1, P2], 5, "s", "shopee", CA, agora + 900, 0,
                      exibidas=[P1, P2])
    db_registrar_post(40042, [P1], 5, "l", "shopee", CB, agora + 900, 0,
                      exibidas=[P1])
    db_fundir_posts(40041, [40042], agora)
    db_registrar_post(40043, [], 5, "k", "shopee", CC, agora + 900, 0)
    with _db() as db:
        db.execute("UPDATE post_estado SET fused_into=? WHERE msg_id_dest=?",
                   (40042, 40043))
    origem.registrar(CD, 7771, 40042)
    origem.registrar(CD, 7772, 40043)
    r.check(destino_vivo_de_origem(CD, 7771) == 40041, "35.um_salto")
    r.check(destino_vivo_de_origem(CD, 7772) == 40041, "35.dois_saltos")


def test_36_familia_enxerga_exibicao_apos_morte_do_dono(r):
    """Estado LEGADO (anterior à 8b, ou escrito pela sincronização antiga):
    B exibe P2 sem ser dono (dono: A). A morre. Um NEW com P2 casa com
    B pela exibição — nunca abre um terceiro post com P2 duplicado."""
    P1, P2, P3 = _pids("T36")
    agora = time.time()
    db_registrar_post(40061, [p(P1), p(P2)], 5, "a", "shopee", CA, agora + 900, 0,
                      exibidas=[p(P1), p(P2)])
    db_registrar_post(40062, [p(P2), p(P3)], 5, "b", "shopee", CB, agora + 900, 0,
                      exibidas=[p(P2), p(P3)])
    r.check(dono(p(P2)) == 40061, "36.pre_dono_A")
    with _db() as db:
        db.execute("UPDATE post_estado SET janela_fim=? WHERE msg_id_dest=?",
                   (agora - 1, 40061))

    async def corpo(c):
        await publicar(shopee(CD, "Produto dois R$ 20", [P2]))
    c = cenario(corpo)
    r.check(c.criados == [], "36.sem_terceiro_post", str(c.criados))
    r.check(vivos_exibindo(p(P2)) == [40062], "36.P2_um_vivo")


def test_37_vizinho_morto_nao_trava_nada(r):
    """Post de ciclo encerrado nunca entra na convergência: nem plano, nem
    lock de post."""
    agora = time.time()
    P1, P2 = p("DM1"), p("DM2")
    db_registrar_post(40051, [P1], 5, "morto", "shopee", CB, agora + 900, 0,
                      exibidas=[P1])
    db_registrar_post(40052, [P2], 5, "s", "shopee", CA, agora + 900, 0,
                      exibidas=[P1, P2])
    with _db() as db:
        db.execute("UPDATE post_estado SET janela_fim=? WHERE msg_id_dest=?",
                   (agora - 1, 40051))
    travados = []
    real = exclusao.lock_post
    s = {}

    async def espia(mid):
        travados.append(mid)
        return await real(mid)

    async def corpo(c):
        exclusao.lock_post = espia
        try:
            s["f"] = await convergencia.consolidar(40052)
        finally:
            exclusao.lock_post = real
    cenario(corpo)
    r.check(s["f"] == [] and travados == [], "37.sem_plano_sem_lock", str(travados))
    r.check(not db_get_post(40051).get("fused_into"), "37.morto_intocado")
    # a releitura sob lock só devolve VIVOS e não fundidos
    db_registrar_post(40053, [p("DM3")], 5, "f", "shopee", CC, agora + 900, 0,
                      exibidas=[p("DM3")])
    db_registrar_post(40054, [p("DM3"), p("DM4")], 5, "g", "shopee", CD, agora + 900, 0,
                      exibidas=[p("DM3"), p("DM4")])
    db_fundir_posts(40054, [40053], agora)
    r.check(set(db_composicoes([40051, 40052, 40053, 40054], time.time()))
            == {40052, 40054}, "37.composicoes_so_vivos")

# ══════════════════════════════════════════════════════════════════
# PARTE 9 — FRENTE 8b: regra definitiva (composição × score × fusão)
# ══════════════════════════════════════════════════════════════════
def _ps(tag, n):
    return [f"8b{tag}0{i}" for i in range(1, n + 1)]


def _txt(pids):
    return "Oferta " + " + ".join(pids)


def score_de(mid):
    return (db_get_post(mid) or {}).get("score")


def test_40_A_P1P2_B_P2_EDIT_B_P1P2(r):
    """A=P1+P2 → B=P2 (outra fonte: REDUZ → não vira post) → EDIT B=P1+P2
    (segue a origem até A: mesma composição, score decide). Um post."""
    P1, P2 = _ps("40", 2)
    s = {}

    async def corpo(c):
        await publicar(shopee(CA, _txt([P1, P2]), [P1, P2]), score=10)
        nb = shopee(CB, _txt([P2]), [P2])
        await publicar(nb, score=12)
        s["a"] = c.criados[0]
        await editar(edicao(nb, _txt([P1, P2]), pids=[P1, P2]), score=12)
    c = cenario(corpo)
    a = s["a"]
    r.check(c.criados == [a] and not c.deletes, "40.um_post", str(c.criados))
    r.check(exibida(a) == {p(P1), p(P2)}, "40.composicao_mantida")
    r.check(vivos_exibindo(p(P1)) == [a] and vivos_exibindo(p(P2)) == [a], "40.INV")
    r.check(score_de(a) == 12, "40.mesma_composicao_score_maior_evolui", str(score_de(a)))


def test_41_mesma_composicao_score_decide_o_sobrevivente(r):
    """A=P1 (score 8) e B=P3; EDIT do líder de B → P1 (score 15): mesma
    composição que A → B sobrevive (texto mais rico), A é fundido.
    Inverso (A 15, B 8): A sobrevive, B é fundido."""
    for sa, sb in ((8, 15), (15, 8)):
        P1, P3 = _ps(f"41{sa}", 2)
        s = {}

        async def corpo(c, P1=P1, P3=P3, sa=sa, sb=sb, s=s):
            na = shopee(CA, _txt([P1]), [P1])
            await publicar(na, score=sa)
            nb = shopee(CB, _txt([P3]), [P3])
            await publicar(nb, score=sb)
            s["a"], s["b"], s["na"], s["nb"] = c.criados[0], c.criados[1], na, nb
            await editar(edicao(nb, _txt([P1]) + " agora", pids=[P1]), score=sb)
        c = cenario(corpo)
        a, b = s["a"], s["b"]
        sv, pd = (b, a) if sb > sa else (a, b)
        r.check(c.vivos_fisicos() == [sv] and c.deletes == [pd],
                f"41.{sa}x{sb}.sobrevive_score_maior", f"{c.vivos_fisicos()} {c.deletes}")
        r.check(vivos_exibindo(p(P1)) == [sv] and dono(p(P1)) == sv,
                f"41.{sa}x{sb}.INV_e_posse")
        r.check(score_de(sv) == max(sa, sb), f"41.{sa}x{sb}.score_do_texto_que_ficou")
        r.check(destino_vivo_de_origem(CA, s["na"].msg_id) == sv
                and destino_vivo_de_origem(CB, s["nb"].msg_id) == sv,
                f"41.{sa}x{sb}.origens_no_sobrevivente")


def test_42_lider_retira_produto_permitido(r):
    """A=P1+P2; EDIT do LÍDER → P1: decisão legítima da fonte. Sem
    bloqueio, sem conflito; P2 deixa de ser exibido."""
    P1, P2 = _ps("42", 2)
    s = {}

    async def corpo(c):
        n = shopee(CA, _txt([P1, P2]), [P1, P2])
        await publicar(n)
        s["a"] = c.criados[0]
        await editar(edicao(n, _txt([P1]), pids=[P1]))
    c = cenario(corpo)
    a = s["a"]
    r.check(exibida(a) == {p(P1)} and vivos_exibindo(p(P2)) == [], "42.P2_saiu")
    r.check(any(m == a for m, _ in c.edits), "42.telegram_editado")
    r.check(conflitos(a) == [], "42.sem_conflito")
    # depois, OUTRA fonte com só P2: a família casa A pela memória, a
    # composição é disjunta do que A exibe → texto não entra, e o descarte
    # fica AUDITADO (P2 sem representação no canal).
    s2 = {}

    async def corpo2(c):
        n2 = shopee(CB, _txt([P2]) + " B", [P2])
        await publicar(n2, score=30)
        s2["n2"] = n2
    c2 = cenario(corpo2)
    cf = conflitos(a)
    r.check(not c2.criados and exibida(a) == {p(P1)}, "42.disjunto_ignorado")
    r.check(len(cf) == 1 and cf[0]["pendentes"] == p(P2)
            and cf[0]["motivo"] == "COMPOSICAO_PERDERIA_IDENTIDADE"
            and cf[0]["msg_id"] == s2["n2"].msg_id, "42.descarte_auditado", str(cf))


def test_43_outra_fonte_REDUZ_bloqueada(r):
    """A=P1+P2; outra fonte (NEW e EDIT de não-líder) com só P1, score
    maior: REDUZ → texto não entra, A mantém P1+P2. REDUZ não deixa
    pendência → nada registrado (sem ruído)."""
    P1, P2 = _ps("43", 2)
    s = {}

    async def corpo(c):
        await publicar(shopee(CA, _txt([P1, P2]), [P1, P2]), score=5)
        s["a"] = c.criados[0]
        nc = shopee(CC, _txt([P1, P2]) + " C", [P1, P2])
        await publicar(nc, score=4)                     # não-líder, IGUAL, perde
        await publicar(shopee(CB, _txt([P1]), [P1]), score=30)
        await editar(edicao(nc, _txt([P1]) + " C", pids=[P1]), score=30)
        s["edits"] = list(c.edits)
    c = cenario(corpo)
    a = s["a"]
    r.check(c.criados == [a] and s["edits"] == [], "43.nada_aplicado", str(s["edits"]))
    r.check(exibida(a) == {p(P1), p(P2)} and score_de(a) == 5, "43.A_intacto")
    r.check(conflitos(a) == [], "43.reduz_sem_ruido")


def test_44_container_absorve_tres_posts(r):
    """A=P1, B=P2, C=P3; D=P1..P4 (score MENOR): AMPLIA → um post exibe
    P1..P4 com o texto de D; os outros dois são fundidos; cada produto
    num único vivo; origens no sobrevivente."""
    P = _ps("44", 4)
    s = {}

    async def corpo(c):
        ns = [shopee(ch, _txt([pp]), [pp]) for ch, pp in zip((CA, CB, CC), P[:3])]
        for n in ns:
            await publicar(n, score=12)
        s["abc"], s["ns"] = list(c.criados), ns
        nd = shopee(CD, _txt(P) + " LISTA", P)
        await publicar(nd, score=8)
        s["nd"] = nd
    c = cenario(corpo)
    vivos = c.vivos_fisicos()
    r.check(len(c.criados) == 3 and len(vivos) == 1 and len(c.deletes) == 2,
            "44.um_vivo", f"criados={c.criados} vivos={vivos} del={c.deletes}")
    sv = vivos[0] if vivos else None
    r.check(exibida(sv) == {p(x) for x in P}, "44.exibe_P1_P4", str(exibida(sv)))
    r.check(any("LISTA" in t for m, t in c.edits if m == sv), "44.texto_de_D")
    r.check(score_de(sv) == 8, "44.score_do_texto_publicado", str(score_de(sv)))
    r.check(all(vivos_exibindo(p(x)) == [sv] for x in P), "44.INV_PRODUTO")
    r.check(all(destino_vivo_de_origem(n.chat, n.msg_id) == sv for n in s["ns"] + [s["nd"]]),
            "44.origens_no_sobrevivente")
    for x in s["abc"]:
        if x != sv:
            r.check(db_get_post(x)["fused_into"] == sv, f"44.fundido_{x}")


def test_45_container_bloqueado_com_exclusivos(r):
    """A=P1+P5, B=P2, C=P3+P6; D=P1..P4: aplicar D duplicaria P1 e P3
    (P5 e P6 são exclusivos) → BLOQUEADO qualquer que seja o post que a
    família escolha; conflito registrado: duplicadas P1,P3 / exclusivas
    P5,P6 / pendentes P4. Nada apagado, estado preservado."""
    P1, P2, P3, P4, P5, P6 = _ps("45", 6)
    s = {}

    async def corpo(c):
        await publicar(shopee(CA, _txt([P1, P5]), [P1, P5]), score=10)
        await publicar(shopee(CB, _txt([P2]), [P2]), score=10)
        await publicar(shopee(CC, _txt([P3, P6]), [P3, P6]), score=10)
        s["a"], s["b"], s["c"] = c.criados
        s["antes"] = {m: exibida(m) for m in c.criados}
        nd = shopee(CD, _txt([P1, P2, P3, P4]), [P1, P2, P3, P4])
        await publicar(nd, score=8)
        s["nd"] = nd
    c = cenario(corpo)
    r.check(len(c.criados) == 3 and not c.deletes and not c.edits,
            "45.nada_mudou", f"{c.criados} {c.deletes} {c.edits}")
    r.check({m: exibida(m) for m in c.criados} == s["antes"], "45.estado_preservado")
    todos = [x for m in c.criados for x in conflitos(m)
             if x["msg_id"] == s["nd"].msg_id]
    r.check(len(todos) == 1, "45.um_registro", str(todos))
    cf = todos[0] if todos else {}
    r.check(cf.get("duplicadas") == ",".join(sorted([p(P1), p(P3)]))
            and cf.get("exclusivas") == ",".join(sorted([p(P5), p(P6)]))
            and cf.get("pendentes") == p(P4), "45.conflito_exato", str(cf))
    r.check(vivos_exibindo(p(P4)) == [], "45.P4_pendente_nao_inventado")


def test_46_container_real_resolve_parcial(r):
    """Estado legado parcial A=P1+P2 × B=P2+P3. Chega D=P1+P2+P3 (real):
    AMPLIA o post da família, cobre os dois → um post, nada perdido."""
    P1, P2, P3 = _ps("46", 3)
    agora = time.time()
    db_registrar_post(40461, [p(P1), p(P2)], 10, "a", "shopee", CA, agora + 900, 0,
                      exibidas=[p(P1), p(P2)])
    db_registrar_post(40462, [p(P2), p(P3)], 10, "b", "shopee", CB, agora + 900, 0,
                      exibidas=[p(P2), p(P3)])

    async def corpo(c):
        await publicar(shopee(CD, _txt([P1, P2, P3]), [P1, P2, P3]), score=6)
    c = cenario(corpo)
    vivos = [m for m in (40461, 40462) if vivo(m)]
    r.check(len(vivos) == 1 and not c.criados, "46.um_vivo", str(vivos))
    sv = vivos[0] if vivos else None
    r.check(exibida(sv) == {p(P1), p(P2), p(P3)}, "46.composicao_maxima")
    r.check(all(vivos_exibindo(p(x)) == [sv] for x in (P1, P2, P3)), "46.INV")
    r.check(len(c.deletes) == 1, "46.um_delete", str(c.deletes))


def test_47_fonte_retira_produto_resolve_parcial(r):
    """Estado legado parcial A=P1+P2 × B=P2+P3. O LÍDER de B retira P2:
    B passa a exibir só P3 — a sobreposição some sem fusão."""
    P1, P2, P3 = _ps("47", 3)
    agora = time.time()
    nb = shopee(CB, _txt([P2, P3]), [P2, P3])
    db_registrar_post(40471, [p(P1), p(P2)], 10, "a", "shopee", CA, agora + 900, 0,
                      exibidas=[p(P1), p(P2)])
    db_registrar_post(40472, [p(P2), p(P3)], 10, "b", "shopee", CB, agora + 900, 0,
                      chat_origem=CB, msg_id_origem=nb.msg_id,
                      exibidas=[p(P2), p(P3)])

    async def corpo(c):
        await editar(edicao(nb, _txt([P3]), pids=[P3]))
    c = cenario(corpo)
    r.check(exibida(40472) == {p(P3)} and exibida(40471) == {p(P1), p(P2)},
            "47.sobreposicao_resolvida", f"{exibida(40471)} {exibida(40472)}")
    r.check(vivo(40471) and vivo(40472) and not c.deletes, "47.sem_fusao")
    r.check(conflitos(40472) == [], "47.sem_conflito")


def test_48_cupom_parcial_bloqueado_e_container_resolve(r):
    """Cupom: A={X,Y} e B={Z}; EDIT do líder de B → {Y,Z}: parcial →
    BLOQUEADO. Depois A (líder) passa a {X,Y,Z}: cobre B → B fundido."""
    X, Y, Z = "CUP8BX48", "CUP8BY48", "CUP8BZ48"
    s = {}

    async def corpo(c):
        na = shopee(FADA, f"Cupons {X} {Y}", cupons=[X, Y])
        nb = shopee(PROMOTOM, f"Cupom {Z}", cupons=[Z])
        await publicar(na)
        await publicar(nb)
        s["a"], s["b"] = c.criados
        await editar(edicao(nb, f"Cupons {Y} {Z}", cupons=[Y, Z]))
        s["b_pos_bloqueio"] = exibida(s["b"])
        s["conf"] = conflitos(s["b"])
        await editar(edicao(na, f"Cupons {X} {Y} {Z}", cupons=[X, Y, Z]))
    c = cenario(corpo)
    a, b = s["a"], s["b"]
    r.check(s["b_pos_bloqueio"] == {cup(Z)} and len(s["conf"]) == 1
            and s["conf"][0]["duplicadas"] == cup(Y), "48.parcial_bloqueado", str(s["conf"]))
    r.check(c.vivos_fisicos() == [a] and c.deletes == [b], "48.container_resolve",
            f"{c.vivos_fisicos()} {c.deletes}")
    r.check(all(vivos_exibindo(cup(k)) == [a] for k in (X, Y, Z)), "48.INV_cupom")


def test_49_fusao_herda_maior_janela(r):
    """O sobrevivente herda o MAIOR fim de vida do grupo; evento tardio do
    perdedor dentro dessa janela segue o sobrevivente."""
    agora = time.time()
    P1, P2 = p("8bJ1"), p("8bJ2")
    db_registrar_post(40491, [P1, P2], 5, "s", "shopee", CA, agora + 300, 0,
                      exibidas=[P1, P2])
    db_registrar_post(40492, [P1], 5, "l", "shopee", CB, agora + 1200, 0,
                      chat_origem=CB, msg_id_origem=4949, exibidas=[P1])
    feitos = db_fundir_posts(40491, [40492], agora)
    r.check(feitos == [40492], "49.fundiu")
    r.check(abs(db_get_post(40491)["janela_fim"] - (agora + 1200)) < 1e-3,
            "49.janela_max", str(db_get_post(40491)["janela_fim"] - agora))
    # menor não encolhe
    db_registrar_post(40493, [p("8bJ3")], 5, "x", "shopee", CC, agora + 60, 0,
                      exibidas=[p("8bJ3")])
    db_fundir_posts(40491, [40493], agora)
    r.check(abs(db_get_post(40491)["janela_fim"] - (agora + 1200)) < 1e-3,
            "49.nao_encolhe")
    r.check(destino_vivo_de_origem(CB, 4949) == 40491, "49.tardio_segue_sobrevivente")


def test_50_imagem_independente_da_composicao(r):
    """Composição vem das URLs do texto. (a) texto P1 + imagem (qualquer)
    → composição {P1}: não cobre B=P2. (b) texto P1+P2 + imagem de P1 →
    {P1,P2}: cobre B. A mídia nunca prova nem nega produto."""
    P1, P2, Q1, Q2 = _ps("50", 4)
    s = {}

    async def corpo(c):
        await publicar(shopee(CA, _txt([P1]), [P1]), score=10)
        await publicar(shopee(CB, _txt([P2]), [P2]), score=10)
        s["a"], s["b"] = c.criados
        # (a) mesma composição + imagem, score igual → só mídia
        await publicar(shopee(CC, _txt([P1]) + " FOTO P1+P2", [P1], midia=True,
                              midia_key="k50a"), score=10)
        s["a_a"] = exibida(s["a"])
        s["del_a"] = list(c.deletes)
        # (b) texto P1+P2 + imagem → AMPLIA A, cobre B
        await publicar(shopee(CD, _txt([Q1]), [Q1]), score=10)
        await publicar(shopee(CD, _txt([Q2]), [Q2]), score=10)
        s["q1"], s["q2"] = c.criados[2], c.criados[3]
        await publicar(shopee(CC, _txt([Q1, Q2]) + " FOTO Q1", [Q1, Q2], midia=True,
                              midia_key="k50b"), score=3)
    c = cenario(corpo)
    r.check(s["a_a"] == {p(P1)} and s["del_a"] == [], "50.imagem_nao_amplia",
            f"{s['a_a']} {s['del_a']}")
    r.check(db_get_post(s["a"])["midia_chat"] == CC, "50.midia_aplicada_independente")
    r.check(vivo(s["b"]), "50.B_intacto")
    vq = [m for m in (s["q1"], s["q2"]) if vivo(m)]
    r.check(len(vq) == 1 and exibida(vq[0]) == {p(Q1), p(Q2)}, "50.texto_amplia_com_imagem",
            str(vq))


def test_51_edit_com_midia_bloqueado_aplica_so_midia(r):
    """A=P1+P2; B=P3 (sem mídia); EDIT do líder de B → P2+P3 COM mídia:
    texto BLOQUEADO (conflito), mídia segue a política (upgrade), nenhuma
    consolidação disparada."""
    P1, P2, P3 = _ps("51", 3)
    s = {}

    async def corpo(c):
        await publicar(shopee(CA, _txt([P1, P2]), [P1, P2]))
        nb = shopee(CB, _txt([P3]), [P3])
        await publicar(nb)
        s["a"], s["b"] = c.criados
        with Espiao() as esp:
            await editar(replace(edicao(nb, _txt([P2, P3]), pids=[P2, P3]),
                                 tem_midia=True, media_obj=object(), midia_key="k51"))
            s["esp"] = list(esp.chamadas)
    c = cenario(corpo)
    a, b = s["a"], s["b"]
    r.check(exibida(b) == {p(P3)}, "51.texto_bloqueado", str(exibida(b)))
    r.check(len(conflitos(b)) == 1, "51.conflito_registrado")
    r.check(db_get_post(b)["midia_chat"] == CB, "51.midia_aplicada",
            str(db_get_post(b)["midia_chat"]))
    r.check(s["esp"] == [] and not c.deletes, "51.sem_consolidacao", str(s["esp"]))


def test_52_edit_sync_uma_consolidacao(r):
    """EDIT do líder que prova cobertura: UMA avaliação e UMA consolidação
    (mesma máquina), nunca duas."""
    P1, P2 = _ps("52", 2)
    s = {}
    chamadas = {"avaliar": 0}
    real = convergencia.avaliar

    def conta(*a, **k):
        chamadas["avaliar"] += 1
        return real(*a, **k)

    async def corpo(c):
        na = shopee(CA, _txt([P1]), [P1])
        await publicar(na)
        await publicar(shopee(CB, _txt([P2]), [P2]))
        s["a"], s["b"] = c.criados
        convergencia.avaliar = conta
        try:
            with Espiao() as esp:
                await editar(edicao(na, _txt([P1, P2]), pids=[P1, P2]))
                s["esp"] = list(esp.chamadas)
        finally:
            convergencia.avaliar = real
    c = cenario(corpo)
    r.check(chamadas["avaliar"] == 1 and s["esp"] == [s["a"]], "52.uma_maquina",
            f"avaliar={chamadas['avaliar']} consolidar={s['esp']}")
    r.check(c.deletes == [s["b"]], "52.fundiu_B")


def test_53_composicao_simetrica_12x8(r):
    """A=P1 (12) + D=P1..P4 (8) → AMPLIA: A evolui com o texto de D.
    A=P1..P4 (8) + D=P1 (12) → REDUZ: IGNORAR, A mantém P1..P4."""
    P = _ps("53a", 4)
    Q = _ps("53b", 4)
    s = {}

    async def corpo(c):
        await publicar(shopee(CA, _txt(P[:1]), P[:1]), score=12)
        await publicar(shopee(CD, _txt(P) + " D", P), score=8)
        await publicar(shopee(CA, _txt(Q) + " A", Q), score=8)
        await publicar(shopee(CD, _txt(Q[:1]) + " D", Q[:1]), score=12)
        s["a1"], s["a2"] = c.criados
    c = cenario(corpo)
    r.check(exibida(s["a1"]) == {p(x) for x in P} and score_de(s["a1"]) == 8,
            "53.AMPLIA_evolui_score_menor", f"{exibida(s['a1'])} {score_de(s['a1'])}")
    r.check(exibida(s["a2"]) == {p(x) for x in Q} and score_de(s["a2"]) == 8,
            "53.REDUZ_ignora_score_maior", f"{exibida(s['a2'])} {score_de(s['a2'])}")
    r.check(len(c.criados) == 2 and not c.deletes, "53.sem_post_novo")


def test_54_decisao_pura_por_composicao(r):
    """decidir(): AMPLIA evolui com score menor; REDUZ/PARCIAL ignoram
    com score maior; IGUAL/None mantêm a regra de score (F5)."""
    from pipeline.decisao import decidir
    agora = time.time()
    n = shopee(CB, "x", ["8b54"])
    montada = types.SimpleNamespace(imagem=None, texto="x", msg_id=n.msg_id)
    est = {"msg_id_dest": 1, "score": 10, "janela_fim": agora + 900,
           "edit_count": 0, "lider": CA, "ts": agora, "midia_chat": None}
    res = {}
    for comp in ("AMPLIA", "REDUZ", "PARCIAL", "IGUAL", None):
        for sc in (5, 20):
            try:
                d = decidir(n, montada, sc, dict(est), agora, False, composicao=comp)
                res[(comp, sc)] = (d.acao, d.motivo)
            except Exception as e:                     # noqa: BLE001
                res[(comp, sc)] = ("ERRO", str(e))
    r.check(res[("AMPLIA", 5)] == ("EVOLUIR", "COMPOSICAO_AMPLIADA"), "54.amplia", str(res))
    from config import _MAX_EDITS
    d = decidir(n, montada, 5, dict(est, edit_count=_MAX_EDITS), agora, False,
                composicao="AMPLIA")
    r.check((d.acao, d.motivo) == ("IGNORAR", "EVOLUCAO_LIMITE_ATINGIDO"),
            "54.amplia_respeita_teto", f"{d.acao} {d.motivo}")
    d = decidir(n, montada, 5, dict(est), agora, False, composicao="AMPLIA")
    r.check(d.novo_score == 5, "54.amplia_grava_score_do_texto", str(d.novo_score))
    r.check(all(res[(k, 20)] == ("IGNORAR", "COMPOSICAO_PERDERIA_IDENTIDADE")
                for k in ("REDUZ", "PARCIAL")), "54.reduz_parcial", str(res))
    r.check(res[("IGUAL", 20)][0] == "EVOLUIR" and res[("IGUAL", 5)][0] == "IGNORAR"
            and res[("IGUAL", 20)] == res[(None, 20)]
            and res[("IGUAL", 5)] == res[(None, 5)], "54.igual_e_none_por_score", str(res))


def test_55_concorrencia_container_e_edicoes(r):
    """A=P1, B=P2; EDIT A→P1+P2 e NEW D=P1+P2 (outra fonte) em paralelo:
    um vivo, P1/P2 num só post, nenhum conflito residual."""
    P1, P2 = _ps("55", 2)
    s = {}

    async def corpo(c):
        na = shopee(CA, _txt([P1]), [P1])
        await publicar(na)
        await publicar(shopee(CB, _txt([P2]), [P2]))
        await asyncio.gather(
            editar(edicao(na, _txt([P1, P2]), pids=[P1, P2])),
            publicar(shopee(CD, _txt([P2, P1]) + " D", [P2, P1]), score=4))
    c = cenario(corpo)
    vivos = c.vivos_fisicos()
    r.check(len(c.criados) == 2 and len(vivos) == 1, "55.um_vivo",
            f"{c.criados} {vivos} {c.deletes}")
    sv = vivos[0] if vivos else None
    r.check(vivos_exibindo(p(P1)) == [sv] and vivos_exibindo(p(P2)) == [sv], "55.INV")


def test_56_renascer_bloqueado_por_parcial(r):
    """RENASCER também passa pelo veredito: o post novo não pode nascer
    duplicando o exclusivo de outro vivo sem cobertura."""
    agora = time.time()
    P1, P2, P3 = p("8b561"), p("8b562"), p("8b563")
    db_registrar_post(40561, [P1, P2], 5, "a", "shopee", CA, agora + 900, 0,
                      exibidas=[P1, P2])
    db_registrar_post(40562, [P3], 5, "b", "shopee", CB, agora + 900, 0,
                      exibidas=[P3])
    v_ok = convergencia.avaliar(-1, [P3], 5, superar=40562)
    v_no = convergencia.avaliar(-1, [P2, P3], 5, superar=40562)
    r.check(v_ok.permitido and not v_no.permitido
            and v_no.motivo == "CONFLITO_ESTRUTURAL", "56.veredito_renascer")
    # o post SUPERADO (histórico) não conta como vizinho do renascido
    v_sup = convergencia.avaliar(-1, [P2, p("8b569")], 5, superar=40561)
    r.check(v_sup.permitido, "56.superado_nao_conflita")



def test_57_posse_vai_para_quem_exibe(r):
    """X={P1,P2} (dono dos dois, score menor), Y={P1,P3}, Z={P2,P4}: X é
    coberto pela UNIÃO de Y e Z → fundido; P1 fica com Y, P2 com Z
    (cada âncora com o post que a EXIBE), não tudo com o principal."""
    agora = time.time()
    P1, P2, P3, P4 = p("8b571"), p("8b572"), p("8b573"), p("8b574")
    db_registrar_post(40571, [P1, P2], 1, "x", "shopee", CA, agora + 900, 0,
                      exibidas=[P1, P2])
    db_registrar_post(40572, [P1, P3], 10, "y", "shopee", CB, agora + 900, 0,
                      exibidas=[P1, P3])
    db_registrar_post(40573, [P2, P4], 10, "z", "shopee", CC, agora + 900, 0,
                      exibidas=[P2, P4])
    s = {}

    async def corpo(c):
        s["f"] = await convergencia.consolidar(40572)
    c = cenario(corpo)
    r.check(s["f"] == [40571] and c.deletes == [40571], "57.X_fundido", str(s["f"]))
    r.check(dono(P1) == 40572 and dono(P2) == 40573, "57.posse_por_exibicao",
            f"{dono(P1)} {dono(P2)}")
    r.check(vivo(40572) and vivo(40573), "57.Y_Z_vivos")


def test_58_mesma_lista_outro_codigo_score_decide(r):
    """Frente 7B preservada: mesma lista (destino) com OUTRO código é a
    mesma oferta — o score decide o texto, a composição não bloqueia."""
    ls = lista("_Container_f8b-58", "58580000")
    t = "🔥 Cupom Mercado Livre em Selecionados\n🎟 R$ 50 OFF: {c}\nLista: https://meli.la/58"
    s = {}

    async def corpo(c):
        await publicar(ml(CA, t.format(c="L58X"), [ls], ["L58X"]), score=10)
        s["a"] = c.criados[0]
        await publicar(ml(CB, t.format(c="L58Y") + " MELHOR", [ls], ["L58Y"]), score=20)
        s["ex1"] = exibida(s["a"])
        await publicar(ml(CC, t.format(c="L58W") + " PIOR", [ls], ["L58W"]), score=5)
    c = cenario(corpo)
    a = s["a"]
    r.check(c.criados == [a] and not c.deletes, "58.um_post", str(c.criados))
    r.check("mercadolivre|cup|L58Y" in s["ex1"] and any("MELHOR" in x for _, x in c.edits),
            "58.score_maior_evolui", str(s["ex1"]))
    r.check(not any("PIOR" in x for _, x in c.edits), "58.score_menor_ignora")
    r.check(conflitos(a) == [], "58.sem_conflito")


def test_59_renascer_supera_o_proprio_post(r):
    """RENASCER com composição PARCIAL em relação ao próprio post: o post
    superado vira histórico e NÃO conta como vizinho — o renascimento
    acontece (o veredito não o bloqueia contra ele mesmo)."""
    X, Y = _codigos("T59")
    Z = "EXTRAT59"
    s = {}

    async def corpo(c):
        await publicar(shopee(FADA, _t(T_FADA_2, "T59"), cupons=[X, Y]))
        s["a"] = c.criados[0]
        nv = shopee(SAMUEL, "🔥 VOLTOU! " + _t(T_SAM_MOD, "T59") + f"\n🎟 {Z}",
                    cupons=[Y, Z])
        s["nv"] = nv
        await publicar(nv)
    c = cenario(corpo)
    a = s["a"]
    r.check(len(c.criados) == 2, "59.renasceu", str(c.criados))
    n = c.criados[-1]
    r.check(exibida(n) == {cup(Y), cup(Z)} and exibida(a) == set(), "59.novo_exibe_o_candidato",
            f"{exibida(n)} {exibida(a)}")
    with _db() as db:
        n_conf = db.execute("SELECT COUNT(*) FROM conflito_estrutural WHERE msg_id=?",
                            (s["nv"].msg_id,)).fetchone()[0]
    r.check(n_conf == 0, "59.sem_bloqueio", str(n_conf))
    r.check(not c.deletes, "59.historico_nao_apagado")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "FRENTE 8 · convergência / fusão de famílias · árvore real"))
