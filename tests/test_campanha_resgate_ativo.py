"""
CAMPANHA POR PÁGINA DE RESGATE — ATIVO (correção funcional atrás de
config.CAMPANHA_RESGATE_MODO, padrão "SOMBRA").

Casos reais (logs da Railway + prévia pública t.me/s; 28/10/2026, UTC):
  03:00  Promotom 112024 "Cupons Shopee" → post 31943 (`cupb|geral`, nos dois
         canais, score 3); Fada 18042 "Começou! Esquenta 11.11 Shopee" 44 s
         depois → post NOVO 31945 (`url|<página>`, só no principal, score 3).
         O dono apagou o 31943 e encaminhou à mão o 31945 ao canal de cupons
         (FullPromotionCupons 618, "encaminhado").
  11:59  Fada 18049 "Cupons Renovados! Esquenta Shopee" → post 32138
         (`cupb|esquenta`), editada 13 s depois com o código 3SQU3NT411
         (R$150 OFF em R$999, score 9); Promotom 112034 "Cupons Shopee" 18 s
         depois → post NOVO 32139 (CUPOM_INCERTO sem_beneficio, score 3). O
         dono apagou o 32139 às 12:08 (APAGADO_DONO via=TELEGRAM).
Os dois posts de cada par levavam à MESMA página de resgate: o 1º link
publicado é s.shopee.com.br/AAHdNLxGSX nos quatro.

Todo cenário roda o caminho REAL a partir da normalização (o mesmo de
test_campanha_resgate: normalizar → enriquecer → deve_enviar_async → montar
→ publicacao.enviar), com o Telegram falso por canal. ANTES = SOMBRA (o
padrão, igual ao #34); DEPOIS = ATIVO. Texto marcado RECONSTRUÍDO: a prévia
só mostra a versão final da mensagem editada; a 1ª versão é a final sem o
bloco que o log prova ter chegado depois (links/codes da linha 📩).
Texto marcado SINTÉTICO: o corpus não tem o par; o cenário é montado com
textos e links reais.

O QUE ESTE ARQUIVO PROVA:
  01  flag: padrão SOMBRA; valor desconhecido vale SOMBRA; a trava sintética
      `<plat>|resgate*|<página>` só existe em ATIVO e só para quem entra na
      regra (nunca produto, nunca outra página)
  02  regra pura COM CÓDIGO: a junção nunca esconde código (post com código ×
      candidato sem; candidato com código × post sem; códigos diferentes);
      dado ausente não junta; sem com_codigo é a regra do #34
  03  03:00 real — antes 2 posts; depois 1 post nos dois canais, com o texto
      que já estava no ar (empate de score 3 × 3)
  04  03:00 na ordem inversa (Fada primeiro) — depois 1 post, também no
      canal de cupons (a página declarada roteia)
  05  11:59 real — antes 2 posts; depois 1 post: o texto mais rico (com o
      código, score 9) fica; a Promotom é absorvida
  06  11:59 inverso, com o código já na 1ª versão — o texto mais rico chega
      depois e EVOLUI o post (nunca um 2º post)
  07  campanhas distintas na mesma página — nome em conflito, códigos
      diferentes, mesma fonte, acima do SUPERIOR: nunca juntam
  08  benefícios conflitantes: nunca juntam (pela regra de cupom ou pela da
      campanha)
  09  múltiplos candidatos: dois válidos → INCERTO ambiguo (o comportamento
      de hoje); um válido + um em conflito → junta no válido
  10  falha de leitura do banco (cada etapa): nenhuma junção, decisão
      INDETERMINADO em WARNING, publicação segue; erro interno do adaptador
      e log que levanta não mudam a publicação nem a junção
  11  publicações apagadas à mão — cópia AUSENTE e cópia ainda não
      descoberta (nunca volta); principal apagado pelo dono (nunca é
      escolhido; não renasce); principal apagado ainda não descoberto (a
      junção não cria 2º post)
  12  monotonicidade: família de hoje e decisão da regra de cupom nunca são
      trocadas (inclusive a guarda de defesa, com a regra pura forçada a
      JUNTA); a junção só transforma "post novo"
  13  corrida: duas mensagens da campanha ao mesmo tempo → 1 post com a
      trava; sem a trava, 2 (a trava é necessária)
  14  SOMBRA não muda nada: a campanha "desligada por dentro" × SOMBRA dão as
      mesmas chamadas ao Telegram, o mesmo banco e as mesmas decisões
  15  edição nunca passa pela regra (vínculo de origem)
  16  a régua da guarda de código é a da decisão de evolução: empate não
      vence, score maior vence, orçamento = edit_count < _MAX_EDITS

    python tests/test_campanha_resgate_ativo.py
"""
import asyncio
import contextlib
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_campanha_resgate as C                               # noqa: E402  (preparar())
from test_campanha_resgate import (CANAL_CUPONS, FUMOTOM, PAGINA,  # noqa: E402
                                   PUB_PAGINA, Canais, _banco, _drenar)
from test_cupom_sem_codigo import FADA, PROMOTOM, SAMUEL         # noqa: E402
import test_cupom_sem_codigo as T                               # noqa: E402
from _telethon_fakes import afetadas                            # noqa: E402
from _harness_e5 import rodar                                   # noqa: E402

import config                                                   # noqa: E402
from database import db_get_post                                # noqa: E402
from database_espelho import db_espelho_get                     # noqa: E402
from pipeline import origem                                     # noqa: E402
from pipeline import campanha_resgate as regra                  # noqa: E402
from pipeline import campanha_resgate_sombra as sombra          # noqa: E402
from pipeline import (cupom_lancamento, espelho_cupons,         # noqa: E402
                      normalizacao, post_apagado, publicacao)
from pipeline.deduplicacao import deve_enviar_async             # noqa: E402
from pipeline.enriquecimento import enriquecer, enriquecer_edicao  # noqa: E402
from pipeline.ingestao import MensagemBruta, links_de           # noqa: E402
from pipeline.montagem import montar                            # noqa: E402
from pipeline.resolucao_identidade import eh_identidade_cupom   # noqa: E402

PRINCIPAL = str(config.GRUPO_DESTINO)

# Resolução PROVADA (a do #34) + os links novos de 11:59.
RES = dict(C.RESOLUCAO)
RES.update({
    "https://s.shopee.com.br/1qU7Zs67MB": ("https://s.shopee.com.br/8pkZllbmly", None,
        "post 32138 (2º link; log 11:59:41 links=2 codes=1)"),
})

# ══════════════════════════════════════════════════════════════════
# Textos
# ══════════════════════════════════════════════════════════════════
P_112024, F_18042, S_123470, FU_42674 = C.P_112024, C.F_18042, C.S_123470, C.FU_42674
P_112034 = P_112024                     # 11:59:46 → 32139 (prévia: texto idêntico ao 112024)
F_18049_V1 = ("**🚨 Cupons Renovados! Esquenta Shopee **\n\n**✅ Resgate todos os cupons aqui:**\n"
              "https://s.shopee.com.br/40aG8LLgaE")      # 11:59:28 → 32138 — RECONSTRUÍDO (log: links=1 codes=0)
F_18049 = (F_18049_V1 + "\n\n**🎟 R$150 OFF em R$999: **`3SQU3NT411`\n"
           "https://s.shopee.com.br/1qU7Zs67MB")         # edição 11:59:41 (prévia; log: links=2 codes=1)
S_123584_V1 = ("🔥 **Cupons Renovados! Esquenta Shopee**\n\n⭐️ **Resgate todos os cupons aqui:**\n"
               "https://s.shopee.com.br/9APkSfq3k8\n\nanúncio")   # 12:00:02 — RECONSTRUÍDO (links=1 codes=0)
S_123584 = ("🔥 **Cupons Renovados! Esquenta Shopee**\n\n🎟 **R$150 OFF em R$999:** `3SQU3NT411`\n\n"
            "⭐️ **Resgate todos os cupons aqui:**\nhttps://s.shopee.com.br/9APkSfq3k8\n\n"
            "🛒 **Link carrinho:**\nhttps://s.shopee.com.br/1142oVQavZ\n\nanúncio")  # edição 12:00:30 (prévia)
# SINTÉTICOS (textos e links reais, par que o corpus não tem)
P_TECH = ("Cupons Shopee Tech\n\n-Resgate aqui:\n1 https://s.shopee.com.br/2B5snGi9NX\n"
          "2 https://s.shopee.com.br/qaVCprDoA")
S_OUTRO_CODIGO = ("🔥 **Cupom Shopee**\n\n🎟 **R$150 OFF em R$999:** `NOVO150XYZ`\n\n"
                  "⭐️ **Resgate todos os cupons aqui:**\nhttps://s.shopee.com.br/6L5YgZgkK8")
S_CODIGO_20 = ("🔥 **Cupom Shopee**\n\n🎟 **R$20 OFF em R$60:** `NOVO20XYZ`\n\n"
               "⭐️ **Resgate todos os cupons aqui:**\nhttps://s.shopee.com.br/6L5YgZgkK8")
F_NOVOS = C.F_17997                     # "Novos Cupons Shopee" + página (Fada, 26/10)
F_ESQUENTA_150 = ("**🚨 Começou! Esquenta 11.11 Shopee **\n\n**R$150 OFF em R$999**\n\n"
                  "**✅ Resgate todos os cupons aqui:**\nhttps://s.shopee.com.br/40aG8LLgaE")
P_20_60 = ("Cupons Shopee\n\nR$ 20 OFF em R$ 60\n\n-Resgate aqui:\n"
           "1 https://s.shopee.com.br/2B5snGi9NX\n2 https://s.shopee.com.br/qaVCprDoA")
P_150 = ("Cupons Shopee\n\nR$ 150 OFF em R$ 999\n\n-Resgate aqui:\n"
         "1 https://s.shopee.com.br/2B5snGi9NX\n2 https://s.shopee.com.br/qaVCprDoA")


# ══════════════════════════════════════════════════════════════════
# Execução: o cenário do #34 com o modo da campanha escolhido
# ══════════════════════════════════════════════════════════════════
def _bruta(chat, mid, texto):
    """Como a ingestão de produção: o trecho entre crases chega também
    como code_entity (ingestao._extrair_code_entities)."""
    return MensagemBruta(msg_id=mid, chat=str(chat), texto=texto, links=links_de(texto),
                         tem_midia=False, media_obj=None,
                         code_entities=re.findall(r"`([^`]+)`", texto))


class Noite(C.Noite):
    async def chega(self, chat, mid, texto):
        norm = await normalizacao.normalizar(_bruta(chat, mid, texto))
        self.norms[(chat, mid)] = norm
        if norm is not None:
            enr = enriquecer(norm)
            if await deve_enviar_async(enr):
                await publicacao.enviar(await montar(norm), norm, enr=enr, is_edit=False)
        await _drenar()
        return norm

    async def edita(self, chat, mid, texto):
        norm = await normalizacao.normalizar(_bruta(chat, mid, texto))
        if norm is not None:
            enr = enriquecer_edicao(norm)
            if await deve_enviar_async(enr):
                await publicacao.enviar(await montar(norm), norm, enr=enr, is_edit=True)
        await _drenar()
        return norm


C.Noite = Noite          # o cenário do #34 passa a usar a ingestão com code_entities


@contextlib.contextmanager
def _modo(valor):
    antigo = config.CAMPANHA_RESGATE_MODO
    config.CAMPANHA_RESGATE_MODO = valor
    try:
        yield
    finally:
        config.CAMPANHA_RESGATE_MODO = antigo


def noite(corpo, modo, resolucao=RES):
    """(noite, captura) do cenário `corpo` com CAMPANHA_RESGATE_MODO = modo
    (CUPOM_MODO = ATIVO, o da produção). O banco é um só por processo: a
    fotografia (posts por origem, estado de cada post) é tirada ao fim da
    rodada, antes da próxima."""
    with _modo(modo):
        nt, cap = C.cenario(corpo, resolucao=resolucao)
    nt.posts = {k: origem.consultar(str(k[0]), k[1]) for k in nt.norms}
    nt.estado = {pid: db_get_post(pid) or {} for pid in set(nt.posts.values()) if pid}
    return nt, cap


def P(nt, chat, mid):
    return nt.posts.get((chat, mid))


def score(nt, pid):
    return (nt.estado.get(pid) or {}).get("score")


def _ler(linha):
    d = C._ler(linha)
    d["aplicada"] = (linha.split(" aplicada=", 1)[1].split()[0]
                     if " aplicada=" in linha else "")
    d["pendente"] = " aprovacao=pendente " in linha
    return d


def linhas_de(cap, msg):
    return [d for d in map(_ler, cap.linhas) if d["msg"] == msg]


def textos(nt, canal):
    return [t for _i, t in sorted(nt.cli.no_ar.get(canal, {}).items())]


def resumo(nt):
    return (len(nt.cli.principal()), len(nt.cli.cupons()))


async def _caso_0300(nt):
    await nt.chega(PROMOTOM, 112024, P_112024)
    await nt.chega(FADA, 18042, F_18042)
    await nt.chega(SAMUEL, 123470, S_123470)
    await nt.chega(FUMOTOM, 42674, FU_42674)


async def _caso_1159(nt):
    await nt.chega(FADA, 18049, F_18049_V1)
    await nt.edita(FADA, 18049, F_18049)
    await nt.chega(PROMOTOM, 112034, P_112034)
    await nt.chega(SAMUEL, 123584, S_123584_V1)
    await nt.edita(SAMUEL, 123584, S_123584)


# ══════════════════════════════════════════════════════════════════
# 01 — flag e trava
# ══════════════════════════════════════════════════════════════════
def test_01_flag_e_trava(r):
    r.check(config.CAMPANHA_RESGATE_MODO == "SOMBRA", "01.padrao_sombra",
            config.CAMPANHA_RESGATE_MODO)
    for valor, esperado in (("SOMBRA", "SOMBRA"), ("ATIVO", "ATIVO"), ("ativo", "SOMBRA"),
                            ("", "SOMBRA"), ("ATIVO_IGUAL", "SOMBRA"), (None, "SOMBRA")):
        with _modo(valor):
            r.check(sombra.modo() == esperado, f"01.modo.{valor!r}", sombra.modo())
    out = {}

    async def corpo(nt):
        for chat, mid, texto in ((PROMOTOM, 112024, P_112024), (FADA, 18042, F_18042),
                                 (FUMOTOM, 42674, FU_42674), (FADA, 17927, C.F_17927)):
            n = await normalizacao.normalizar(_bruta(chat, mid, texto))
            ofs = enriquecer(n).ofertas
            for m in ("SOMBRA", "ATIVO"):
                with _modo(m):
                    out[(mid, m)] = sombra.mutexes(n, ofs)
        with _modo("ATIVO"):
            out["none"] = sombra.mutexes(None, ["shopee|cupb|geral"])
    noite(corpo, "SOMBRA")
    trava = f"shopee|resgate*|{PAGINA}"
    r.check(all(out[(m, "SOMBRA")] == [] for m in (112024, 18042, 42674, 17927)),
            "01.sombra_sem_trava", str(out))
    r.check(out[(112024, "ATIVO")] == [trava] and out[(18042, "ATIVO")] == [trava],
            "01.ativo_trava_da_campanha", str(out))
    r.check(out[(42674, "ATIVO")] == [], "01.produto_sem_trava", str(out[(42674, "ATIVO")]))
    r.check(out[(17927, "ATIVO")] == [], "01.outra_pagina_sem_trava",
            str(out[(17927, "ATIVO")]))
    r.check(out["none"] == [], "01.sem_norm_sem_trava")
    # a trava não é chave real: nenhuma espécie `resgate*` existe
    r.check("*" in trava and not regra.ancora_estrutural([trava]),
            "01.trava_nao_e_chave_real")


# ══════════════════════════════════════════════════════════════════
# 02 — regra pura com código
# ══════════════════════════════════════════════════════════════════
AGORA, SUP = C.AGORA, C.SUP
COD_A, COD_B = "shopee|cup|3SQU3NT411", "shopee|cup|NOVO150XYZ"


def _vivo(mid, texto, chaves, score_maior=None, orcamento=None, fontes=(FADA,)):
    v = C._vivo(mid, texto, chaves=chaves, fontes=fontes)
    return regra.PostVivo(v.msg_id, v.plat, v.chaves, v.primeiro_link, v.perfil,
                          v.nascimento, v.fontes, score_maior, orcamento)


def test_02_regra_pura_com_codigo(r):
    pub_32138 = ("🚨 Cupons Renovados! Esquenta Shopee\n\n✅ Resgate todos os cupons aqui:\n"
                 + PUB_PAGINA + "\n\n🎟 R$150 OFF em R$999: 3SQU3NT411\nhttps://s.shopee.com.br/8pkZllbmly")
    com_cod = ("shopee|cupb|esquenta", COD_A)
    gen = C._cand(P_112034, chat=PROMOTOM, chaves=("shopee|cupb|geral",))

    def av(c, vivos, com=True):
        return regra.avaliar(c, vivos, AGORA, SUP, com_codigo=com)
    # sem com_codigo: a regra do #34 (post com código nem é par)
    d = av(gen, [_vivo(32138, pub_32138, com_cod, False, True)], com=False)
    r.check((d.acao, d.motivo) == (regra.MANTER, "sem_par"), "02.sem_com_codigo_e_o_34", str(d))
    d = av(C._cand(F_18049, chaves=com_cod), [], com=False)
    r.check(d.motivo == "fora:ancora_forte", "02.sem_com_codigo_candidato_com_codigo_fora", str(d))
    # 11:59: post com código, candidato sem, candidato NÃO vence pelo score → junta
    d = av(gen, [_vivo(32138, pub_32138, com_cod, score_maior=False, orcamento=True)])
    r.check((d.acao, d.post, d.pendente) == (regra.JUNTA, 32138, True),
            "02.post_com_codigo_candidato_menor_junta", str(d))
    # ... mas se venceria (o texto sem código substituiria): não junta
    d = av(gen, [_vivo(32138, pub_32138, com_cod, score_maior=True, orcamento=True)])
    r.check((d.acao, d.motivo) == (regra.INCERTO, "codigo_do_post_sairia"),
            "02.candidato_maior_esconderia_codigo", str(d))
    d = av(gen, [_vivo(32138, pub_32138, com_cod, score_maior=None, orcamento=None)])
    r.check((d.acao, d.motivo) == (regra.INCERTO, "codigo_do_post_sairia"),
            "02.score_desconhecido_nao_junta", str(d))
    # candidato com código, post sem: só se o texto com código ENTRA
    cand_cod = C._cand(F_18049, chaves=com_cod)
    pub_gen = C.PUB_31943
    for sm, orc, ok in ((True, True, True), (False, True, False), (True, False, False),
                        (None, True, False), (True, None, False)):
        d = av(cand_cod, [_vivo(31943, pub_gen, ("shopee|cupb|geral",), sm, orc,
                                fontes=(PROMOTOM,))])
        esperado = (regra.JUNTA if ok else regra.INCERTO)
        r.check(d.acao == esperado and (ok or d.motivo == "codigo_novo_ficaria_oculto"),
                f"02.candidato_com_codigo.score_maior={sm}.orcamento={orc}", str(d))
    # códigos diferentes nos dois lados: nunca, qualquer score
    for sm in (True, False):
        d = av(C._cand(S_OUTRO_CODIGO, chat=SAMUEL, chaves=(COD_B,)),
               [_vivo(32138, pub_32138, com_cod, sm, True)])
        r.check((d.acao, d.motivo) == (regra.INCERTO, "codigos_distintos"),
                f"02.codigos_distintos.{sm}", str(d))
    # parte dos códigos: REDUZ (nunca substitui) e AMPLIA (só orçamento)
    r.check(regra.codigo_preservado([COD_A], _vivo(1, pub_32138, (COD_A, COD_B))) == "",
            "02.reduz_sempre_preserva")
    r.check(regra.codigo_preservado([COD_A, COD_B], _vivo(1, pub_32138, (COD_A,), None, True)) == "",
            "02.amplia_com_orcamento")
    r.check(regra.codigo_preservado([COD_A, COD_B], _vivo(1, pub_32138, (COD_A,), True, False))
            == "codigo_novo_ficaria_oculto", "02.amplia_sem_orcamento")
    # produto e destino nunca, nem com código
    r.check(not regra.entra_na_regra(["shopee|1930141978.58270173531", COD_A], True)
            and not regra.entra_na_regra(["mercadolivre|dest|abc"], True)
            and regra.entra_na_regra([COD_A, "shopee|cupb|x"], True)
            and not regra.entra_na_regra([], True), "02.ancora_estrutural_fora")
    # roteamento: só a página declarada como destino, sem produto/destino
    r.check(regra.so_pagina_de_resgate([f"shopee|url|{PAGINA}"])
            and not regra.so_pagina_de_resgate([f"shopee|url|{PAGINA}", "shopee|123.456"])
            and not regra.so_pagina_de_resgate(["shopee|url|https://shopee.com.br/m/11-11"])
            and not regra.so_pagina_de_resgate([f"mercadolivre|url|{PAGINA}"])
            and not regra.so_pagina_de_resgate([]), "02.so_pagina_de_resgate")


# ══════════════════════════════════════════════════════════════════
# 03/04 — 28/10 03:00, real e invertido
# ══════════════════════════════════════════════════════════════════
def test_03_caso_0300_antes_e_depois(r):
    antes, cap_a = noite(_caso_0300, "SOMBRA")
    depois, cap_d = noite(_caso_0300, "ATIVO")
    pa_a, pf_a = P(antes, PROMOTOM, 112024), P(antes, FADA, 18042)
    r.check(pa_a != pf_a and resumo(antes) == (3, 1), "03.antes_2_posts_da_campanha",
            f"{resumo(antes)} {pa_a} {pf_a}")
    pa, pf, ps = (P(depois, PROMOTOM, 112024), P(depois, FADA, 18042),
                  P(depois, SAMUEL, 123470))
    r.check(pa == pf == ps, "03.depois_1_post", f"{pa} {pf} {ps}")
    r.check(resumo(depois) == (2, 1), "03.depois_principal_2_cupons_1 (campanha + produto)",
            str(resumo(depois)))
    r.check(P(depois, FUMOTOM, 42674) not in (pa, None), "03.produto_continua_separado")
    lf = linhas_de(cap_d, 18042)
    r.check(len(lf) == 1 and (lf[0]["acao"], lf[0]["post"], lf[0]["aplicada"]) ==
            ("JUNTA", pa, "sim") and lf[0]["pendente"], "03.linha_junta_aplicada",
            "\n".join(cap_d.linhas))
    # texto: empate de score (3 × 3) — fica o que está no ar (o da Promotom)
    pub = depois.cli.no_ar[PRINCIPAL][pa]
    r.check("Cupons Shopee" in pub and "Esquenta" not in pub, "03.empate_mantem_o_texto_no_ar",
            pub)
    r.check(textos(depois, CANAL_CUPONS) == [pub], "03.mesma_copia_no_canal_de_cupons")
    r.check(score(depois, pa) == 3, "03.score_do_texto_publicado", str(score(depois, pa)))
    # SOMBRA: nenhuma linha diz aplicada
    r.check(all(_ler(x)["aplicada"] == "" for x in cap_a.linhas), "03.sombra_sem_campo_aplicada")


def test_04_caso_0300_invertido(r):
    async def corpo(nt):
        await nt.chega(FADA, 18042, F_18042)
        await nt.chega(PROMOTOM, 112024, P_112024)
        await nt.chega(SAMUEL, 123470, S_123470)
    antes, _ca = noite(corpo, "SOMBRA")
    depois, cap = noite(corpo, "ATIVO")
    r.check(resumo(antes) == (2, 1) and P(antes, FADA, 18042) != P(antes, PROMOTOM, 112024),
            "04.antes_2_posts_fada_so_no_principal", str(resumo(antes)))
    pf = P(depois, FADA, 18042)
    r.check(pf == P(depois, PROMOTOM, 112024) == P(depois, SAMUEL, 123470),
            "04.depois_1_post")
    r.check(resumo(depois) == (1, 1), "04.depois_tambem_no_canal_de_cupons", str(resumo(depois)))
    r.check(textos(depois, CANAL_CUPONS) == textos(depois, PRINCIPAL)
            and "Esquenta" in textos(depois, PRINCIPAL)[0], "04.copia_igual_ao_principal",
            str(textos(depois, PRINCIPAL)))
    r.check([d["aplicada"] for d in linhas_de(cap, 112024)] == ["sim"], "04.promotom_junta",
            "\n".join(cap.linhas))


# ══════════════════════════════════════════════════════════════════
# 05/06 — 28/10 11:59, real e invertido
# ══════════════════════════════════════════════════════════════════
def test_05_caso_1159_antes_e_depois(r):
    antes, _ca = noite(_caso_1159, "SOMBRA")
    depois, cap = noite(_caso_1159, "ATIVO")
    r.check(P(antes, FADA, 18049) != P(antes, PROMOTOM, 112034) and resumo(antes) == (2, 2),
            "05.antes_2_posts_nos_dois_canais (32138 + 32139)", str(resumo(antes)))
    pf = P(depois, FADA, 18049)
    r.check(pf == P(depois, PROMOTOM, 112034) == P(depois, SAMUEL, 123584),
            "05.depois_1_post")
    r.check(resumo(depois) == (1, 1), "05.depois_1_em_cada_canal", str(resumo(depois)))
    pub = depois.cli.no_ar[PRINCIPAL][pf]
    r.check("3SQU3NT411" in pub and "Renovados" in pub, "05.fica_o_texto_mais_rico", pub)
    r.check(textos(depois, CANAL_CUPONS) == [pub], "05.copia_igual")
    r.check(score(depois, pf) == 9, "05.score_9_do_texto_no_ar", str(score(depois, pf)))
    lp = linhas_de(cap, 112034)
    r.check(len(lp) == 1 and (lp[0]["acao"], lp[0]["aplicada"]) == ("JUNTA", "sim"),
            "05.promotom_junta", "\n".join(cap.linhas))
    r.check("SCORE_NAO_EVOLUI" in T.MOTIVOS, "05.promotom_nao_substitui_o_texto",
            str(T.MOTIVOS))


def test_06_caso_1159_invertido_texto_mais_rico_evolui(r):
    async def corpo(nt):
        await nt.chega(PROMOTOM, 112034, P_112034)
        nt.n_promotom = dict(nt.cli.no_ar.get(PRINCIPAL, {}))
        await nt.chega(FADA, 18049, F_18049)          # com o código já na 1ª versão
    antes, _ca = noite(corpo, "SOMBRA")
    depois, cap = noite(corpo, "ATIVO")
    r.check(resumo(antes) == (2, 2), "06.antes_2_posts", str(resumo(antes)))
    pp = P(depois, PROMOTOM, 112034)
    r.check(pp == P(depois, FADA, 18049) and resumo(depois) == (1, 1),
            "06.depois_1_post", str(resumo(depois)))
    pub = depois.cli.no_ar[PRINCIPAL][pp]
    r.check("3SQU3NT411" in pub and "Renovados" in pub, "06.o_mais_rico_entra_por_edicao", pub)
    r.check(textos(depois, CANAL_CUPONS) == [pub], "06.copia_editada_junto")
    r.check([c[0] for c in depois.cli.chamadas if c[1] == PRINCIPAL] == ["send", "edit"],
            "06.uma_publicacao_e_uma_edicao",
            str([c[:3] for c in depois.cli.chamadas]))
    r.check("EVOLUI" in T.MOTIVOS, "06.decisao_evolui", str(T.MOTIVOS))
    r.check(score(depois, pp) == 9, "06.score_acompanha_o_texto", str(score(depois, pp)))


# ══════════════════════════════════════════════════════════════════
# 07/08 — campanhas distintas na mesma página; benefícios conflitantes
# ══════════════════════════════════════════════════════════════════
def _dois_posts(r, nome, corpo, primeiro, segundo, motivo=None):
    for modo in ("SOMBRA", "ATIVO"):
        nt, cap = noite(corpo, modo)
        a, b = P(nt, *primeiro), P(nt, *segundo)
        r.check(a is not None and b is not None and a != b, f"{nome}.{modo}.dois_posts",
                f"{a} {b} {resumo(nt)}")
        ls = linhas_de(cap, segundo[1])
        r.check(not [d for d in ls if d["aplicada"] == "sim"], f"{nome}.{modo}.nada_aplicado",
                "\n".join(cap.linhas))
        if modo == "ATIVO" and motivo:
            r.check(any(d["motivo"] == motivo for d in ls) or motivo == "-",
                    f"{nome}.{modo}.motivo={motivo}", "\n".join(cap.linhas))


def test_07_campanhas_distintas_na_mesma_pagina(r):
    async def nome(nt):                 # "Esquenta" (só página) × "Tech"
        await nt.chega(FADA, 18042, F_18042)
        await nt.chega(PROMOTOM, 900001, P_TECH)
    _dois_posts(r, "07.nome_em_conflito", nome, (FADA, 18042), (PROMOTOM, 900001),
                "nome_ou_beneficio_em_conflito")

    async def codigos(nt):              # dois códigos diferentes, mesma página
        await nt.chega(FADA, 18049, F_18049)
        await nt.chega(SAMUEL, 900002, S_OUTRO_CODIGO)
    _dois_posts(r, "07.codigos_distintos", codigos, (FADA, 18049), (SAMUEL, 900002),
                "codigos_distintos")
    nt, _cap = noite(codigos, "ATIVO")
    tudo = " ".join(textos(nt, PRINCIPAL))
    r.check("3SQU3NT411" in tudo and "NOVO150XYZ" in tudo, "07.os_dois_codigos_no_ar", tudo)

    async def mesma_fonte(nt):          # a Fada publica duas campanhas na página
        await nt.chega(FADA, 18042, F_18042)
        nt.recuar(10 * 60)
        await nt.chega(FADA, 900003, F_NOVOS)
    _dois_posts(r, "07.mesma_fonte", mesma_fonte, (FADA, 18042), (FADA, 900003), "mesma_fonte")

    async def acima(nt):                # outra fonte, 31 min depois
        await nt.chega(FADA, 18042, F_18042)
        nt.recuar(31 * 60)
        await nt.chega(PROMOTOM, 112024, P_112024)
    _dois_posts(r, "07.acima_do_superior", acima, (FADA, 18042), (PROMOTOM, 112024),
                "acima_do_superior")

    async def outra_pagina(nt):         # 21/10 12h real: Dia de Cupons × página geral
        await nt.chega(FADA, 17927, C.F_17927)
        await nt.chega(PROMOTOM, 111724, C.P_111724)
    _dois_posts(r, "07.outra_pagina", outra_pagina, (FADA, 17927), (PROMOTOM, 111724))


def test_08_beneficios_conflitantes(r):
    async def pela_campanha(nt):        # post só página (com benefício) × cupb com outro
        await nt.chega(FADA, 900010, F_ESQUENTA_150)
        await nt.chega(PROMOTOM, 900011, P_20_60)
    _dois_posts(r, "08.pela_campanha", pela_campanha, (FADA, 900010), (PROMOTOM, 900011),
                "nome_ou_beneficio_em_conflito")

    async def pela_regra_de_cupom(nt):  # os dois cupb: a regra de cupom decide antes
        await nt.chega(FADA, 18049, F_18049_V1)
        await nt.edita(FADA, 18049, F_18049)
        await nt.chega(PROMOTOM, 900012, P_20_60)
    _dois_posts(r, "08.pela_regra_de_cupom", pela_regra_de_cupom, (FADA, 18049),
                (PROMOTOM, 900012), "-")


# ══════════════════════════════════════════════════════════════════
# 09 — múltiplos candidatos
# ══════════════════════════════════════════════════════════════════
def test_09_multiplos_candidatos(r):
    async def dois_validos(nt):
        await nt.chega(FADA, 18049, F_18049)              # código A, R$150/999
        await nt.chega(SAMUEL, 900020, S_OUTRO_CODIGO)    # código B: post próprio
        await nt.chega(PROMOTOM, 112034, P_112034)        # genérico: cabe nos dois
    nt, cap = noite(dois_validos, "ATIVO")
    a, b, c = (P(nt, FADA, 18049), P(nt, SAMUEL, 900020), P(nt, PROMOTOM, 112034))
    lp = linhas_de(cap, 112034)
    r.check(len({a, b, c}) == 3, "09.ambiguo_fica_como_hoje (post novo)", f"{a} {b} {c}")
    r.check(lp and lp[0]["acao"] == "INCERTO" and lp[0]["motivo"] == "ambiguo"
            and sorted(lp[0]["outros"]) == sorted([a, b]), "09.linha_ambiguo",
            "\n".join(cap.linhas))
    sombra_nt, _c = noite(dois_validos, "SOMBRA")
    r.check(resumo(sombra_nt) == resumo(nt), "09.ambiguo_igual_a_sombra",
            f"{resumo(sombra_nt)} × {resumo(nt)}")

    async def um_valido(nt):
        await nt.chega(FADA, 18049, F_18049)              # R$150/999 (nome esquenta)
        await nt.chega(SAMUEL, 900021, S_CODIGO_20)       # R$20/60: conflito com o candidato
        await nt.chega(PROMOTOM, 900022, P_150)           # R$150/999 sem código
    nt, cap = noite(um_valido, "ATIVO")
    a, b, c = (P(nt, FADA, 18049), P(nt, SAMUEL, 900021), P(nt, PROMOTOM, 900022))
    lp = linhas_de(cap, 900022)
    r.check(a != b and c == a, "09.um_valido_junta_no_valido", f"{a} {b} {c}")
    r.check(lp and (lp[0]["acao"], lp[0]["post"], lp[0]["aplicada"]) == ("JUNTA", a, "sim")
            and sorted(lp[0]["outros"]) == sorted([a, b]), "09.linha_um_valido",
            "\n".join(cap.linhas))
    r.check("3SQU3NT411" in nt.cli.no_ar[PRINCIPAL][a], "09.codigo_continua_no_ar")


# ══════════════════════════════════════════════════════════════════
# 10 — falha de leitura do banco
# ══════════════════════════════════════════════════════════════════
def test_10_falha_de_leitura_nunca_junta(r):
    ref, _c = noite(_caso_0300, "SOMBRA")
    for etapa, ausente in (("vivos", False), ("post", False), ("post", True),
                           ("exibida", False), ("ofertas", False), ("origens", False),
                           ("espelho_post", False)):
        async def corpo(nt, etapa=etapa, ausente=ausente):
            await nt.chega(PROMOTOM, 112024, P_112024)
            with C._falha_na_leitura(etapa, ausente=ausente):
                await nt.chega(FADA, 18042, F_18042)
        nt, cap = noite(corpo, "ATIVO")
        nome = f"10.{etapa}{'.ausente' if ausente else ''}"
        lf = linhas_de(cap, 18042)
        r.check(P(nt, FADA, 18042) not in (None, P(nt, PROMOTOM, 112024)),
                f"{nome}.nenhuma_juncao", f"{P(nt, FADA, 18042)} {P(nt, PROMOTOM, 112024)}")
        r.check(lf and lf[0]["acao"] == "INDETERMINADO" and lf[0]["aplicada"] == "-"
                and lf[0]["motivo"].startswith(f"erro_leitura:{etapa}:"),
                f"{nome}.indeterminado", "\n".join(cap.linhas))
        r.check(len(nt.cli.principal()) == 2, f"{nome}.publicacao_segue",
                str(resumo(nt)))
    # erro inesperado dentro do adaptador: comportamento de hoje e uma linha ❌
    real = sombra._vivos

    def quebra(*a, **k):
        raise RuntimeError("bug")

    async def corpo_bug(nt):
        await nt.chega(PROMOTOM, 112024, P_112024)
        sombra._vivos = quebra
        try:
            await nt.chega(FADA, 18042, F_18042)
        finally:
            sombra._vivos = real
    nt, cap = noite(corpo_bug, "ATIVO")
    r.check(P(nt, FADA, 18042) != P(nt, PROMOTOM, 112024)
            and cap.erros == ["❌ [RESGATE_SOMBRA] RuntimeError"], "10.erro_vira_hoje",
            str(cap.erros))
    del ref


    # o registro que levanta não muda a junção (a decisão não depende da linha)
    class _Bomba(C.logging.Handler):
        def emit(self, rec):
            if "RESGATE_SOMBRA" in rec.getMessage():
                raise RuntimeError("log quebrado")
    b = _Bomba()
    C.log_out.addHandler(b)
    try:
        nt, _cap = noite(_caso_0300, "ATIVO")
    finally:
        C.log_out.removeHandler(b)
    r.check(P(nt, FADA, 18042) == P(nt, PROMOTOM, 112024) and resumo(nt) == (2, 1),
            "10.log_que_levanta_nao_muda_a_juncao", str(resumo(nt)))


# ══════════════════════════════════════════════════════════════════
# 11 — publicações apagadas à mão
# ══════════════════════════════════════════════════════════════════
def test_11_publicacoes_apagadas_a_mao(r):
    out = {}

    # a) cópia apagada e já descoberta (AUSENTE) → o mais rico evolui só no principal
    async def ausente(nt):
        await nt.chega(PROMOTOM, 112034, P_112034)
        pa = nt.post(PROMOTOM, 112034)
        copia = db_espelho_get(pa)
        nt.cli.apagar_a_mao(CANAL_CUPONS, copia)
        await nt.edita(PROMOTOM, 112034, C.P_112024_EDITADO)       # o worker descobre
        out["a_meio"] = db_espelho_get(pa)
        n = len(nt.cli.chamadas)
        await nt.chega(FADA, 18049, F_18049)
        out["a"] = (pa, copia, nt.cli.chamadas[n:], db_espelho_get(pa))
    nt, _cap = noite(ausente, "ATIVO")
    pa, copia, depois, vinc = out["a"]
    r.check(out["a_meio"] == 0, "11a.descoberta_marca_ausente", str(out["a_meio"]))
    r.check(P(nt, FADA, 18049) == pa and "3SQU3NT411" in nt.cli.no_ar[PRINCIPAL][pa],
            "11a.junta_e_evolui_no_principal")
    r.check(not [c for c in depois if c[1] == CANAL_CUPONS] and vinc == 0
            and nt.cli.cupons() == [], "11a.copia_nunca_volta", str(depois))

    # b) cópia apagada, o worker ainda não sabe → a evolução descobre, nunca recria
    async def nao_descoberta(nt):
        await nt.chega(PROMOTOM, 112034, P_112034)
        pa = nt.post(PROMOTOM, 112034)
        copia = db_espelho_get(pa)
        nt.cli.apagar_a_mao(CANAL_CUPONS, copia)
        n = len(nt.cli.chamadas)
        await nt.chega(FADA, 18049, F_18049)
        out["b"] = (pa, copia, nt.cli.chamadas[n:], db_espelho_get(pa))
    nt, _cap = noite(nao_descoberta, "ATIVO")
    pa, copia, depois, vinc = out["b"]
    r.check(P(nt, FADA, 18049) == pa and "3SQU3NT411" in nt.cli.no_ar[PRINCIPAL][pa],
            "11b.junta_e_evolui")
    r.check(not [c for c in depois if c[1] == CANAL_CUPONS and c[0] == "send"]
            and vinc == 0 and nt.cli.cupons() == [], "11b.descobre_e_nunca_recria",
            f"{depois} vinculo={vinc}")

    # c) principal apagado pelo dono (já processado) → nunca é escolhido, não renasce
    async def principal_apagado(nt):
        await nt.chega(PROMOTOM, 112024, P_112024)
        pa = nt.post(PROMOTOM, 112024)
        nt.cli.apagar_a_mao(PRINCIPAL, pa)
        feitos = await post_apagado.apagados([pa], "TELEGRAM")
        await _drenar()
        n = len(nt.cli.chamadas)
        await nt.chega(FADA, 18042, F_18042)
        out["c"] = (pa, feitos, nt.cli.chamadas[n:])
    nt, cap = noite(principal_apagado, "ATIVO")
    pa, feitos, depois = out["c"]
    pf = P(nt, FADA, 18042)
    r.check(feitos and pf not in (None, pa), "11c.apagado_nunca_e_escolhido", f"{pa} {pf}")
    r.check(not [c for c in depois if c[2] == pa], "11c.apagado_nao_renasce", str(depois))
    r.check((nt.estado.get(pa) or {}).get("delete_status") == "ok", "11c.continua_apagado")
    r.check(not [d for d in linhas_de(cap, 18042) if d["post"] == pa or pa in d["outros"]],
            "11c.apagado_nem_aparece", "\n".join(cap.linhas))

    # d) principal apagado à mão, o worker ainda não sabe → a junção não cria 2º post
    class SemProva(Canais):
        async def delete_messages(self, dest, ids, *a, **k):
            lista = ids if isinstance(ids, (list, tuple)) else [ids]
            some = [i for i in lista if i in self.no_ar.get(str(dest), {})]
            for i in lista:
                self.chamadas.append(("delete", str(dest), i, None))
                self.no_ar.get(str(dest), {}).pop(i, None)
            return afetadas(lista, apagadas=len(some))     # como o Telegram: 0 se já sumiu

    async def principal_nao_descoberto(nt):
        sem = SemProva()
        sem.seq, sem.chamadas, sem.no_ar = nt.cli.seq, nt.cli.chamadas, nt.cli.no_ar
        import client as modulo_client
        modulo_client.client = sem
        await nt.chega(PROMOTOM, 112034, P_112034)
        pa = nt.post(PROMOTOM, 112034)
        nt.cli.apagar_a_mao(PRINCIPAL, pa)
        n = len(nt.cli.chamadas)
        await nt.chega(FADA, 18049, F_18049)
        await asyncio.sleep(0.2)
        await _drenar()
        out["d"] = (pa, nt.cli.chamadas[n:])
    nt, cap = noite(principal_nao_descoberto, "ATIVO")
    pa, depois = out["d"]
    r.check(P(nt, FADA, 18049) == pa, "11d.junta_no_post_que_o_worker_conhece")
    r.check(not [c for c in depois if c[0] == "send" and c[1] == PRINCIPAL],
            "11d.nenhum_post_novo_no_principal", str([c[:3] for c in depois]))


# ══════════════════════════════════════════════════════════════════
# 12 — monotonicidade
# ══════════════════════════════════════════════════════════════════
def test_12_monotonicidade(r):
    out = {}

    async def corpo(nt):
        await nt.chega(PROMOTOM, 112024, P_112024)
        pa = nt.post(PROMOTOM, 112024)
        # o Samuel 123470 (mesma página), normalizado mas ainda não publicado
        n = await normalizacao.normalizar(_bruta(SAMUEL, 123470, S_123470))
        ofs = enriquecer(n).ofertas
        with _modo("ATIVO"):
            # com família escolhida (qualquer uma): nunca troca
            out["familia"] = sombra.familia(n, ofs, 777, False, score=3)
            # a regra de cupom já decidiu (JUNTA ou NOVA): nunca contraria
            out["regra_de_cupom"] = sombra.familia(n, ofs, None, True, score=3)
            # post novo e nada decidido: junta
            out["novo"] = sombra.familia(n, ofs, None, False, score=3)
        with _modo("SOMBRA"):
            out["sombra"] = sombra.familia(n, ofs, None, False, score=3)
        # a guarda é de defesa: mesmo que a regra pura um dia devolvesse
        # JUNTA com família, a família de hoje fica
        real = regra.avaliar

        def junta_sempre(c, vivos, agora, sup, com_codigo=False):
            d = real(c, vivos, agora, sup, com_codigo)
            return regra.Decisao(regra.JUNTA, 999, "forcado", 0.0, d.destino_hoje,
                                 d.destino_elegivel)
        regra.avaliar = junta_sempre
        try:
            with _modo("ATIVO"):
                out["forcado"] = sombra.familia(n, ofs, 777, False, score=3)
        finally:
            regra.avaliar = real
        out["pa"] = pa
    nt, cap = noite(corpo, "ATIVO")
    pa = out["pa"]
    r.check(out["familia"] == (777, False), "12.familia_de_hoje_nunca_trocada", str(out))
    r.check(out["regra_de_cupom"] == (None, False), "12.regra_de_cupom_nunca_contrariada",
            str(out))
    r.check(out["novo"] == (pa, True), "12.post_novo_vira_juncao", str(out))
    r.check(out["sombra"] == (None, False), "12.sombra_nunca_muda", str(out))
    r.check(out["forcado"] == (777, False), "12.guarda_de_defesa_da_familia", str(out))
    ap = [_ler(x)["aplicada"] for x in cap.linhas]
    r.check("nao:regra_de_cupom" in ap and "nao:familia_de_hoje" in ap,
            "12.linha_diz_por_que", str(ap))
    # diferencial nas noites reais: ATIVO só troca "post novo" por um post vivo da campanha
    for nome, corpo_ in (("0300", _caso_0300), ("1159", _caso_1159)):
        a, _ca = noite(corpo_, "SOMBRA")
        d, _cd = noite(corpo_, "ATIVO")
        criadores_a = {}
        for (chat, mid), _n in a.norms.items():
            criadores_a.setdefault(P(a, chat, mid), (chat, mid))
        mudou = []
        for (chat, mid) in a.norms:
            pa_, pd_ = P(a, chat, mid), P(d, chat, mid)
            if criadores_a.get(pa_) == (chat, mid):
                continue                  # abriu post nos dois mundos (ou juntou só no ATIVO)
            mudou.append((mid, pa_, pd_))
        # quem já tinha família no SOMBRA segue com o mesmo "criador" no ATIVO
        ok = all(P(d, *criadores_a[pa_]) == pd_ for (mid, pa_, pd_) in mudou)
        r.check(ok, f"12.{nome}.familias_de_hoje_preservadas", str(mudou))


# ══════════════════════════════════════════════════════════════════
# 13 — corrida
# ══════════════════════════════════════════════════════════════════
class Lento(Canais):
    """Telegram com latência: o envio cede o loop (como a rede)."""
    async def send_message(self, dest, texto, *a, **k):
        await asyncio.sleep(0.05)
        return self._novo(dest, texto)


def test_13_corrida_com_e_sem_trava(r):
    async def corpo(nt):
        lento = Lento()
        lento.seq, lento.chamadas, lento.no_ar = nt.cli.seq, nt.cli.chamadas, nt.cli.no_ar
        import client as modulo_client
        modulo_client.client = lento
        envios = []
        for chat, mid, texto in ((PROMOTOM, 112024, P_112024), (FADA, 18042, F_18042)):
            n = await normalizacao.normalizar(_bruta(chat, mid, texto))
            nt.norms[(chat, mid)] = n
            enr = enriquecer(n)
            if await deve_enviar_async(enr):
                envios.append(publicacao.enviar(await montar(n), n, enr=enr, is_edit=False))
        await asyncio.gather(*envios)
        await _drenar()
    com, _c = noite(corpo, "ATIVO")
    r.check(P(com, PROMOTOM, 112024) == P(com, FADA, 18042) and resumo(com) == (1, 1),
            "13.com_trava_1_post", str(resumo(com)))
    real = sombra.mutexes
    sombra.mutexes = lambda *a, **k: []
    try:
        sem, _c2 = noite(corpo, "ATIVO")
    finally:
        sombra.mutexes = real
    r.check(P(sem, PROMOTOM, 112024) != P(sem, FADA, 18042),
            "13.sem_trava_2_posts (a trava é necessária)", str(resumo(sem)))


# ══════════════════════════════════════════════════════════════════
# 14 — SOMBRA não muda nada
# ══════════════════════════════════════════════════════════════════
def test_14_sombra_nao_muda_nada(r):
    def rodada(desligada):
        reais = sombra.familia, sombra.mutexes
        if desligada:
            sombra.familia = lambda norm, ofertas, fam, via=False, agora=None, **k: (fam, False)
            sombra.mutexes = lambda *a, **k: []
        try:
            res = []
            for corpo in (_caso_0300, _caso_1159):
                nt, cap = noite(corpo, "SOMBRA")
                res.append((nt.cli.chamadas, _banco(), list(T.MOTIVOS)))
            return res
        finally:
            sombra.familia, sombra.mutexes = reais
    base, liga = rodada(True), rodada(False)
    for i, nome in enumerate(("0300", "1159")):
        r.check(liga[i][0] == base[i][0], f"14.{nome}.mesmas_chamadas_ao_telegram",
                f"{len(liga[i][0])} × {len(base[i][0])}")
        r.check(liga[i][1] == base[i][1], f"14.{nome}.mesmo_banco")
        r.check(liga[i][2] == base[i][2], f"14.{nome}.mesmas_decisoes",
                f"{liga[i][2]} × {base[i][2]}")
    with _modo("SOMBRA"):
        chaves = [f"shopee|url|{PAGINA}"]
        r.check(espelho_cupons._de_cupons(chaves) == eh_identidade_cupom(chaves) is False,
                "14.roteamento_de_sempre")
    with _modo("ATIVO"):
        r.check(espelho_cupons._de_cupons(chaves) is True, "14.ativo_roteia_a_pagina")


# ══════════════════════════════════════════════════════════════════
# 16 — a régua da guarda de código é a da decisão de evolução
# ══════════════════════════════════════════════════════════════════
def test_16_regua_da_decisao(r):
    out = {}

    async def corpo(nt):
        await nt.chega(FADA, 18049, F_18049)                 # post com código, score 9
        pf = nt.post(FADA, 18049)
        n = await normalizacao.normalizar(_bruta(PROMOTOM, 112034, P_112034))
        est = db_get_post(pf)
        out["score_post"] = est.get("score")

        def regua(sc):
            v = [p for p in sombra._vivos("shopee", sombra._COM_CODIGO, n, sc) if p.msg_id == pf]
            return (v[0].score_maior, v[0].orcamento) if v else None
        out["igual"], out["maior"], out["menor"] = regua(9), regua(10), regua(3)
        out["sem_score"] = regua(None)
        from database_conexao import _db
        with _db() as db:
            db.execute("UPDATE post_estado SET edit_count = ? WHERE msg_id_dest = ?",
                       (config._MAX_EDITS, pf))
        out["sem_orcamento"] = regua(10)
        with _db() as db:
            db.execute("UPDATE post_estado SET edit_count = ? WHERE msg_id_dest = ?",
                       (config._MAX_EDITS - 1, pf))
        out["ultimo_orcamento"] = regua(10)
    noite(corpo, "ATIVO")
    r.check(out["score_post"] == 9, "16.score_do_post", str(out))
    r.check(out["igual"] == (False, True), "16.empate_nao_vence (como decidir: score MAIOR)",
            str(out["igual"]))
    r.check(out["maior"] == (True, True) and out["menor"] == (False, True),
            "16.maior_e_menor", str(out))
    r.check(out["sem_score"] == (None, None), "16.sem_score_nada_afirmado", str(out["sem_score"]))
    r.check(out["sem_orcamento"] == (True, False) and out["ultimo_orcamento"] == (True, True),
            "16.orcamento_e_edit_count_menor_que_o_teto", str(out))


# ══════════════════════════════════════════════════════════════════
# 15 — edição nunca passa pela regra
# ══════════════════════════════════════════════════════════════════
def test_15_edicao_nao_passa(r):
    chamadas = []
    real = sombra.familia

    def espiao(norm, *a, **k):
        chamadas.append(norm.msg_id)
        return real(norm, *a, **k)

    async def corpo(nt):
        await nt.chega(FADA, 18049, F_18049_V1)
        await nt.chega(PROMOTOM, 112034, P_112034)
        nt.antes = list(chamadas)
        await nt.edita(FADA, 18049, F_18049)
        await nt.edita(PROMOTOM, 112034, C.P_112024_EDITADO)
    sombra.familia = espiao
    try:
        nt, _cap = noite(corpo, "ATIVO")
    finally:
        sombra.familia = real
    r.check(nt.antes == [18049, 112034] and chamadas == nt.antes, "15.so_mensagem_nova",
            str(chamadas))
    r.check(P(nt, FADA, 18049) == P(nt, PROMOTOM, 112034) and resumo(nt) == (1, 1),
            "15.edicoes_seguem_o_vinculo_de_origem", str(resumo(nt)))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CAMPANHA POR PÁGINA DE RESGATE · ATIVO (flag) · 28/10"))
