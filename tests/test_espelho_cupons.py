"""
ESPELHO DE CUPONS — o post de CUPOM também no @FullPromotionCupons.

Regra: o post do canal principal cuja composição EXIBIDA é de cupom
(cupom com ou sem código, nenhum produto) ganha uma cópia fiel no canal
de cupons e a mantém em dia (evolução, sincronização do líder, upgrade
de mídia, substituição, fusão). O canal principal nunca espera o de
cupons: os aplicadores só notificam (sem await) e um trabalhador único
processa em ordem.

CORPUS REAL (t.me/s, texto fiel): Fada 17525 (30/90) e 17529 (PROMOML,
com foto), Promotom 110370 (seco) e 110373 (12 códigos ML), Samuel
118773 (listas), cupom da live (Samuel 118656), AOC 32" da live
7199917, Amazon psp A1P4S4HG0ZKYIC, Shopee Figurinhas (Fada 17527).
Links curtos são bloqueados aqui: URLs longas de produto/lista sintéticas.

  01  classificação pura sobre o corpus (cupom × produto × campanha ×
      cashback × live × página Amazon)
  02  cupom publicado → principal + cópia fiel no canal de cupons
      (mesmo texto, MESMA mídia por referência, parse_mode=None)
  03  produto → nenhuma operação no canal de cupons
  04  desligado (sem CANAL_CUPONS) → zero operação, zero task
  05  VELOCIDADE: canal de cupons lento (1,5 s) não atrasa o principal
  06  evolução (seco → rico) → espelho editado com o texto novo
  07  upgrade de mídia (classe melhor) → espelho com a mídia nova
  08  substituição (apagar+reenviar) → o mesmo espelho segue o id novo
      e refaz-se com a mídia (Telegram não põe mídia em texto)
  09  fusão (remoção do post) → espelho apagado
  10  sincronização do líder → espelho editado
  11  deixou de ser cupom → espelho apagado; passou a ser → criado
  12  falha do canal de cupons não afeta o principal nem trava a fila
  13  FloodWait no canal de cupons: espera e tenta de novo
  14  ordem: publicar + evoluir com canal lento → cria, depois edita
  15  shutdown: drena a fila e cancela limpo; depois disso, no-op

    python tests/test_espelho_cupons.py
"""
import asyncio
import os
import sys
import time
import types
from dataclasses import replace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_cupom_sem_codigo import (                             # noqa: E402
    FADA, PROMOTOM, SAMUEL, _isolar_loop, _zerar_banco, msg)
from _harness_e5 import rodar                                   # noqa: E402
import client as modulo_client                                  # noqa: E402
from config import GRUPO_DESTINO                                # noqa: E402
from database import db_espelho_get                             # noqa: E402
from database_conexao import _db                                # noqa: E402
from pipeline import convergencia, espelho_cupons, publicacao   # noqa: E402
from pipeline.enriquecimento import enriquecer, enriquecer_edicao  # noqa: E402
from pipeline.montagem import montar                            # noqa: E402
from pipeline.resolucao_identidade import eh_identidade_cupom   # noqa: E402
from telethon.errors import FloodWaitError                      # noqa: E402

CANAL = "FullPromotionCupons"
convergencia._ESPERA_BASE_S = 0.0

# ── corpus real ───────────────────────────────────────────────────
SECO_110370 = ("Cupons Shopee\n\n-Resgate aqui:\n1 https://s.shopee.com.br/2B5snGi9NX\n"
               "2 https://s.shopee.com.br/qaVCprDoA")
RICO_17525 = ("🚨 Novos Cupons Shopee\n\n🎟 R$30 OFF em R$299\n🎟 R$90 OFF em R$899\n\n"
              "✅ Resgate aqui: \nhttps://s.shopee.com.br/AAGNE4pfS9")
ML_110373 = ("Cupom Mercado Livre\n\n8% OFF em R$ 150, Limite de R$ 300 OFF: PROMOAQU\n"
             "10% OFF em R$ 79, Limite de R$ 100 OFF: PROMOML\n\n"
             "-Resgate aqui: https://mercadolivre.com/sec/2U6U32Q")
ML_17529 = ("🔥 Cupom Mercado Livre\n\n🎟 10% OFF acima de R$79, limite R$100: PROMOML\n\n"
            "✅ Resgate aqui:\nhttps://meli.la/2XoB7wc\n\nMais cupons aqui 👇")
LISTA_118773 = ("🔥 Cupons Mercado Livre\n\n🎟 15% OFF acima de R$ 79, limite R$ 60: TEMPROMO\n"
                "👉 Lista: https://meli.la/2SxES6P")
CUPOM_LIVE_118656 = ("🔥 Cupom R$ 10 OFF em R$ 50 Shopee\n\n🎟 Resgate o cupom na Live "
                     "shopee_br (sacola laranja) no APP aqui:\nhttps://s.shopee.com.br/20vruIAJxg")
AOC32 = ("🔥 Smart TV 32 Polegadas HD 32S5155/78G Roku TV AOC\n\n💵 R$ 699\n"
         "🎟 Resgate todos os cupons na Live (sacola laranja) no APP aqui:\n"
         "https://s.shopee.com.br/9AP2eWq9OA")
PSP = ("🔥 20% off em Brinquedos\n\nhttps://amzn.to/4jz1ssz\n\nanúncio")
FIGURINHAS_17527 = ("🚨 SHOPEE FIGURINHAS \n\n-Colecione figurinhas e ganhe cupons de até R$100 OFF\n\n"
                    "https://s.shopee.com.br/2LYirRO4lB")
CASHBACK = "🔥 Moedas Shopee: até 25% de cashback em moedas\nhttps://s.shopee.com.br/zzz"
PRODUTO_COM_CUPOM = ("🔥 Fone JBL Tune 520BT\n\n💵 R$ 199\n🎟 Use o cupom FONE10\n"
                     "https://s.shopee.com.br/abc")

LIVE_URL = "https://live.shopee.com.br/live/7199917"
PRODUTO_URL = "https://shopee.com.br/product/338371559/22593482107"
LISTA_URL = ("https://lista.mercadolivre.com.br/_Container_cupom-tempromo"
             "?coupon_campaign_id=13800001")
PSP_URL = "https://www.amazon.com.br/promotion/psp/A1P4S4HG0ZKYIC?tag=fullpromotion-20"
CAMPANHA_URL = "https://shopee.com.br/m/figurinhas?mmp_pid=an_1"


# ══════════════════════════════════════════════════════════════════
# Telegram falso, por canal — mídia rastreada por REFERÊNCIA
# ══════════════════════════════════════════════════════════════════
class _Midia:
    def __init__(self, tag):
        self.tag = tag


class _Msg:
    def __init__(self, canal, i, texto, media=None, entities=None):
        self.canal, self.id, self.message, self.media = canal, i, texto, media
        self.entities, self.photo = entities, None


def _tag(file):
    try:
        return bytes(file.getvalue()[:24]).split(b"|", 1)[0].decode()
    except Exception:                                   # noqa: BLE001
        return "?"


class Cliente:
    def __init__(self, latencia_cupons=0.0, falhas_cupons=()):
        self.msgs, self.ops, self._seq = {}, [], {}
        self.latencia_cupons = latencia_cupons
        self.falhas_cupons = list(falhas_cupons)    # exceções consumidas por op
        self.kwargs_cupons = []

    def _id(self, canal):
        self._seq[canal] = self._seq.get(canal, 0 if canal == CANAL else 70000) + 1
        return self._seq[canal]

    async def _canal_cupons(self, op, kw):
        self.kwargs_cupons.append((op, kw))
        await asyncio.sleep(self.latencia_cupons)
        if self.falhas_cupons:
            falha = self.falhas_cupons.pop(0)
            if falha is not None:
                raise falha

    def _registrar(self, canal, texto, media, entities=None):
        m = _Msg(canal, self._id(canal), texto, media, entities)
        self.msgs[(canal, m.id)] = m
        self.ops.append((time.monotonic(), canal, "novo", m.id))
        return m

    async def send_message(self, dest, texto, parse_mode="md", link_preview=None,
                           formatting_entities=None):
        if dest == CANAL:
            await self._canal_cupons("send_message", dict(parse_mode=parse_mode,
                                                          formatting_entities=formatting_entities))
        return self._registrar(dest, texto, None, formatting_entities)

    async def send_file(self, dest, img, caption=None, parse_mode="md",
                        force_document=False, formatting_entities=None):
        if dest == CANAL:
            await self._canal_cupons("send_file", dict(parse_mode=parse_mode,
                                                       formatting_entities=formatting_entities))
            media = img                                # referência: sem upload
        else:
            media = _Midia(_tag(img))
        return self._registrar(dest, caption or "", media, formatting_entities)

    async def edit_message(self, dest, msg_id, texto, parse_mode="md", file=None,
                           formatting_entities=None, link_preview=None):
        if dest == CANAL:
            await self._canal_cupons("edit_message", dict(parse_mode=parse_mode, file=file))
        m = self.msgs.get((dest, msg_id))
        if m is None:
            raise RuntimeError("MESSAGE_ID_INVALID")
        if file is not None and m.media is None:
            raise RuntimeError("MEDIA_PREV_INVALID")   # texto não ganha mídia
        m.message = texto
        if file is not None:
            m.media = file if dest == CANAL else _Midia(_tag(file))
        self.ops.append((time.monotonic(), dest, "edit", msg_id))
        return m

    async def delete_messages(self, dest, ids):
        if dest == CANAL:
            await self._canal_cupons("delete_messages", {})
        for i in (ids if isinstance(ids, (list, tuple)) else [ids]):
            self.msgs.pop((dest, i), None)
            self.ops.append((time.monotonic(), dest, "delete", i))
        return True

    async def get_messages(self, dest, ids=None):
        return self.msgs.get((dest, ids))

    async def download_media(self, media, file=None):
        if file is not None:
            file.write((getattr(media, "tag", "?") + "|").encode().ljust(4096, b"x"))
        return "arquivo"

    # leitura
    def no_canal(self, canal):
        return {i: m for (c, i), m in self.msgs.items() if c == canal}

    def ops_em(self, canal):
        return [(op, i) for _t, c, op, i in self.ops if c == canal]


def cenario(corpo, cli=None, ligado=True):
    async def _run():
        _isolar_loop()
        c = cli or Cliente()
        modulo_client.client = c
        antes = os.environ.get("CANAL_CUPONS")
        if ligado:
            os.environ["CANAL_CUPONS"] = CANAL
        else:
            os.environ.pop("CANAL_CUPONS", None)
        tarefa = espelho_cupons.iniciar()
        try:
            await corpo(c)
            await drenar()
        finally:
            await espelho_cupons.encerrar(tarefa)
            if antes is None:
                os.environ.pop("CANAL_CUPONS", None)
            else:
                os.environ["CANAL_CUPONS"] = antes
        return c
    _zerar_banco()
    return asyncio.run(_run())


async def drenar(limite=15.0):
    fila = espelho_cupons._fila
    if fila is not None:
        await asyncio.wait_for(fila.join(), limite)


def foto(n):
    tag = f"m{n.msg_id}"
    return replace(n, tem_midia=True, media_obj=types.SimpleNamespace(tag=tag),
                   midia_key=f"k-{tag}")


async def pub(n, score=None, is_edit=False):
    enr = enriquecer_edicao(n) if is_edit else enriquecer(n)
    if score is not None:
        enr = replace(enr, score=score)
    await publicacao.enviar(await montar(n), n, enr=enr, is_edit=is_edit)
    return enr


def principal_de(c, n):
    from pipeline import origem
    return origem.consultar(n.chat, n.msg_id)


# ══════════════════════════════════════════════════════════════════
def test_01_classificacao(r):
    casos = [
        ("cupb_assinatura_17525", msg(FADA, RICO_17525), True),
        ("cupb_geral_110370", msg(PROMOTOM, SECO_110370), True),
        ("codigos_ml_110373", msg(PROMOTOM, ML_110373, cupons=["PROMOAQU", "PROMOML"],
                                  plat="mercadolivre"), True),
        ("lista_ml_118773", msg(SAMUEL, LISTA_118773, urls_longas=[LISTA_URL],
                                cupons=["TEMPROMO"], plat="mercadolivre"), True),
        ("cupom_da_live_118656", msg(SAMUEL, CUPOM_LIVE_118656, urls_longas=[LIVE_URL]), True),
        ("produto_com_cupom", msg(FADA, PRODUTO_COM_CUPOM, urls_longas=[PRODUTO_URL],
                                  cupons=["FONE10"]), False),
        ("produto_da_live_aoc", msg(SAMUEL, AOC32, urls_longas=[PRODUTO_URL, LIVE_URL]), False),
        ("pagina_amazon_psp", msg(SAMUEL, PSP, urls_longas=[PSP_URL], plat="amazon"), False),
        ("campanha_figurinhas_17527", msg(FADA, FIGURINHAS_17527, urls_longas=[CAMPANHA_URL]), False),
        ("cashback_moedas", msg(FADA, CASHBACK), False),
    ]
    for rot, n, esperado in casos:
        ofertas = enriquecer(n).ofertas
        r.check(eh_identidade_cupom(ofertas) is esperado, f"01.{rot}", str(ofertas))
    r.check(eh_identidade_cupom([]) is False, "01.vazio_nao_e_cupom")
    r.check(eh_identidade_cupom(["shopee|cup|FONE10", "shopee|338371559.22593482107"]) is False
            and eh_identidade_cupom(["mercadolivre|dest|mercadolivre:lista:_Container_x"]) is False
            and eh_identidade_cupom(["shopee|cupb|geral", "shopee|url|" + LIVE_URL]) is True,
            "01.guarda_produto_destino_sem_codigo_e_live")


def test_02_cupom_publicado_vai_aos_dois_canais(r):
    out = {}

    async def corpo(c):
        n = foto(msg(FADA, RICO_17525))
        await pub(n, score=10)
        await drenar()
        out["principal"] = principal_de(c, n)
    c = cenario(corpo)
    pr = c.no_canal(GRUPO_DESTINO)[out["principal"]]
    cupons = c.no_canal(CANAL)
    r.check(len(cupons) == 1, "02.uma_copia", str(cupons))
    copia = next(iter(cupons.values()), None)
    r.check(copia is not None and copia.message == pr.message, "02.mesmo_texto")
    r.check(copia is not None and copia.media is pr.media, "02.mesma_midia_por_referencia")
    r.check(all(kw.get("parse_mode") is None for _op, kw in c.kwargs_cupons),
            "02.parse_mode_none", str(c.kwargs_cupons))
    t_pr = min(t for t, canal, _op, _i in c.ops if canal == GRUPO_DESTINO)
    t_esp = min(t for t, canal, _op, _i in c.ops if canal == CANAL)
    r.check(t_pr < t_esp, "02.principal_primeiro")


def test_03_produto_nao_vai_ao_canal_de_cupons(r):
    async def corpo(c):
        await pub(foto(msg(SAMUEL, AOC32, urls_longas=[PRODUTO_URL, LIVE_URL])), score=5)
        await pub(msg(SAMUEL, PSP, urls_longas=[PSP_URL], plat="amazon"), score=5)
    c = cenario(corpo)
    r.check(len(c.no_canal(GRUPO_DESTINO)) == 2 and not c.ops_em(CANAL),
            "03.zero_operacao_no_canal_de_cupons", str(c.ops_em(CANAL)))


def test_04_desligado_e_inerte(r):
    out = {}

    async def corpo(c):
        out["fila"] = espelho_cupons._fila
        await pub(foto(msg(FADA, RICO_17525)), score=10)
    c = cenario(corpo, ligado=False)
    r.check(out["fila"] is None, "04.sem_task_sem_fila")
    r.check(len(c.no_canal(GRUPO_DESTINO)) == 1 and not c.ops_em(CANAL), "04.zero_operacao")


def test_05_canal_de_cupons_lento_nao_atrasa_o_principal(r):
    medidas = {}
    for rot, ligado, lat in (("desligado", False, 0.0), ("ligado_lento", True, 1.5)):
        out = {}

        async def corpo(c, out=out):
            t0 = time.monotonic()
            await pub(foto(msg(FADA, RICO_17525)), score=10)
            out["publicar_s"] = time.monotonic() - t0
            out["t_principal"] = c.ops[0][0] - t0 if c.ops else None
        c = cenario(corpo, Cliente(latencia_cupons=lat), ligado=ligado)
        medidas[rot] = out["publicar_s"]
        if ligado:
            t_esp = [t for t, canal, op, _i in c.ops if canal == CANAL]
            t_pr = [t for t, canal, op, _i in c.ops if canal == GRUPO_DESTINO]
            r.check(t_esp and t_pr and t_pr[0] < t_esp[0], "05.principal_antes_do_espelho")
    r.check(medidas["ligado_lento"] < medidas["desligado"] + 0.05,
            "05.mesma_latencia_do_principal",
            f"desligado={medidas['desligado']:.4f}s ligado_lento={medidas['ligado_lento']:.4f}s")


def test_06_evolucao_edita_o_espelho(r):
    out = {}

    async def corpo(c):
        seco = msg(PROMOTOM, SECO_110370)
        await pub(seco, score=3)
        await drenar()
        await pub(msg(FADA, RICO_17525), score=10)          # rico EVOLUI o seco
        out["principal"] = principal_de(c, seco)
    c = cenario(corpo)
    pr = c.no_canal(GRUPO_DESTINO)[out["principal"]]
    cupons = list(c.no_canal(CANAL).values())
    r.check(len(cupons) == 1 and "R$30 OFF em R$299" in pr.message, "06.principal_evoluiu",
            pr.message[:60])
    r.check(cupons and cupons[0].message == pr.message, "06.espelho_acompanhou",
            cupons and cupons[0].message[:60])
    r.check(("edit", cupons[0].id) in c.ops_em(CANAL), "06.edicao_no_espelho")


def test_07_upgrade_de_midia_chega_ao_espelho(r):
    out = {}

    async def corpo(c):
        seco = foto(msg(PROMOTOM, SECO_110370))            # promotom: mídia ruim
        await pub(seco, score=3)
        await drenar()
        mesma = foto(msg(FADA, SECO_110370.replace("Cupons Shopee", "🔥 Cupons Shopee")))
        await pub(mesma, score=3)                          # IGUAL → upgrade de mídia
        out["principal"], out["fada"] = principal_de(c, seco), mesma.msg_id
    c = cenario(corpo)
    pr = c.no_canal(GRUPO_DESTINO)[out["principal"]]
    cupons = list(c.no_canal(CANAL).values())
    r.check(pr.media.tag == f"m{out['fada']}", "07.principal_trocou_a_imagem", pr.media.tag)
    r.check(cupons and cupons[0].media is pr.media, "07.espelho_com_a_imagem_nova",
            cupons and getattr(cupons[0].media, "tag", None))


def test_08_substituicao_reaponta_o_espelho(r):
    out = {}

    async def corpo(c):
        seco = msg(PROMOTOM, SECO_110370)                  # nasce SEM mídia
        await pub(seco, score=3)
        await drenar()
        out["antigo"] = principal_de(c, seco)
        out["esp_antigo"] = db_espelho_get(out["antigo"])
        rico = foto(msg(FADA, RICO_17525))
        await pub(rico, score=10)                          # evolui exigindo imagem
        await drenar()
        out["novo"] = principal_de(c, rico)                # o id do repost
        out["esp_novo"] = db_espelho_get(out["novo"])
        out["esp_do_antigo"] = db_espelho_get(out["antigo"])
    c = cenario(corpo)
    r.check(out["novo"] != out["antigo"] and out["antigo"] not in c.no_canal(GRUPO_DESTINO),
            "08.principal_substituido", str(out))
    r.check(out["esp_do_antigo"] is None and out["esp_novo"] is not None, "08.espelho_reapontado",
            str(out))
    pr = c.no_canal(GRUPO_DESTINO)[out["novo"]]
    esp = c.no_canal(CANAL).get(out["esp_novo"])
    r.check(len(c.no_canal(CANAL)) == 1 and esp is not None and esp.media is pr.media
            and esp.message == pr.message, "08.espelho_fiel_ao_novo",
            f"{len(c.no_canal(CANAL))} {esp and esp.message[:40]}")


def test_09_fusao_apaga_o_espelho(r):
    out = {}

    async def corpo(c):
        n = msg(FADA, RICO_17525)
        await pub(n, score=10)
        await drenar()
        mid = principal_de(c, n)
        out["antes"] = db_espelho_get(mid)
        await convergencia._remover(mid)
        await drenar()
        out["depois"] = db_espelho_get(mid)
    c = cenario(corpo)
    r.check(out["antes"] is not None and out["depois"] is None, "09.mapa_removido", str(out))
    r.check(not c.no_canal(CANAL) and not c.no_canal(GRUPO_DESTINO), "09.os_dois_canais_limpos")


def test_10_sincronizacao_do_lider(r):
    out = {}

    async def corpo(c):
        n = msg(PROMOTOM, ML_110373, cupons=["PROMOAQU", "PROMOML"], plat="mercadolivre")
        await pub(n, score=26)
        await drenar()
        editada = replace(n, texto_limpo=n.texto_limpo.replace("8% OFF", "9% OFF"),
                          texto_analise=n.texto_analise.replace("8% OFF", "9% OFF"))
        await pub(editada, score=26, is_edit=True)
        out["principal"] = principal_de(c, n)
    c = cenario(corpo)
    pr = c.no_canal(GRUPO_DESTINO)[out["principal"]]
    cupons = list(c.no_canal(CANAL).values())
    r.check("9% OFF" in pr.message, "10.lider_sincronizou", pr.message[:60])
    r.check(cupons and cupons[0].message == pr.message, "10.espelho_sincronizado")


def test_11_deixou_e_passou_a_ser_cupom(r):
    out = {}

    async def corpo(c):
        n = msg(FADA, RICO_17525)
        await pub(n, score=10)
        await drenar()
        mid = principal_de(c, n)
        with _db() as db:                                  # passa a exibir produto
            db.execute("INSERT INTO post_exibida(msg_id_dest,identity,ts) VALUES(?,?,?)",
                       (mid, "shopee|338371559.22593482107", time.time()))
        espelho_cupons.conteudo(mid)
        await drenar()
        out["saiu"] = db_espelho_get(mid)
        with _db() as db:                                  # volta a ser só cupom
            db.execute("DELETE FROM post_exibida WHERE identity LIKE 'shopee|338%'")
        espelho_cupons.conteudo(mid)
        await drenar()
        out["voltou"] = db_espelho_get(mid)
    c = cenario(corpo)
    r.check(out["saiu"] is None, "11.deixou_de_ser_cupom_apaga", str(out))
    r.check(out["voltou"] is not None and len(c.no_canal(CANAL)) == 1,
            "11.passou_a_ser_cupom_cria", str(out))


def test_12_falha_no_canal_de_cupons_isolada(r):
    cli = Cliente(falhas_cupons=[RuntimeError("CHAT_WRITE_FORBIDDEN"), None])

    async def corpo(c):
        await pub(msg(FADA, RICO_17525), score=10)
        await drenar()
        await pub(msg(PROMOTOM, ML_110373, cupons=["PROMOAQU", "PROMOML"],
                      plat="mercadolivre"), score=26)
    c = cenario(corpo, cli)
    r.check(len(c.no_canal(GRUPO_DESTINO)) == 2, "12.principal_intacto")
    r.check(len(c.no_canal(CANAL)) == 1, "12.fila_seguiu_e_espelhou_o_seguinte",
            str(c.ops_em(CANAL)))


def test_13_floodwait_no_canal_de_cupons(r):
    cli = Cliente(falhas_cupons=[FloodWaitError(1), None])
    out = {}

    async def corpo(c):
        t0 = time.monotonic()
        await pub(msg(FADA, RICO_17525), score=10)
        out["publicar_s"] = time.monotonic() - t0
        await drenar()
        out["espelho_s"] = time.monotonic() - t0
    c = cenario(corpo, cli)
    r.check(len(c.no_canal(CANAL)) == 1, "13.espelhou_depois_do_floodwait")
    r.check(out["publicar_s"] < 0.5 <= 1.0 <= out["espelho_s"], "13.esperou_sem_atrasar_o_principal",
            str(out))


def test_14_ordem_cria_depois_edita(r):
    cli = Cliente(latencia_cupons=0.3)
    out = {}

    async def corpo(c):
        seco = msg(PROMOTOM, SECO_110370)
        await pub(seco, score=3)
        await pub(msg(FADA, RICO_17525), score=10)          # antes de o espelho nascer
        out["principal"] = principal_de(c, seco)
    c = cenario(corpo, cli)
    pr = c.no_canal(GRUPO_DESTINO)[out["principal"]]
    ops = [op for op, _i in c.ops_em(CANAL)]
    cupons = list(c.no_canal(CANAL).values())
    r.check(ops[:2] == ["novo", "edit"], "14.cria_e_depois_edita", str(ops))
    r.check(cupons and cupons[0].message == pr.message, "14.estado_final_fiel")


def test_15_shutdown_drena_e_vira_no_op(r):
    out = {}

    async def _run():
        _isolar_loop()
        c = Cliente(latencia_cupons=0.2)
        modulo_client.client = c
        os.environ["CANAL_CUPONS"] = CANAL
        try:
            tarefa = espelho_cupons.iniciar()
            await pub(msg(FADA, RICO_17525), score=10)
            await espelho_cupons.encerrar(tarefa)            # sem drenar antes
            out["copias"] = len(c.no_canal(CANAL))
            out["task_ok"] = tarefa.done() and not tarefa.cancelled() or tarefa.cancelled()
            espelho_cupons.publicado(999, None)              # pós-shutdown: no-op
            out["fila"] = espelho_cupons._fila
        finally:
            os.environ.pop("CANAL_CUPONS", None)
    _zerar_banco()
    asyncio.run(_run())
    r.check(out["copias"] == 1, "15.drenou_antes_de_cancelar", str(out))
    r.check(out["task_ok"] and out["fila"] is None, "15.cancelou_limpo_e_no_op", str(out))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "ESPELHO DE CUPONS · canal de cupons"))
