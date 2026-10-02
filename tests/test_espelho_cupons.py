"""
CANAL DE CUPONS — o post de CUPOM sai no principal e no
@FullPromotionCupons AO MESMO TEMPO.

Regra: o post cuja composição é de cupom (cupom com ou sem código,
nenhum produto) é enviado aos dois canais no mesmo instante — a imagem
sobe uma vez, a mesma rotina de saída, nenhum espera o outro — e as
edições (evolução, líder, mídia) também saem juntas. Substituição,
fusão, falha e mudança de classificação são reconciliadas depois.

CORPUS REAL (t.me/s, texto fiel): Fada 17525 (30/90) e 17529 (PROMOML,
com foto), Promotom 110370 (seco) e 110373 (12 códigos ML), Samuel
118773 (listas), cupom da live (Samuel 118656), AOC 32" da live
7199917, Amazon psp A1P4S4HG0ZKYIC, Shopee Figurinhas (Fada 17527).
Links curtos são bloqueados aqui: URLs longas de produto/lista sintéticas.

  01  classificação pura sobre o corpus (cupom × produto × campanha ×
      cashback × live × página Amazon)
  02  cupom publicado → os dois envios começam juntos; imagem sobe uma
      vez; mesmo texto, mesma imagem, mesma formatação
  03  produto → nenhuma operação no canal de cupons
  04  desligado (sem CANAL_CUPONS) → zero operação, zero task
  05  VELOCIDADE: canal de cupons lento (1,5 s): saem juntos e o
      principal não atrasa
  06  evolução (seco → rico) → espelho editado com o texto novo
  07  upgrade de mídia (classe melhor) → espelho com a mídia nova
  08  substituição (apagar+reenviar) → o mesmo espelho segue o id novo
      e refaz-se com a mídia (Telegram não põe mídia em texto)
  09  fusão (remoção do post) → espelho apagado
  10  sincronização do líder → espelho editado
  11  deixou de ser cupom → espelho apagado; passou a ser → criado
  12  falha do canal de cupons: principal intacto; pontual → a rede de
      segurança publica depois; permanente → nada trava
  13  FloodWait no canal de cupons: espera e tenta de novo
  14  ordem: publicar + evoluir com canal lento → cria, depois edita
  15  shutdown: espera o que está em curso e cancela limpo o que travou;
      depois disso, no-op
  16  edição: as duas edições começam juntas; o principal não espera
  17  principal não saiu → a mensagem do canal de cupons é apagada
  18  CORRIDA: edição do canal de cupons falha rápido enquanto a do
      principal ainda está no ar → a conferência (depois do principal)
      deixa os dois iguais
  19  principal degradou para texto (falha da imagem) e o de cupons saiu
      com imagem → refeito igual ao principal
  20  fusão com edição lenta em curso → apagado no fim, sem ressurreição
  21  CANAL_CUPONS igual ao canal principal → desligado (sem duplicar)
  22  o canal de cupons não loga (só as operações do principal)
  23  PROPRIEDADE: latências aleatórias, falhas rápidas no simultâneo,
      só um dos canais aceitando a imagem, principal falhando a edição ou
      degradando para texto, substituição — sem drenar entre os passos →
      no fim o canal de cupons é IGUAL ao principal (texto e imagem), uma
      mensagem, vínculo certo
  24  upload único falhou → o principal recebe a imagem original (como
      sempre) e o de cupons uma CÓPIA (nunca o mesmo buffer ao mesmo tempo)

    python tests/test_espelho_cupons.py
"""
import asyncio
import logging
import os
import random
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

# O preparo da imagem que o Telethon faz no send_file (redimensionar/
# converter) — aqui identidade, mas REGISTRADO: o upload único tem de
# passar por ele, senão o post de cupom sairia diferente dos outros.
_PREPAROS = []


def _resize_falso(file, is_image):
    _PREPAROS.append(is_image)
    return file


_uploads = types.ModuleType("telethon.client.uploads")
_uploads._resize_photo_if_needed = _resize_falso
sys.modules.setdefault("telethon.client", types.ModuleType("telethon.client"))
sys.modules["telethon.client.uploads"] = _uploads
import telethon.utils as _tutils                                # noqa: E402
if not hasattr(_tutils, "is_image"):                 # mesma regra: extensão do nome
    _tutils.is_image = lambda f: str(getattr(f, "name", "")).lower().endswith(
        (".png", ".jpg", ".jpeg"))

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


class _Handle:
    """Resultado de upload_file: a imagem subiu UMA vez."""
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


def _como_midia(file, dest):
    if isinstance(file, _Handle):
        return _Midia(file.tag)
    if isinstance(file, _Midia):
        return file                                     # referência (cópia)
    return _Midia(_tag(file))


class Cliente:
    def __init__(self, latencia_cupons=0.0, falhas_cupons=(), latencia_principal=0.0,
                 principal_falha=False, falhas_principal=(), regra=None):
        self.msgs, self.ops, self._seq = {}, [], {}
        self.latencia_cupons, self.latencia_principal = latencia_cupons, latencia_principal
        self.falhas_cupons = list(falhas_cupons)    # exceções consumidas por op
        self.falhas_principal = list(falhas_principal)
        self.principal_falha = principal_falha
        self.regra = regra          # (dest, op, kw) -> (latência, exceção|None)
        self.kwargs = {GRUPO_DESTINO: [], CANAL: []}
        self.inicios = []                           # (t, canal, op) na ENTRADA de cada chamada
        self.uploads, self.upload_falha = 0, False

    def _id(self, canal):
        self._seq[canal] = self._seq.get(canal, 0 if canal == CANAL else 70000) + 1
        return self._seq[canal]

    async def _entrada(self, dest, op, kw):
        self.inicios.append((time.monotonic(), dest, op))
        self.kwargs[dest].append((op, kw))
        if self.regra is not None:
            latencia, falha = self.regra(dest, op, kw)
            await asyncio.sleep(latencia)
            if falha is not None:
                raise falha
            return
        if dest == CANAL:
            await asyncio.sleep(self.latencia_cupons)
            if self.falhas_cupons:
                falha = self.falhas_cupons.pop(0)
                if falha is not None:
                    raise falha
        else:
            await asyncio.sleep(self.latencia_principal)
            if self.principal_falha and op in ("send_message", "send_file"):
                raise RuntimeError("principal fora do ar")
            if self.falhas_principal:
                falha = self.falhas_principal.pop(0)
                if falha is not None:
                    raise falha

    def _registrar(self, canal, texto, media, entities=None):
        m = _Msg(canal, self._id(canal), texto, media, entities)
        self.msgs[(canal, m.id)] = m
        self.ops.append((time.monotonic(), canal, "novo", m.id))
        return m

    async def upload_file(self, file):
        self.uploads += 1
        if self.upload_falha:
            raise ConnectionError("upload caiu")
        return _Handle(_tag(file))

    async def send_message(self, dest, texto, parse_mode="md", link_preview=None,
                           formatting_entities=None):
        await self._entrada(dest, "send_message", dict(parse_mode=parse_mode))
        return self._registrar(dest, texto, None, formatting_entities)

    async def send_file(self, dest, img, caption=None, parse_mode="md",
                        force_document=False, formatting_entities=None):
        await self._entrada(dest, "send_file", dict(parse_mode=parse_mode,
                                                    legenda=caption is not None, arquivo=img))
        return self._registrar(dest, caption or "", _como_midia(img, dest), formatting_entities)

    async def edit_message(self, dest, msg_id, texto, parse_mode="md", file=None,
                           formatting_entities=None, link_preview=None):
        await self._entrada(dest, "edit_message", dict(parse_mode=parse_mode, file=file))
        m = self.msgs.get((dest, msg_id))
        if m is None:
            raise RuntimeError("MESSAGE_ID_INVALID")
        if file is not None and m.media is None:
            raise RuntimeError("MEDIA_PREV_INVALID")   # texto não ganha mídia
        m.message = texto
        if file is not None:
            m.media = _como_midia(file, dest)
        self.ops.append((time.monotonic(), dest, "edit", msg_id))
        return m

    async def delete_messages(self, dest, ids):
        await self._entrada(dest, "delete_messages", {})
        for i in (ids if isinstance(ids, (list, tuple)) else [ids]):
            self.msgs.pop((dest, i), None)
            self.ops.append((time.monotonic(), dest, "delete", i))
        return True

    async def get_messages(self, dest, ids=None):
        await asyncio.sleep(0)
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

    def inicio(self, canal, op):
        return [t for t, c, o in self.inicios if c == canal and o == op]


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
        espelho_cupons.iniciar()
        try:
            await corpo(c)
            await drenar()
        finally:
            await espelho_cupons.encerrar()
            if antes is None:
                os.environ.pop("CANAL_CUPONS", None)
            else:
                os.environ["CANAL_CUPONS"] = antes
        return c
    _zerar_banco()
    return asyncio.run(_run())


async def drenar(limite=15.0):
    """Até não haver operação do canal de cupons em curso."""
    fim = time.monotonic() + limite
    while espelho_cupons._tarefas:
        restante = fim - time.monotonic()
        assert restante > 0, "operações do canal de cupons não terminaram"
        await asyncio.wait(set(espelho_cupons._tarefas), timeout=restante)


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
    """Os dois canais recebem AO MESMO TEMPO: os envios começam juntos
    (nenhum espera o outro terminar), a imagem sobe UMA vez, mesmo texto,
    mesma imagem, mesma formatação (a mesma rotina de saída)."""
    out = {}

    async def corpo(c):
        n = foto(msg(FADA, RICO_17525))
        del _PREPAROS[:]
        await pub(n, score=10)
        await drenar()
        out["principal"] = principal_de(c, n)
        out["vinculo"] = db_espelho_get(out["principal"])
        out["preparos"] = list(_PREPAROS)
    c = cenario(corpo, Cliente(latencia_cupons=0.2, latencia_principal=0.2))
    pr = c.no_canal(GRUPO_DESTINO)[out["principal"]]
    cupons = list(c.no_canal(CANAL).values())
    r.check(len(cupons) == 1, "02.uma_mensagem_no_canal_de_cupons", str(cupons))
    copia = cupons[0] if cupons else None
    r.check(copia is not None and copia.message == pr.message, "02.mesmo_texto")
    r.check(copia is not None and copia.media.tag == pr.media.tag, "02.mesma_imagem")
    r.check(c.uploads == 1, "02.imagem_sobe_uma_vez", str(c.uploads))
    r.check(out["preparos"] == [True], "02.mesmo_preparo_de_imagem_do_send_file",
            str(out["preparos"]))
    t_pr, t_cp = c.inicio(GRUPO_DESTINO, "send_file"), c.inicio(CANAL, "send_file")
    r.check(t_pr and t_cp and abs(t_pr[0] - t_cp[0]) < 0.02, "02.comecam_juntos",
            f"Δ={abs(t_pr[0] - t_cp[0]) * 1000:.1f}ms" if t_pr and t_cp else str(c.inicios))
    r.check(c.kwargs[CANAL][0] == c.kwargs[GRUPO_DESTINO][0], "02.mesma_formatacao",
            f"{c.kwargs[CANAL][:1]} {c.kwargs[GRUPO_DESTINO][:1]}")
    r.check(copia is not None and out["vinculo"] == copia.id, "02.vinculo_gravado",
            f"{out['vinculo']} {copia and copia.id}")


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
        out["aberto"] = espelho_cupons._aberto
        await pub(foto(msg(FADA, RICO_17525)), score=10)
        out["tarefas"] = len(espelho_cupons._tarefas) + len(espelho_cupons._cadeia)
    c = cenario(corpo, ligado=False)
    r.check(out["aberto"] is False and out["tarefas"] == 0 and c.uploads == 0,
            "04.sem_task_sem_upload", str(out))
    r.check(len(c.no_canal(GRUPO_DESTINO)) == 1 and not c.ops_em(CANAL), "04.zero_operacao")


def test_05_canal_de_cupons_lento_nao_atrasa_o_principal(r):
    medidas = {}
    for rot, ligado, lat in (("desligado", False, 0.0), ("ligado_lento", True, 1.5)):
        out = {}

        async def corpo(c, out=out):
            t0 = time.monotonic()
            await pub(foto(msg(FADA, RICO_17525)), score=10)
            out["publicar_s"] = time.monotonic() - t0
        c = cenario(corpo, Cliente(latencia_cupons=lat), ligado=ligado)
        medidas[rot] = out["publicar_s"]
        if ligado:
            t_pr, t_cp = c.inicio(GRUPO_DESTINO, "send_file"), c.inicio(CANAL, "send_file")
            r.check(t_pr and t_cp and abs(t_pr[0] - t_cp[0]) < 0.02,
                    "05.saem_juntos_mesmo_com_o_de_cupons_lento")
            r.check(len(c.no_canal(CANAL)) == 1, "05.chegou_no_canal_de_cupons")
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
    c = cenario(corpo, Cliente(latencia_cupons=0.1, latencia_principal=0.1))
    pr = c.no_canal(GRUPO_DESTINO)[out["principal"]]
    cupons = list(c.no_canal(CANAL).values())
    r.check(pr.media.tag == f"m{out['fada']}", "07.principal_trocou_a_imagem", pr.media.tag)
    r.check(cupons and cupons[0].media.tag == pr.media.tag, "07.canal_de_cupons_com_a_imagem_nova",
            cupons and cupons[0].media.tag)
    t_pr, t_cp = c.inicio(GRUPO_DESTINO, "edit_message"), c.inicio(CANAL, "edit_message")
    r.check(t_pr and t_cp and abs(t_pr[-1] - t_cp[-1]) < 0.02, "07.trocam_juntos")


def test_08_substituicao_reaponta_o_espelho(r):
    out = {}

    async def corpo(c):
        seco = msg(PROMOTOM, SECO_110370)                  # nasce SEM mídia
        await pub(seco, score=3)
        await drenar()
        out["antigo"] = principal_de(c, seco)
        rico = foto(msg(FADA, RICO_17525))
        await pub(rico, score=10)                          # evolui exigindo imagem
        await drenar()
        out["novo"] = principal_de(c, rico)                # o id do repost
        out["esp_novo"] = db_espelho_get(out["novo"])
        out["esp_do_antigo"] = db_espelho_get(out["antigo"])
    c = cenario(corpo)
    r.check(out["novo"] != out["antigo"] and out["antigo"] not in c.no_canal(GRUPO_DESTINO),
            "08.principal_substituido", str(out))
    r.check(out["esp_do_antigo"] is None and out["esp_novo"] is not None, "08.vinculo_segue_o_id_novo",
            str(out))
    pr = c.no_canal(GRUPO_DESTINO)[out["novo"]]
    esp = c.no_canal(CANAL).get(out["esp_novo"])
    r.check(len(c.no_canal(CANAL)) == 1 and esp is not None and esp.media is not None
            and esp.media.tag == pr.media.tag and esp.message == pr.message,
            "08.canal_de_cupons_igual_ao_novo",
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
    # falha pontual: o principal segue; a rede de segurança publica depois
    async def corpo(c):
        await pub(msg(FADA, RICO_17525), score=10)
        await drenar()
        await pub(msg(PROMOTOM, ML_110373, cupons=["PROMOAQU", "PROMOML"],
                      plat="mercadolivre"), score=26)
    c = cenario(corpo, Cliente(falhas_cupons=[RuntimeError("CHAT_WRITE_FORBIDDEN")]))
    r.check(len(c.no_canal(GRUPO_DESTINO)) == 2, "12.principal_intacto")
    r.check(len(c.no_canal(CANAL)) == 2, "12.rede_de_seguranca_recuperou", str(c.ops_em(CANAL)))
    # falha permanente: principal intacto, nada trava
    c = cenario(corpo, Cliente(falhas_cupons=[RuntimeError("CHAT_WRITE_FORBIDDEN")] * 20))
    r.check(len(c.no_canal(GRUPO_DESTINO)) == 2 and not c.no_canal(CANAL),
            "12.falha_permanente_isolada")


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
            espelho_cupons.iniciar()
            await pub(msg(FADA, RICO_17525), score=10)
            await espelho_cupons.encerrar()                  # sem drenar antes
            out["copias"] = len(c.no_canal(CANAL))
            out["em_curso"] = len(espelho_cupons._tarefas)
            out["pos"] = (espelho_cupons.publicar_junto("x", None),     # no-op
                          espelho_cupons.vai_para_cupons(["shopee|cupb|geral"]))
            # operação travada: o shutdown não espera além de _DRENO_S
            espelho_cupons.iniciar()
            c.latencia_cupons = 60.0
            await pub(msg(PROMOTOM, ML_110373, cupons=["PROMOAQU", "PROMOML"],
                          plat="mercadolivre"), score=26)
            dreno, espelho_cupons._DRENO_S = espelho_cupons._DRENO_S, 0.2
            t0 = time.monotonic()
            try:
                await espelho_cupons.encerrar()
            finally:
                espelho_cupons._DRENO_S = dreno
            out["travada_s"] = time.monotonic() - t0
            out["travada_em_curso"] = len(espelho_cupons._tarefas)
        finally:
            os.environ.pop("CANAL_CUPONS", None)
    _zerar_banco()
    asyncio.run(_run())
    r.check(out["copias"] == 1 and out["em_curso"] == 0, "15.esperou_o_que_estava_em_curso",
            str(out))
    r.check(out["pos"] == (None, False), "15.depois_do_shutdown_no_op", str(out))
    r.check(out["travada_s"] < 1.0 and out["travada_em_curso"] == 0,
            "15.travada_cancelada_limpa", str(out))


def test_16_edicao_sai_junto(r):
    """Evolução com os dois canais lentos: as edições começam juntas e o
    principal não espera a do canal de cupons."""
    out = {}

    async def corpo(c):
        seco = msg(PROMOTOM, SECO_110370)
        await pub(seco, score=3)
        await drenar()
        c.latencia_cupons = 1.0
        t0 = time.monotonic()
        await pub(msg(FADA, RICO_17525), score=10)
        out["evoluir_s"] = time.monotonic() - t0
        out["principal"] = principal_de(c, seco)
    c = cenario(corpo, Cliente(latencia_principal=0.1))
    t_pr, t_cp = c.inicio(GRUPO_DESTINO, "edit_message"), c.inicio(CANAL, "edit_message")
    r.check(t_pr and t_cp and abs(t_pr[0] - t_cp[0]) < 0.02, "16.edicoes_comecam_juntas",
            f"{t_pr} {t_cp}")
    r.check(out["evoluir_s"] < 0.6, "16.principal_nao_espera", f"{out['evoluir_s']:.3f}s")
    pr = c.no_canal(GRUPO_DESTINO)[out["principal"]]
    r.check([m.message for m in c.no_canal(CANAL).values()] == [pr.message], "16.textos_iguais")


def test_17_principal_falhou_nao_deixa_cupom_sozinho(r):
    async def corpo(c):
        await pub(foto(msg(FADA, RICO_17525)), score=10)
    c = cenario(corpo, Cliente(principal_falha=True))
    r.check(not c.no_canal(GRUPO_DESTINO), "17.principal_nao_saiu")
    r.check(not c.no_canal(CANAL) and ("novo", 1) in c.ops_em(CANAL)
            and ("delete", 1) in c.ops_em(CANAL), "17.canal_de_cupons_limpo", str(c.ops_em(CANAL)))


def test_18_falha_rapida_durante_a_edicao_do_principal(r):
    """A edição do canal de cupons falha NA HORA (FloodWait longo) enquanto
    a do principal ainda está no ar: reconciliar já copiaria o texto velho.
    A conferência só acontece depois que o principal termina."""
    out = {}

    async def corpo(c):
        seco = msg(PROMOTOM, SECO_110370)
        await pub(seco, score=3)
        await drenar()
        c.latencia_principal = 0.3
        c.falhas_cupons = [FloodWaitError(200)]
        await pub(msg(FADA, RICO_17525), score=10)          # evolui
        out["principal"] = principal_de(c, seco)
    c = cenario(corpo)
    pr = c.no_canal(GRUPO_DESTINO)[out["principal"]]
    cupons = list(c.no_canal(CANAL).values())
    r.check("R$30 OFF em R$299" in pr.message, "18.principal_evoluiu")
    r.check(len(cupons) == 1 and cupons[0].message == pr.message, "18.iguais_no_fim",
            cupons and cupons[0].message[:50])


def test_19_principal_degradou_para_texto(r):
    """O principal falhou com a imagem e saiu só com texto (retry do
    aplicador); o canal de cupons tinha saído com imagem → refeito."""
    out = {}

    async def corpo(c):
        n = foto(msg(FADA, RICO_17525))
        await pub(n, score=10)
        out["principal"] = principal_de(c, n)
        await drenar()
        out["vinculo"] = db_espelho_get(out["principal"])
    falhas = [RuntimeError("IMAGE_PROCESS_FAILED")] * 3   # caption, sem caption, texto
    c = cenario(corpo, Cliente(falhas_principal=falhas))
    pr = c.no_canal(GRUPO_DESTINO)[out["principal"]]
    cupons = list(c.no_canal(CANAL).values())
    r.check(pr.media is None, "19.principal_so_texto")
    r.check(len(cupons) == 1 and cupons[0].media is None and cupons[0].message == pr.message
            and out["vinculo"] == cupons[0].id, "19.canal_de_cupons_igual",
            f"{[(m.id, m.media) for m in cupons]} {out}")
    r.check(("delete", 1) in c.ops_em(CANAL), "19.a_com_imagem_foi_apagada", str(c.ops_em(CANAL)))


def test_20_fusao_com_edicao_lenta_em_curso(r):
    out = {}

    async def corpo(c):
        seco = msg(PROMOTOM, SECO_110370)
        await pub(seco, score=3)
        await drenar()
        mid = principal_de(c, seco)
        c.latencia_cupons = 0.4
        await pub(msg(FADA, RICO_17525), score=10)          # edição lenta lá
        await convergencia._remover(mid)                   # fusão no meio dela
        await drenar()
        out["vinculo"] = db_espelho_get(mid)
    c = cenario(corpo)
    r.check(not c.no_canal(GRUPO_DESTINO) and not c.no_canal(CANAL)
            and out["vinculo"] is None, "20.os_dois_limpos_no_fim",
            f"{c.ops_em(CANAL)} {out}")


def test_21_canal_igual_ao_principal_desliga(r):
    out = {}
    antes = os.environ.get("CANAL_CUPONS")
    try:
        for valor in (GRUPO_DESTINO, "@" + GRUPO_DESTINO.upper()):
            os.environ["CANAL_CUPONS"] = valor
            espelho_cupons.iniciar()
            out[valor] = espelho_cupons._aberto
        os.environ["CANAL_CUPONS"] = CANAL
        espelho_cupons.iniciar()
        out[CANAL] = espelho_cupons._aberto
    finally:
        espelho_cupons._aberto = False
        if antes is None:
            os.environ.pop("CANAL_CUPONS", None)
        else:
            os.environ["CANAL_CUPONS"] = antes
    r.check(list(out.values()) == [False, False, True], "21.so_liga_com_outro_canal", str(out))


def test_22_canal_de_cupons_nao_loga(r):
    registros = []

    class _Coletor(logging.Handler):
        def emit(self, rec):
            registros.append(rec.getMessage())

    coletor = _Coletor()
    logger = logging.getLogger("ENVIO")
    logger.addHandler(coletor)
    try:
        async def corpo(c):
            seco = msg(PROMOTOM, SECO_110370)
            await pub(seco, score=3)
            await drenar()
            c.falhas_cupons = [RuntimeError("falha só lá")]
            await pub(msg(FADA, RICO_17525), score=10)      # evolui; lá falha
        c = cenario(corpo)
    finally:
        logger.removeHandler(coletor)
    editados = [m for m in registros if "Editado" in m]
    edicoes_principal = [op for op, _kw in c.kwargs[GRUPO_DESTINO] if op == "edit_message"]
    r.check(len(editados) == len(edicoes_principal) == 1, "22.so_o_principal_loga_edicao",
            str(editados))
    r.check(not any("falha só lá" in m for m in registros), "22.falha_de_la_nao_loga",
            str([m for m in registros if "falha" in m]))


def test_23_propriedade_iguais_no_fim(r):
    divergentes = []
    for semente in range(40):
        rng = random.Random(semente)
        queda = {CANAL: 0, GRUPO_DESTINO: 0}

        def regra(dest, op, kw, rng=rng, queda=queda):
            latencia = rng.uniform(0.0, 0.03)
            if kw.get("parse_mode") != "md":
                return latencia, None          # a reconciliação não falha
            # Canal fora do ar: a rotina de envio falha INTEIRA (legenda →
            # sem legenda → texto), como numa queda real.
            if queda[dest]:
                queda[dest] -= 1
                return latencia, RuntimeError("CHAT_WRITE_FORBIDDEN")
            sorte = rng.random()
            if op == "send_file" and kw.get("legenda"):
                if sorte < (0.35 if dest == CANAL else 0.15):
                    queda[dest] = 2            # o principal degrada para texto
                    return latencia, RuntimeError("CHAT_WRITE_FORBIDDEN")
            elif op == "send_message" and dest == CANAL and sorte < 0.35:
                return latencia, RuntimeError("CHAT_WRITE_FORBIDDEN")
            elif op == "edit_message":
                if kw.get("file") is not None and sorte < 0.3:
                    return latencia, RuntimeError("MEDIA_INVALID")   # só texto entra
                if sorte > (0.7 if dest == CANAL else 0.8):
                    return latencia, FloodWaitError(200)             # falha rápida
            return latencia, None

        out = {}

        async def corpo(c, rng=rng, out=out):
            com_foto = (lambda n: foto(n)) if rng.random() < 0.6 else (lambda n: n)
            seco = com_foto(msg(PROMOTOM, SECO_110370))
            await pub(seco, score=3)
            if rng.random() < 0.3:
                await drenar()
            rico = com_foto(msg(FADA, RICO_17525))
            await pub(rico, score=10)
            if rng.random() < 0.5:
                await asyncio.sleep(rng.uniform(0.0, 0.03))
            editada = replace(rico, texto_limpo=rico.texto_limpo.replace("R$90", "R$95"),
                              texto_analise=rico.texto_analise.replace("R$90", "R$95"))
            await pub(editada, score=10, is_edit=True)
            await drenar()
            pr = list(c.no_canal(GRUPO_DESTINO).values())
            out["vinculo"] = db_espelho_get(pr[0].id) if len(pr) == 1 else "?"
        c = cenario(corpo, Cliente(regra=regra))
        pr = list(c.no_canal(GRUPO_DESTINO).values())
        cp = list(c.no_canal(CANAL).values())
        ok = (len(pr) == 1 and len(cp) == 1 and cp[0].message == pr[0].message
              and getattr(cp[0].media, "tag", None) == getattr(pr[0].media, "tag", None)
              and out["vinculo"] == cp[0].id)
        if not ok:
            divergentes.append(
                f"semente={semente} principal={[(m.id, m.message[:24], getattr(m.media, 'tag', None)) for m in pr]} "
                f"cupons={[(m.id, m.message[:24], getattr(m.media, 'tag', None)) for m in cp]} {out}")
    r.check(not divergentes, "23.iguais_em_todas_as_sementes",
            f"{len(divergentes)} divergentes: " + " | ".join(divergentes[:3]))


def test_24_upload_falhou_buffers_separados(r):
    out = {}

    async def corpo(c):
        c.upload_falha = True
        n = foto(msg(FADA, RICO_17525))
        await pub(n, score=10)
        await drenar()
        out["principal"] = principal_de(c, n)
    c = cenario(corpo)
    arq_pr = [kw["arquivo"] for op, kw in c.kwargs[GRUPO_DESTINO] if op == "send_file"]
    arq_cp = [kw["arquivo"] for op, kw in c.kwargs[CANAL] if op == "send_file"]
    r.check(len(arq_pr) == 1 and len(arq_cp) == 1 and arq_pr[0] is not arq_cp[0]
            and arq_pr[0].getvalue() == arq_cp[0].getvalue()
            and getattr(arq_cp[0], "name", "") == getattr(arq_pr[0], "name", ""),
            "24.copia_identica_em_buffer_proprio", f"{arq_pr} {arq_cp}")
    pr = c.no_canal(GRUPO_DESTINO)[out["principal"]]
    cupons = list(c.no_canal(CANAL).values())
    r.check(len(cupons) == 1 and pr.media is not None and cupons[0].media is not None
            and cupons[0].media.tag == pr.media.tag and cupons[0].message == pr.message,
            "24.mesma_imagem_e_texto")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "ESPELHO DE CUPONS · canal de cupons"))
