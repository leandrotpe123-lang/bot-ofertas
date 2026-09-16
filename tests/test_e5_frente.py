"""
E5.0 — NIVEL 2: comportamento observavel contra a ARVORE REAL.

Exercita o trecho REAL que a E5.0 altera:

    montada = await montagem.montar(norm)          <- onde hoje se baixa
    await publicacao.enviar(montada, norm, enr=..., is_edit=...)

Nada do Foguetao e reimplementado. A unica fronteira fakeada e o
CLIENTE TELEGRAM (client.client), que e I/O externo — e e justamente
ele que conta os downloads e as escritas.

CLASSIFICACAO DE CADA TESTE:
  test_GUARDA_*  deve ficar verde ANTES e DEPOIS da E5.0 (nao-regressao)
  test_ALVO_*    VERMELHO hoje por desenho; verde quando a E5.0 existir

Roda stdlib-only / standalone; compatibilidade pytest nao validada
nesta fase:

    python tests/test_e5_frente.py
"""
import asyncio
import os
import sys
import time
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar, Resultado  # noqa: E402

preparar()

import config                                                  # noqa: E402
import globals as g                                            # noqa: E402
import client as modulo_client                                 # noqa: E402
from telethon.tl.types import MessageMediaPhoto                 # noqa: E402
from database import _init_db                                   # noqa: E402
from database_posts import db_registrar_post, db_get_post, db_origem_set  # noqa: E402
from pipeline import identidade                                 # noqa: E402
from pipeline.montagem import montar                            # noqa: E402
from pipeline.publicacao import enviar                          # noqa: E402
from pipeline.enriquecimento import derivar                     # noqa: E402
from pipeline.normalizacao import MensagemNormalizada           # noqa: E402
from pipeline.decisao import decidir                            # noqa: E402
from pipeline.score import V_CONTEUDO                           # noqa: E402
from pipeline.vida_oferta import VIDA_OFERTA_S                  # noqa: E402
from pipeline.montagem import MensagemMontada                   # noqa: E402

identidade._CHAT_USERNAME.update({
    -1001: "promotom", -1003: "fumotom",
    -1002: "fadapromos", -1004: "ofertasconvertidas",
})
RUIM_A = "-1001"
BOM_A = "-1002"
BOM_B = "-1004"

_init_db()


# ─────────────────────────────────────────────────────────────────
# Fronteira Telegram fakeada (I/O externo, nao logica do Foguetao)
# ─────────────────────────────────────────────────────────────────
class MsgFake:
    def __init__(self, ident: int, com_midia: bool):
        self.id = ident
        self.media = (MessageMediaPhoto(photo=types.SimpleNamespace(id=777))
                      if com_midia else None)


class ClienteFake:
    """Conta I/O. `download_media` e o contador central da frente."""

    def __init__(self, bytes_midia=4096, falha_download=False,
                 falha_edit_com_file=False, falha_edit_total=False,
                 falha_envio_primeiro=False):
        self.downloads = 0
        self.envios_file = 0
        self.envios_texto = 0
        self.edits = []            # (msg_id, tem_file)
        self.deletes = []
        self.bytes_midia = bytes_midia
        self.falha_download = falha_download
        self.falha_edit_com_file = falha_edit_com_file
        self.falha_edit_total = falha_edit_total
        self.falha_envio_primeiro = falha_envio_primeiro
        self._proximo_id = 5000

    async def download_media(self, media, file=None):
        self.downloads += 1
        if self.falha_download:
            return None
        if file is not None:
            file.write(b"x" * self.bytes_midia)
        return "arquivo"

    async def send_file(self, dest, img, caption=None, parse_mode=None,
                        force_document=False):
        if self.falha_envio_primeiro and self.envios_file == 0:
            self.envios_file += 1
            raise RuntimeError("falha simulada no primeiro envio")
        self.envios_file += 1
        self._proximo_id += 1
        return MsgFake(self._proximo_id, com_midia=True)

    async def send_message(self, dest, texto, parse_mode=None,
                           link_preview=None):
        self.envios_texto += 1
        self._proximo_id += 1
        return MsgFake(self._proximo_id, com_midia=False)

    async def edit_message(self, dest, msg_id, texto, parse_mode=None,
                           file=None):
        self.edits.append((msg_id, file is not None))
        if self.falha_edit_total:
            raise RuntimeError("edit falhou")
        if file is not None and self.falha_edit_com_file:
            raise RuntimeError("midia recusada")
        return MsgFake(msg_id, com_midia=file is not None)

    async def delete_messages(self, dest, msg_id):
        self.deletes.append(msg_id)
        return True


def instalar(cliente: ClienteFake) -> None:
    modulo_client.client = cliente


# ─────────────────────────────────────────────────────────────────
# Fixtures de dominio
# ─────────────────────────────────────────────────────────────────
_SEQ = {"n": 70000}


def novo_id() -> int:
    _SEQ["n"] += 1
    return _SEQ["n"]


def norm(chat=BOM_A, texto="Oferta boa R$ 99,90", tem_midia=True,
         midia_key="mk-1", oferta="shopee|P1", msg_id=None):
    return MensagemNormalizada(
        msg_id=msg_id or novo_id(), chat=chat, texto_limpo=texto,
        texto_analise=texto, mapa={"http://o": "http://a"}, preservar=[],
        plat="shopee", sku="P1", ids_globais=[oferta.split("|")[1]],
        idents=[("shopee", oferta.split("|")[1], "produto")],
        tem_midia=tem_midia, media_obj=object() if tem_midia else None,
        midia_key=midia_key)


async def rodar_caminho(n, is_edit=False):
    """O trecho REAL que a E5.0 altera: montagem + publicacao."""
    enr = derivar(n)
    montada = await montar(n)
    ok = await enviar(montada, n, enr=enr, is_edit=is_edit)
    return ok, montada, enr


def preparar_loop():
    g._init_globals()


def post_vivo(msg_id_dest, ofertas, score, texto, lider, midia_chat,
              edit_count=0, chat_origem="", msg_id_origem=0):
    agora = time.time()
    db_registrar_post(msg_id_dest, ofertas, score, texto, "shopee", lider,
                      agora + VIDA_OFERTA_S, edit_count,
                      chat_origem=chat_origem, msg_id_origem=msg_id_origem,
                      midia_chat=midia_chat, score_versao=V_CONTEUDO)


def cenario(corpo):
    """Roda um cenario num loop proprio, com globals reinicializados."""
    async def _run():
        preparar_loop()
        return await corpo()
    return asyncio.run(_run())


# ══════════════════════════════════════════════════════════════════
# GUARDAS — verdes antes E depois da E5.0
# ══════════════════════════════════════════════════════════════════
def test_GUARDA_publicacao_nova_com_midia(r: Resultado):
    """T6 — post novo com midia continua publicando e baixa 1x."""
    c = ClienteFake(); instalar(c)
    n = norm(oferta="shopee|NOVA1")

    async def corpo():
        return await rodar_caminho(n)

    ok, montada, _ = cenario(corpo)
    r.check(ok is True, "G1.ok")
    r.check(c.downloads == 1, "G1.um_download", f"downloads={c.downloads}")
    r.check(c.envios_file == 1, "G1.send_file", f"{c.envios_file}")
    r.check(montada.imagem is not None, "G1.bytes_no_aplicador")


def test_GUARDA_publicacao_nova_sem_midia(r: Resultado):
    """Sem midia candidata: nenhum download, envio por texto."""
    c = ClienteFake(); instalar(c)
    n = norm(tem_midia=False, midia_key="", oferta="shopee|NOVA2")

    async def corpo():
        return await rodar_caminho(n)

    ok, montada, _ = cenario(corpo)
    r.check(ok is True, "G2.ok")
    r.check(c.downloads == 0, "G2.zero_download", f"downloads={c.downloads}")
    r.check(c.envios_texto == 1, "G2.send_message", f"{c.envios_texto}")
    r.check(montada.imagem is None, "G2.sem_bytes")


def test_GUARDA_T16_renascer_materializa(r: Resultado):
    """T16 — RENASCER tem trocar_midia=False e AINDA ASSIM precisa
    dos bytes: e publicacao nova. Baixa exatamente 1x."""
    c = ClienteFake(); instalar(c)
    dest = novo_id()
    post_vivo(dest, ["shopee|REN"], 10, "texto antigo", BOM_A, BOM_A)
    n = norm(texto="a oferta voltou ao estoque R$ 50", oferta="shopee|REN")

    async def corpo():
        return await rodar_caminho(n, is_edit=False)

    ok, montada, enr = cenario(corpo)
    d = decidir(n, MensagemMontada(1, BOM_A, "shopee", "", "t",
                                   b"x" * 100, {}, 1),
                enr.score, db_get_post(dest), time.time(), False)
    r.check(d.acao == "RENASCER", "G3.acao_renascer", d.acao)
    r.check(d.trocar_midia is False, "G3.trocar_false",
            "RENASCER nao passa por _com_midia")
    r.check(ok is True, "G3.ok")
    r.check(c.downloads == 1, "G3.um_download", f"downloads={c.downloads}")
    r.check(c.envios_file == 1, "G3.publicou_com_imagem")


def test_GUARDA_T9_sincronizar_noop_sem_telegram(r: Resultado):
    """T9/E4.0 — texto igual + politica sem troca: nenhuma escrita."""
    c = ClienteFake(); instalar(c)
    dest = novo_id()
    n = norm(chat=BOM_A, oferta="shopee|SYNC1")
    texto_publicado = None

    async def preparo():
        nonlocal texto_publicado
        m = await montar(n)
        texto_publicado = m.texto

    asyncio.run(preparo())
    # post do proprio lider, ja com a MESMA midia aceita (guarda E4.0)
    post_vivo(dest, ["shopee|SYNC1"], 10, texto_publicado, BOM_A, BOM_A)

    async def corpo():
        g.midia_aceita_set(dest, "mk-1")
        return await rodar_caminho(n, is_edit=True)

    ok, _, _ = cenario(corpo)
    r.check(ok is True, "G4.ok")
    r.check(c.edits == [], "G4.nenhuma_edicao", f"edits={c.edits}")
    r.check(c.deletes == [], "G4.nenhum_delete")
    r.check(c.envios_file == 0 and c.envios_texto == 0, "G4.nenhum_envio")


def test_GUARDA_T12_midia_key_so_com_evidencia(r: Resultado):
    """T12 — edit com file falha e degrada para texto: a chave aceita
    NAO pode ser registrada."""
    c = ClienteFake(falha_edit_com_file=True); instalar(c)
    dest = novo_id()
    n = norm(chat=BOM_A, oferta="shopee|EV1")
    # post pobre e SEM midia -> politica autoriza TROCA/post_sem_midia
    post_vivo(dest, ["shopee|EV1"], 1, "texto antigo pobre", BOM_A, "")

    async def corpo():
        return await rodar_caminho(n, is_edit=False)

    ok, _, _ = cenario(corpo)
    r.check(ok is True, "G6.ok")
    r.check(any(f for _, f in c.edits), "G6.tentou_com_file", str(c.edits))
    r.check(g.midia_aceita_get(dest) == "", "G6.chave_nao_registrada",
            f"registrou {g.midia_aceita_get(dest)!r} sem evidencia")


def test_GUARDA_T5_falha_de_materializacao_nao_destroi_post(r: Resultado):
    """T4/T5 — download falha: preserva a midia publicada e NUNCA
    chega a delete+repost. Invariante critica da frente."""
    c = ClienteFake(falha_download=True, falha_edit_total=True)
    instalar(c)
    dest = novo_id()
    n = norm(chat=BOM_A, oferta="shopee|DEL1")
    post_vivo(dest, ["shopee|DEL1"], 1, "texto antigo pobre", BOM_A, RUIM_A)

    async def corpo():
        return await rodar_caminho(n, is_edit=False)

    ok, montada, _ = cenario(corpo)
    r.check(ok is True, "G7.ok")
    r.check(montada.imagem is None, "G7.sem_bytes")
    r.check(c.deletes == [], "G7.sem_delete",
            f"delete+repost sem imagem! deletes={c.deletes}")
    est = db_get_post(dest)
    r.check(est is not None, "G7.post_vivo")
    r.check(est["midia_chat"] == RUIM_A, "G7.midia_preservada",
            f"midia_chat={est['midia_chat']!r}")


def test_GUARDA_origem_ja_publicada_nao_envia(r: Resultado):
    """ORIGEM_JA_PUBLICADA: NEW absorvido antes de qualquer aplicador."""
    c = ClienteFake(); instalar(c)
    dest = novo_id()
    n = norm(chat=BOM_A, oferta="shopee|ORIG1")
    post_vivo(dest, ["shopee|ORIG1"], 10, "t", BOM_A, BOM_A)
    db_origem_set(n.chat, n.msg_id, dest)

    async def corpo():
        return await rodar_caminho(n, is_edit=False)

    ok, _, _ = cenario(corpo)
    r.check(ok is True, "G8.ok")
    r.check(c.envios_file == 0 and c.envios_texto == 0, "G8.sem_envio")
    r.check(c.edits == [], "G8.sem_edicao")
    r.check(c.deletes == [], "G8.sem_delete")


def test_GUARDA_retry_de_envio_nao_baixa_de_novo(r: Resultado):
    """O laco de retry de _aplicar_novo_envio reusa `img`; nunca
    materializa duas vezes."""
    c = ClienteFake(falha_envio_primeiro=True); instalar(c)
    n = norm(oferta="shopee|RETRY1")

    async def corpo():
        return await rodar_caminho(n)

    ok, _, _ = cenario(corpo)
    r.check(c.downloads == 1, "G9.um_download_apesar_do_retry",
            f"downloads={c.downloads}")


def test_GUARDA_T17d_fronteira_do_relogio(r: Resultado):
    """T17d — recapturar o relogio na fronteira de viva() muda a ACAO e
    produz post duplicado. Prova por que `agora` e congelado."""
    fim = 1_000_000.0
    est = dict(msg_id_dest=1, score=1, texto="antigo", lider=BOM_A,
               edit_count=0, janela_fim=fim, ts=fim - 60,
               score_versao=V_CONTEUDO, plat="shopee", midia_chat=BOM_A)
    n = norm(tem_midia=False, midia_key="", oferta="shopee|CLK")
    m = MensagemMontada(1, BOM_A, "shopee", "", "texto novo", None, {}, 1)

    d_vivo = decidir(n, m, 99, est, fim - 0.001, False)
    d_morto = decidir(n, m, 99, est, fim, False)

    r.check(d_vivo.na_janela is True, "G10.vivo")
    r.check(d_morto.na_janela is False, "G10.morto_na_fronteira")
    r.check(d_vivo.acao == "EVOLUIR", "G10.acao_viva", d_vivo.acao)
    r.check(d_morto.acao == "PUBLICAR", "G10.acao_morta", d_morto.acao)
    r.check(d_vivo.acao != d_morto.acao, "G10.divergencia",
            "1ms de recaptura vira post DUPLICADO — por isso agora e congelado")


# ══════════════════════════════════════════════════════════════════
# ALVOS — VERMELHOS hoje por desenho; verdes quando a E5.0 existir
# ══════════════════════════════════════════════════════════════════
def test_ALVO_T1_preserva_sem_download(r: Resultado):
    """T1 — politica PRESERVA/nao_rebaixa: nenhum byte deve ser baixado."""
    c = ClienteFake(); instalar(c)
    dest = novo_id()
    n = norm(chat=RUIM_A, oferta="shopee|PRES1")   # candidato de midia RUIM
    post_vivo(dest, ["shopee|PRES1"], 99, "texto antigo", BOM_A, BOM_A)

    async def corpo():
        return await rodar_caminho(n, is_edit=False)

    ok, _, _ = cenario(corpo)
    r.check(ok is True, "A1.ok")
    r.check(c.downloads == 0, "A1.zero_download",
            f"downloads={c.downloads} (E5.0 deve evitar este download)")
    r.check(c.edits == [], "A1.sem_edicao")


def test_ALVO_T2_midia_igual_sem_download(r: Resultado):
    """T2 — guarda E4.0 (chave_nova == chave_aceita): sem download."""
    c = ClienteFake(); instalar(c)
    dest = novo_id()
    n = norm(chat=BOM_A, midia_key="mk-igual", oferta="shopee|IGUAL1")
    post_vivo(dest, ["shopee|IGUAL1"], 99, "texto antigo", BOM_A, "")

    async def corpo():
        g.midia_aceita_set(dest, "mk-igual")
        return await rodar_caminho(n, is_edit=False)

    ok, _, _ = cenario(corpo)
    r.check(ok is True, "A2.ok")
    r.check(c.downloads == 0, "A2.zero_download",
            f"downloads={c.downloads} (mesma midia ja aceita)")


def test_ALVO_T9_sincronizar_sem_troca_sem_download(r: Resultado):
    """T9 — SINCRONIZAR no-op ja evita a escrita (E4.0); a E5.0 deve
    evitar tambem a LEITURA."""
    c = ClienteFake(); instalar(c)
    dest = novo_id()
    n = norm(chat=BOM_A, oferta="shopee|SYNC2")
    texto_publicado = None

    async def preparo():
        nonlocal texto_publicado
        texto_publicado = (await montar(n)).texto

    asyncio.run(preparo())
    post_vivo(dest, ["shopee|SYNC2"], 10, texto_publicado, BOM_A, BOM_A)
    c.downloads = 0     # zera o download do preparo

    async def corpo():
        g.midia_aceita_set(dest, "mk-1")
        return await rodar_caminho(n, is_edit=True)

    ok, _, _ = cenario(corpo)
    r.check(ok is True, "A3.ok")
    r.check(c.edits == [], "A3.no_op_mantido")
    r.check(c.downloads == 0, "A3.zero_download",
            f"downloads={c.downloads} (no-op nao pode baixar)")


def test_ALVO_T3_troca_autorizada_baixa_e_aplica(r: Resultado):
    """T3 — TROCA/post_sem_midia: baixa 1x e aplica com file."""
    c = ClienteFake(); instalar(c)
    dest = novo_id()
    n = norm(chat=BOM_A, oferta="shopee|TROCA1")
    post_vivo(dest, ["shopee|TROCA1"], 99, "texto antigo", BOM_A, "")

    async def corpo():
        return await rodar_caminho(n, is_edit=False)

    ok, _, _ = cenario(corpo)
    r.check(ok is True, "A4.ok")
    r.check(c.downloads == 1, "A4.um_download", f"downloads={c.downloads}")
    r.check(any(f for _, f in c.edits), "A4.aplicou_com_file", str(c.edits))
    r.check(g.midia_aceita_get(dest) == "mk-1", "A4.chave_registrada",
            f"{g.midia_aceita_get(dest)!r}")


def test_ALVO_T17a_decidir_aceita_o_fato(r: Resultado):
    """T17a — decidir() aceita midia_candidata e o fato substitui
    bool(montada.imagem) nos DOIS consumidores."""
    est = dict(msg_id_dest=1, score=1, texto="antigo", lider=BOM_A,
               edit_count=0, janela_fim=time.time() + 600, ts=time.time(),
               score_versao=V_CONTEUDO, plat="shopee", midia_chat="")
    n = norm(chat=BOM_A, oferta="shopee|FATO")
    m_sem = MensagemMontada(1, BOM_A, "shopee", "", "texto novo", None, {}, 1)
    agora = time.time()
    try:
        d = decidir(n, m_sem, 99, est, agora, False, midia_candidata=True)
    except TypeError as e:
        r.check(False, "A5.parametro_existe", f"decidir() nao aceita: {e}")
        return
    r.check(d.trocar_midia is True, "A5.politica_usou_o_fato",
            "imagem=None + fato=True deve dar TROCA/post_sem_midia")
    r.check(d.motivo_midia == "TROCA/post_sem_midia", "A5.motivo",
            d.motivo_midia)
    r.check(d.exigir_imagem is True, "A5.exigir_imagem_do_fato")
    d0 = decidir(n, m_sem, 99, est, agora, False, midia_candidata=False)
    r.check(d0.trocar_midia is False, "A5.fato_falso")
    r.check(d0.motivo_midia == "PRESERVA/sem_imagem_nova", "A5.motivo_falso",
            d0.motivo_midia)
    r.check(d0.acao == d.acao, "A5.acao_invariante",
            f"{d.acao} != {d0.acao}: o fato nao pode mudar a acao")


def _cenario_corrida():
    """Duas tasks convergindo no MESMO post, com materializacao lenta.
    Devolve (cliente, dest, eventos, resultados, n1, n2)."""
    eventos = []

    class ClienteLento(ClienteFake):
        async def download_media(self, media, file=None):
            tag = getattr(media, "tag", "?")
            eventos.append(f"download.{tag}.inicio")
            await asyncio.sleep(0.05)          # janela de corrida
            eventos.append(f"download.{tag}.fim")
            return await ClienteFake.download_media(self, media, file=file)

        async def edit_message(self, dest, msg_id, texto, parse_mode=None,
                               file=None):
            eventos.append(f"edit.{texto.split()[0][:8]}")
            return await ClienteFake.edit_message(
                self, dest, msg_id, texto, parse_mode=parse_mode, file=file)

    c = ClienteLento(); instalar(c)
    dest = novo_id()
    # post pobre e SEM midia: ambos os candidatos evoluem e a politica
    # autoriza TROCA/post_sem_midia no primeiro.
    post_vivo(dest, ["shopee|RACE"], 1, "antigo pobre", BOM_A, "")

    n1 = norm(chat=BOM_A, texto="T1primeira R$ 10 50% off",
              midia_key="mk-T1", oferta="shopee|RACE")
    n2 = norm(chat=BOM_B,
              texto="T2segunda R$ 20 50% off frete gratis acima de R$ 100",
              midia_key="mk-T2", oferta="shopee|RACE")
    n1.media_obj = types.SimpleNamespace(tag="T1")
    n2.media_obj = types.SimpleNamespace(tag="T2")

    async def corpo():
        return await asyncio.gather(rodar_caminho(n1, is_edit=False),
                                    rodar_caminho(n2, is_edit=False))

    res = cenario(corpo)
    return c, dest, eventos, res, n1, n2


def test_GUARDA_T18_corrida_estado_final_consistente(r: Resultado):
    """T18 — duas tasks no mesmo post: sem duplicacao, sem delete
    indevido, estado e midia finais coerentes, e a mutacao serializada
    pela cadeia de locks (edit_count nao sofre lost update)."""
    c, dest, eventos, res, n1, n2 = _cenario_corrida()

    r.check(len(res) == 2 and all(x[0] is True for x in res),
            "T18.ambas_concluem", str([x[0] for x in res]))

    # 1. NENHUMA DUPLICACAO: nenhum post novo foi publicado
    r.check(c.envios_file == 0 and c.envios_texto == 0, "T18.sem_post_novo",
            f"file={c.envios_file} texto={c.envios_texto}")

    # 2. NENHUM DELETE/REPOST INDEVIDO
    r.check(c.deletes == [], "T18.sem_delete", f"deletes={c.deletes}")

    # 3. MUTACAO SERIALIZADA: as duas evolucoes contaram; um lost update
    #    deixaria edit_count em 1.
    est = db_get_post(dest)
    r.check(est is not None, "T18.post_existe")
    if est is None:
        return
    r.check(est["msg_id_dest"] == dest, "T18.mesmo_post")
    r.check(est["edit_count"] == 2, "T18.sem_lost_update",
            f"edit_count={est['edit_count']} (esperado 2: 2 evolucoes)")
    r.check(len(c.edits) == 2, "T18.duas_edicoes", str(c.edits))
    r.check(all(mid == dest for mid, _ in c.edits), "T18.edicoes_no_alvo",
            str(c.edits))

    # 4. ESTADO FINAL COERENTE: o texto gravado e o do vencedor (ja
    #    montado, com marcacao), e o score descreve ESSE texto —
    #    invariante de post_estado.
    vencedor = c.edits[-1]
    r.check(("T1primeira" in est["texto"]) or ("T2segunda" in est["texto"]),
            "T18.texto_de_um_candidato", repr(est["texto"][:40]))
    r.check(vencedor[0] == dest, "T18.ultima_edicao_no_alvo", str(vencedor))
    r.check(est["score"] >= 7, "T18.score_do_texto_publicado",
            f"score={est['score']}")

    # 5. MIDIA FINAL COERENTE: apos uma TROCA autorizada, midia_chat tem
    #    dono real — nunca "" nem None — e a chave aceita bate com a
    #    midia_key de QUEM de fato aplicou.
    r.check(est["midia_chat"] in (BOM_A, BOM_B), "T18.midia_dono_real",
            f"midia_chat={est['midia_chat']!r}")
    aceita = g.midia_aceita_get(dest)
    r.check(aceita in ("mk-T1", "mk-T2"), "T18.chave_de_quem_aplicou",
            f"aceita={aceita!r}")
    esperada = "mk-T1" if est["midia_chat"] == BOM_A else "mk-T2"
    r.check(aceita == esperada, "T18.chave_casa_com_midia_chat",
            f"midia_chat={est['midia_chat']} mas chave={aceita!r}")


def test_ALVO_T18_materializacao_dentro_da_cadeia_de_locks(r: Resultado):
    """T18 (alvo) — com a E5.0, a materializacao acontece DEPOIS da
    decisao, sob lock_post. Logo a segunda task so baixa depois que a
    primeira libera a cadeia: os downloads NAO se sobrepoem.

    Hoje os dois downloads acontecem em montar(), fora dos locks, e se
    sobrepoem — e exatamente o trabalho que a frente move para dentro."""
    _c, _dest, eventos, _res, _n1, _n2 = _cenario_corrida()

    janelas = []
    for i, ev in enumerate(eventos):
        if ev.endswith(".inicio"):
            tag = ev.split(".")[1]
            fim = next((j for j in range(i + 1, len(eventos))
                        if eventos[j] == f"download.{tag}.fim"), None)
            if fim is not None:
                janelas.append((i, fim, tag))

    r.check(len(janelas) == 2, "T18.alvo.dois_downloads", str(eventos))
    for ini, fim, tag in janelas:
        intrusos = [e for e in eventos[ini + 1:fim] if not e.startswith(
            f"download.{tag}")]
        r.check(intrusos == [], "T18.alvo.downloads_nao_se_sobrepoem",
                f"durante o download de {tag}: {intrusos} "
                f"(E5.0 materializa sob lock_post)")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "E5.0 — NIVEL 2 (FRENTE) · arvore real"))
