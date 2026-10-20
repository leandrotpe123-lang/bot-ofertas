"""
FRENTE M — o DONO apagou um post do canal de destino (decisão de 16/10):
a mensagem seguinte da mesma campanha vai para o POST QUE FICOU (não abre
post novo) e o espelho do post apagado no canal de cupons sai junto.

CASOS REAIS (prévia pública e logs da Railway):
  18/10 22:00–22:37 UTC — relâmpago ML de 20%: Promotom 111543 → post
    29716; Fada 17883 (2,1 min) → post 29717 (duplicata); o dono apagou o
    29716; Samuel 121892 (36,6 min) caiu no 29716 apagado (DUP_SILENCIOSO
    num post invisível, 6 × "message ID is invalid").
  17/10 02:59–03:02 UTC — Shopee 00h BRT: Promotom 111508 "Cupons Shopee"
    (seco) → 29546; Fada 17870 "Novo Cupom Shopee VIP" (2,8 min) → 29549;
    o dono apagou o 29546.

  01  18/10: apagado o post da Promotom → fundido no da Fada; a mensagem do
      Samuel vai para o que ficou (nenhum post novo, nenhuma edição no
      apagado); espelho sai; status ok
  02  17/10: seco × VIP (sem benefício, mesmo momento) → fundido no que ficou
  03  post sem par → APAGADO FICA APAGADO até o fim da vida: a edição da
      própria fonte e a mesma oferta por outra fonte não abrem post nem
      tocam o canal (antes, a edição da fonte trazia o post de volta);
      depois da vida, renasce como sempre
  03b sem par, outra versão da campanha (outra identidade) publica — em
      SOMBRA e em ATIVO (a regra não junta no apagado)
  04  benefício diferente, mesma fonte ou dois candidatos → nunca funde
  05  o que o PRÓPRIO worker apaga (remoção da fusão, substituição) é
      ignorado; o handler repete o aviso → idempotente
  06  conferência: get_messages volta vazio para o apagado → mesmo
      tratamento; o anotado pelo worker não entra
  07  post fora da vida: só o espelho sai

  Endurecimento (revisão de 20/10):
  08  G1 — conferência por ID (conjuntos), nunca por posição: id omitido
      pelo Telegram é detectado; lista desalinhada, resposta malformada ou
      exceção não provam ausência
  09  G2 — a substituição só reenvia com a exclusão PROVADA (pts_count > 0);
      pts_count 0, vazio, desconhecido, exceção, exceção na 2ª tentativa e
      FloodWait longo terminam iguais: desmarca + conferência por id + None
  10  G3 — MessageIdInvalid não prova exclusão: uma chamada só (sem retry),
      com e sem mídia, exigindo ou não a imagem; quem decide é a
      confirmação por id
  11  _aplicar_evolucao → substituição sobre o post já apagado (aviso ainda
      não chegou): nenhum post novo; confirmado → silenciado, espelho sai
  12  falha no espelho (editar ou apagar no canal de cupons) nunca dispara a
      verificação nem apagados() do principal

    python tests/test_post_apagado.py
"""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_cupom_sem_codigo import (                             # noqa: E402
    FADA, PROMOTOM, SAMUEL, _db, cenario, msg, publicar)
from _harness_e5 import rodar                                   # noqa: E402
from database import db_exibida, db_get_post, db_ofertas_de_post  # noqa: E402
from pipeline import (apagados_pelo_worker, espelho_cupons,      # noqa: E402
                      origem, post_apagado, publicacao, saida)
from pipeline.enriquecimento import enriquecer                   # noqa: E402
from pipeline.montagem import montar                             # noqa: E402

ML, SHP = "mercadolivre", "shopee"
TUTORIAL_P = ["https://produto.mercadolivre.com.br/MLB-63220587-tutorial-_JM"]   # Promotom/Samuel
TUTORIAL_F = ["https://produto.mercadolivre.com.br/MLB-26070781-tutorial-_JM"]   # Fada

P_111543 = ('Próximo Cupom 20% OFF Mercado Livre\n\n•Tutorial para resgate: \n1 Entre no APP do Meli aqui: '
            '\n2 Clique em "Início" no rodapé do APP \n3 Depois no carrossel no canto superior esquerdo '
            'clique em "CUPONS RELÂMPAGO TODOS OS DIAS" \n4 Resgate o cupom')
F_17883 = ('🔥 Cupom 20% OFF Mercado Livre Full\n\nResgate agora e use 00h\n\n🚨🚨 LEIA PARA RESGATAR:\n'
           '1. Entre no APP 👇\n2. Clique em "Início" no rodapé do APP\n3. Depois, escolha o banner '
           '“Cupons Relâmpago”\n4. Resgate o cupom')
S_121892 = ('🔥 Próximo Cupom 20% OFF Mercado Livre\n\n⚠️ Resgate agora e use MEIA-NOITE\n\n⭐️ Tutorial para '
            'resgate:\n1. Entre no APP do Meli aqui:\n2. Clique em "Início" no rodapé do APP\n3. Depois no '
            'carrossel no canto superior esquerdo clique em "CUPONS RELÂMPAGO TODOS OS DIAS"\n4. Resgate o '
            'cupom\n\nanúncio')
P_111508 = "Cupons Shopee\n\n-Resgate aqui:\n1 https://s.shopee.com.br/2B5snGi9NX\n2 https://s.shopee.com.br/qaVCprDoA"
F_17870 = ("🚨 Novo Cupom Shopee VIP\n\n✅ Resgate aqui : https://s.shopee.com.br/40aG8LLgaE\n\n"
           "🔝 Assine aqui (1 Mês Grátis): https://s.shopee.com.br/AKZmtpLNvr")
F_30 = F_17883.replace("20%", "30%")


def _recuar(seg):
    with _db() as db:
        db.execute("UPDATE post_estado SET janela_fim = janela_fim - ?, ts = ts - ?", (seg, seg))


class _Espelho:
    """Espia espelho_cupons.removido (o caminho de sempre do espelho)."""
    def __init__(self):
        self.removidos, self._orig = [], espelho_cupons.removido

    def __enter__(self):
        espelho_cupons.removido = lambda mid: self.removidos.append(mid)
        return self

    def __exit__(self, *_):
        espelho_cupons.removido = self._orig


class _M:
    """Message do Telethon: o que importa à conferência é o id."""
    def __init__(self, i):
        self.id = i


def _post_de(n):
    return origem.consultar(n.chat, n.msg_id)


def _vivo(mid):
    e = db_get_post(mid) or {}
    return bool(e) and not e.get("fused_into") and not e.get("delete_status") \
        and float(e.get("janela_fim") or 0) > time.time()


# ══════════════════════════════════════════════════════════════════
def test_01_relampago_1810_mensagem_seguinte_vai_para_o_que_ficou(r):
    out = {}

    async def corpo(c):
        a = msg(PROMOTOM, P_111543, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(a)
        _recuar(127)
        b = msg(FADA, F_17883, urls_longas=TUTORIAL_F, plat=ML)
        await publicar(b)
        pa, pb = _post_de(a), _post_de(b)
        out["dois_posts"] = (pa != pb, c.novos)
        with _Espelho() as esp:
            out["feitos"] = await post_apagado.apagados([pa], "TELEGRAM")
            out["espelho"] = list(esp.removidos)
        ea = db_get_post(pa)
        out["fundido"] = (ea.get("fused_into"), ea.get("delete_status"))
        out["ancoras_a"] = db_ofertas_de_post(pa)
        novos_antes, edits_antes = c.novos, len(c.edits)
        _recuar(34 * 60 + 31)
        s = msg(SAMUEL, S_121892, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(s)
        out["samuel"] = (_post_de(s), pa, pb)
        out["sem_post_novo"] = c.novos == novos_antes
        out["sem_edicao_no_apagado"] = all(m != pa for m, _t in c.edits[edits_antes:])
        out["deletes"] = list(c.deletes)
    cenario(corpo)
    r.check(out["dois_posts"] == (True, 2), "01.hoje_dois_posts", str(out["dois_posts"]))
    pa, pb = out["samuel"][1], out["samuel"][2]
    r.check(out["feitos"] == [(pa, "fundido", pb)], "01.fundido_no_que_ficou", str(out["feitos"]))
    r.check(out["fundido"] == (pb, "ok"), "01.banco_fused_into_e_ok", str(out["fundido"]))
    r.check(out["ancoras_a"] == [], "01.ancoras_foram_para_o_que_ficou", str(out["ancoras_a"]))
    r.check(out["espelho"] == [pa], "01.espelho_sai_junto", str(out["espelho"]))
    r.check(out["samuel"][0] == pb, "01.samuel_vai_para_o_que_ficou", str(out["samuel"]))
    r.check(out["sem_post_novo"], "01.nenhum_post_novo")
    r.check(out["sem_edicao_no_apagado"], "01.nenhuma_edicao_no_apagado")
    r.check(out["deletes"] == [], "01.nenhuma_chamada_de_delete_no_canal", str(out["deletes"]))


def test_02_shopee_1710_seco_x_vip_funde_no_que_ficou(r):
    out = {}

    async def corpo(c):
        a = msg(PROMOTOM, P_111508, plat=SHP)
        await publicar(a)
        _recuar(166)
        b = msg(FADA, F_17870, plat=SHP)
        await publicar(b)
        pa, pb = _post_de(a), _post_de(b)
        with _Espelho():
            out["feitos"] = await post_apagado.apagados([pa], "TELEGRAM")
        out["ids"] = (pa, pb)
    cenario(corpo)
    pa, pb = out["ids"]
    r.check(pa != pb, "02.hoje_dois_posts")
    r.check(out["feitos"] == [(pa, "fundido", pb)], "02.fundido", str(out["feitos"]))


async def _editar(n):
    """A FONTE edita a mensagem `n` (mesmo chat e msg_id, texto novo)."""
    enr = enriquecer(n)
    await publicacao.enviar(await montar(n), n, enr=enr, is_edit=True)


def _intocado(c, antes, pa):
    novos, edits, deletes = antes
    return (c.novos == novos and len(c.deletes) == deletes
            and all(m != pa for m, _t in c.edits[edits:]))


def test_03_sem_par_fica_apagado_ate_o_fim_da_vida(r):
    # Apagado sem post que ficou: o post segue DONO da identidade e das
    # origens até o fim da vida, mas nada dele volta ao canal. Antes
    # (encerrar + esquecer) a edição da própria fonte — a Promotom edita o
    # "Próximo Cupom" — caía em NOVO ENVIO e o post apagado VOLTAVA.
    out = {}

    async def corpo(c):
        a = msg(PROMOTOM, P_111543, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(a)
        pa = _post_de(a)
        with _Espelho() as esp:
            out["feitos"] = await post_apagado.apagados([pa], "TELEGRAM")
            out["espelho"] = list(esp.removidos)
        e = db_get_post(pa)
        out["banco"] = (e.get("fused_into"), e.get("delete_status"),
                        float(e.get("janela_fim") or 0) > time.time(),
                        bool(db_ofertas_de_post(pa)), db_exibida(pa))
        # 1. a fonte edita a própria mensagem
        antes = (c.novos, len(c.edits), len(c.deletes))
        a2 = msg(PROMOTOM, P_111543.replace("Próximo Cupom", "Cupom LIBERADO"),
                 urls_longas=TUTORIAL_P, plat=ML, msg_id=a.msg_id)
        await _editar(a2)
        out["edicao_da_fonte"] = _intocado(c, antes, pa)
        # 2. a mesma oferta (mesma identidade) por outra fonte, 30 min depois
        _recuar(30 * 60)
        antes = (c.novos, len(c.edits), len(c.deletes))
        s = msg(SAMUEL, S_121892, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(s)
        out["mesma_identidade"] = _intocado(c, antes, pa)
        # 3. a vida acabou: renasce como post novo (a régua de sempre)
        _recuar(31 * 60)
        n = c.novos
        s2 = msg(SAMUEL, S_121892, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(s2)
        out["depois_da_vida"] = (c.novos - n, _post_de(s2) != pa)
        out["pa"] = pa
    cenario(corpo)
    pa = out["pa"]
    r.check(out["feitos"] == [(pa, "silenciado", None)], "03.silenciado", str(out["feitos"]))
    r.check(out["banco"] == (None, "ok", True, True, set()),
            "03.vivo_e_dono_da_identidade_sem_nada_exibido", str(out["banco"]))
    r.check(out["espelho"] == [pa], "03.espelho_sai", str(out["espelho"]))
    r.check(out["edicao_da_fonte"], "03.edicao_da_fonte_nao_traz_o_post_de_volta")
    r.check(out["mesma_identidade"], "03.mesma_oferta_nao_abre_post")
    r.check(out["depois_da_vida"] == (1, True), "03.fim_da_vida_renasce", str(out["depois_da_vida"]))


def test_03b_outra_versao_da_campanha_publica(r):
    # O apagado sem par não é candidato de regra: a versão de OUTRA fonte
    # (outra identidade `cupb`) publica normalmente — também com a regra
    # CUPOM-SEM-CÓDIGO em ATIVO, que de outro modo a juntaria no apagado.
    import config
    for modo in ("SOMBRA", "ATIVO"):
        out = {}

        async def corpo(c):
            a = msg(PROMOTOM, P_111543, urls_longas=TUTORIAL_P, plat=ML)
            await publicar(a)
            pa = _post_de(a)
            with _Espelho():
                await post_apagado.apagados([pa], "TELEGRAM")
            _recuar(127)
            n = c.novos
            b = msg(FADA, F_17883, urls_longas=TUTORIAL_F, plat=ML)
            await publicar(b)
            out["fada"] = (c.novos - n, _post_de(b) != pa)
        antigo = config.CUPOM_MODO
        config.CUPOM_MODO = modo
        try:
            cenario(corpo)
        finally:
            config.CUPOM_MODO = antigo
        r.check(out["fada"] == (1, True), f"03b.{modo}.fada_publica", str(out["fada"]))


def test_04_nunca_funde_sem_prova(r):
    casos = {
        # benefício diferente (20% × 30%): campanhas diferentes
        "beneficio_diferente": [(PROMOTOM, P_111543, TUTORIAL_P, 0), (FADA, F_30, TUTORIAL_F, 60)],
        # o outro post nasceu fora da rajada (8 min) do apagado
        "fora_da_rajada": [(PROMOTOM, P_111543, TUTORIAL_P, 0), (FADA, F_17883, TUTORIAL_F, 9 * 60)],
        # dois candidatos com o mesmo benefício: ambíguo
        "ambiguo": [(PROMOTOM, P_111543, TUTORIAL_P, 0), (FADA, F_17883, TUTORIAL_F, 60),
                    (SAMUEL, "🔥 Cupom Relâmpago 20% OFF Mercado Livre para Lojas Oficiais\n\nResgate agora",
                     TUTORIAL_P, 60)],
    }
    for nome, passos in casos.items():
        out = {}

        async def corpo(c, passos=passos):
            ns = []
            for chat, texto, urls, avanco in passos:
                if avanco:
                    _recuar(avanco)
                n = msg(chat, texto, urls_longas=urls, plat=ML)
                await publicar(n)
                ns.append(n)
            posts = [_post_de(n) for n in ns]
            out["distintos"] = len(set(posts)) == len(posts)
            with _Espelho():
                out["feitos"] = await post_apagado.apagados([posts[0]], "TELEGRAM")
            out["outros_vivos"] = all(_vivo(p) for p in posts[1:])
            out["p0"] = posts[0]
        cenario(corpo)
        r.check(out["distintos"], f"04.{nome}.hoje_posts_separados")
        r.check(out["feitos"] == [(out["p0"], "silenciado", None)], f"04.{nome}.silencia_sem_fundir",
                str(out["feitos"]))
        r.check(out["outros_vivos"], f"04.{nome}.outros_intocados")
    # mesma fonte: nunca é "duplicata entre grupos"
    out = {}

    async def corpo_mesma(c):
        a = msg(PROMOTOM, P_111543, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(a)
        _recuar(60)
        b = msg(PROMOTOM, F_17883, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(b)
        pa, pb = _post_de(a), _post_de(b)
        with _Espelho():
            out["feitos"] = await post_apagado.apagados([pa], "TELEGRAM") if pa != pb else "mesmo_post"
        out["pa"] = pa
    cenario(corpo_mesma)
    r.check(out["feitos"] in ("mesmo_post", [(out["pa"], "silenciado", None)]), "04.mesma_fonte_nao_funde",
            str(out["feitos"]))


def test_05_o_que_o_worker_apaga_e_ignorado_e_idempotente(r):
    out = {}

    async def corpo(c):
        a = msg(PROMOTOM, P_111543, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(a)
        _recuar(60)
        b = msg(FADA, F_17883, urls_longas=TUTORIAL_F, plat=ML)
        await publicar(b)
        pa, pb = _post_de(a), _post_de(b)
        # o worker apaga (remoção física da fusão, substituição): anotado antes
        await saida.apagar_post(pa)
        out["anotado"] = apagados_pelo_worker.foi_o_worker(pa)
        with _Espelho() as esp:
            out["do_worker"] = await post_apagado.apagados([pa], "TELEGRAM")
            out["vivo_ainda"] = _vivo(pa)
            # aviso repetido do dono sobre o b: o 1º funde/encerra, o 2º não muda nada
            out["b1"] = await post_apagado.apagados([pb], "TELEGRAM")
            out["b2"] = await post_apagado.apagados([pb], "TELEGRAM")
            out["espelho"] = list(esp.removidos)
        out["ids"] = (pa, pb)
    cenario(corpo)
    pa, pb = out["ids"]
    r.check(out["anotado"], "05.apagar_post_anota")
    r.check(out["do_worker"] == [] and out["vivo_ainda"], "05.exclusao_do_worker_ignorada",
            f"{out['do_worker']} vivo={out['vivo_ainda']}")
    r.check(len(out["b1"]) == 1 and out["b2"] == [], "05.idempotente", f"{out['b1']} {out['b2']}")
    r.check(out["espelho"] == [pb], "05.espelho_uma_vez", str(out["espelho"]))


def test_05b_worker_anota_enquanto_o_handler_espera_o_lock(r):
    # A substituição anota o id e apaga segurando o lock do post: o aviso
    # chega, o handler espera o lock e, sob ele, vê a anotação.
    from pipeline import exclusao
    out = {}

    async def corpo(c):
        a = msg(PROMOTOM, P_111543, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(a)
        _recuar(60)
        b = msg(FADA, F_17883, urls_longas=TUTORIAL_F, plat=ML)
        await publicar(b)
        pa = _post_de(a)
        lk = await exclusao.lock_post(pa)
        async with lk:
            t = asyncio.get_running_loop().create_task(post_apagado.apagados([pa], "TELEGRAM"))
            await asyncio.sleep(0.05)               # o handler já passou do atalho e espera
            apagados_pelo_worker.marcar(pa)
        with _Espelho():
            out["feitos"] = await t
        out["vivo"] = _vivo(pa)
    cenario(corpo)
    r.check(out["feitos"] == [] and out["vivo"], "05b.anotado_sob_o_lock_ignorado",
            f"{out['feitos']} vivo={out['vivo']}")


def test_06_conferencia_pega_o_aviso_perdido(r):
    out = {}

    async def corpo(c):
        a = msg(PROMOTOM, P_111543, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(a)
        _recuar(127)
        b = msg(FADA, F_17883, urls_longas=TUTORIAL_F, plat=ML)
        await publicar(b)
        _recuar(30)
        x = msg(SAMUEL, "Cupom Shopee VIP\n\nResgate aqui", plat=SHP)
        await publicar(x)
        pa, pb, px = _post_de(a), _post_de(b), _post_de(x)
        apagados_pelo_worker.marcar(px)          # o worker apagando o px agora
        sumidos = {pa, px}
        perguntados = []

        async def get_messages(dest, ids=None):
            perguntados.append(list(ids))
            return [None if i in sumidos else _M(i) for i in ids]
        c.get_messages = get_messages
        post_apagado._CONFIRMAR_S = 0.0
        with _Espelho():
            out["feitos"] = await post_apagado.conferir()
        out["perguntados"] = perguntados
        out["ids"] = (pa, pb, px)
        # um vazio isolado (1ª pergunta vazia, 2ª com a mensagem) não mexe em nada
        _recuar(1)
        y = msg(FADA, "Cupom Shopee VIP 2\n\nResgate", plat=SHP)
        await publicar(y)
        py = _post_de(y)
        respostas = iter([[None], [_M(py)]])

        async def soluco(dest, ids=None):
            return next(respostas) if ids == [py] else [_M(i) for i in ids]
        c.get_messages = soluco
        post_apagado.db_posts_vivos_recentes = lambda agora, lim, _o=post_apagado.db_posts_vivos_recentes: [py]
        try:
            out["soluco"] = await post_apagado.conferir()
        finally:
            post_apagado.db_posts_vivos_recentes = post_apagado.db_posts_vivos_recentes.__defaults__[0]
        out["py_vivo"] = _vivo(py)
    cenario(corpo)
    pa, pb, px = out["ids"]
    r.check(out["feitos"] == [(pa, "fundido", pb)], "06.conferencia_funde", str(out["feitos"]))
    r.check(len(out["perguntados"]) == 2 and set(out["perguntados"][0]) >= {pa, pb}
            and set(out["perguntados"][1]) == {pa, px},
            "06.lote_e_confirmacao_so_dos_sumidos", str(out["perguntados"]))
    r.check(out["soluco"] == [] and out["py_vivo"], "06.vazio_isolado_nao_mexe",
            f"{out['soluco']} vivo={out['py_vivo']}")


def test_07_fora_da_vida_so_o_espelho(r):
    out = {}

    async def corpo(c):
        a = msg(PROMOTOM, P_111543, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(a)
        pa = _post_de(a)
        _recuar(3601)
        with _Espelho() as esp:
            out["feitos"] = await post_apagado.apagados([pa], "TELEGRAM")
            out["espelho"] = list(esp.removidos)
        out["status"] = (db_get_post(pa) or {}).get("delete_status")
        out["pa"] = pa
    cenario(corpo)
    r.check(out["feitos"] == [(out["pa"], "fora_da_vida", None)], "07.fora_da_vida", str(out["feitos"]))
    r.check(out["espelho"] == [out["pa"]] and out["status"] == "ok", "07.espelho_sai",
            f"{out['espelho']} {out['status']}")


# ══════════════════════════════════════════════════════════════════
# Endurecimento (revisão de 20/10): G1 conferência por id; G2 reenvio só
# com exclusão provada; G3 MessageIdInvalid não prova exclusão; espelho.
# ══════════════════════════════════════════════════════════════════
def _dois_posts(c_out):
    """pa (Promotom) e pb (Fada, 127 s depois): o par de 18/10."""
    async def montar_par():
        a = msg(PROMOTOM, P_111543, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(a)
        _recuar(127)
        b = msg(FADA, F_17883, urls_longas=TUTORIAL_F, plat=ML)
        await publicar(b)
        return _post_de(a), _post_de(b)
    return montar_par


async def _drenar():
    """Espera as verificações agendadas pelo sinal de suspeita."""
    for _ in range(50):
        tarefas = [t for t in list(post_apagado._TAREFAS) if not t.done()]
        if not tarefas:
            return
        await asyncio.gather(*tarefas, return_exceptions=True)


def test_08_conferencia_por_id_nunca_por_posicao(r):
    casos = {
        # o Telegram OMITE o id apagado: a lista volta mais curta
        "omite": lambda pa, pb: (lambda ids: [_M(i) for i in ids if i != pa]),
        # desalinhada: um vazio de outro id na frente + os dois presentes
        "desalinhada": lambda pa, pb: (lambda ids: [None] + [_M(i) for i in ids]),
        # malformada: não é lista / item sem id — não prova ausência
        "nao_lista": lambda pa, pb: (lambda ids: None),
        "item_sem_id": lambda pa, pb: (lambda ids: [object() for _ in ids]),
    }
    esperado = {"omite": "fundido", "desalinhada": None, "nao_lista": None, "item_sem_id": None}
    for nome, fabrica in casos.items():
        out = {}

        async def corpo(c, fabrica=fabrica):
            pa, pb = await _dois_posts(out)()
            resp = fabrica(pa, pb)

            async def get_messages(dest, ids=None):
                return resp(list(ids))
            c.get_messages = get_messages
            post_apagado._CONFIRMAR_S = 0.0
            with _Espelho():
                out["feitos"] = await post_apagado.conferir()
            out["vivos"] = (_vivo(pa), _vivo(pb))
            out["ids"] = (pa, pb)
        cenario(corpo)
        pa, pb = out["ids"]
        if esperado[nome] == "fundido":
            r.check(out["feitos"] == [(pa, "fundido", pb)], f"08.{nome}.detecta_por_id", str(out["feitos"]))
        else:
            r.check(out["feitos"] == [] and out["vivos"] == (True, True), f"08.{nome}.nao_mexe",
                    f"{out['feitos']} vivos={out['vivos']}")
    # exceção na pergunta: não prova ausência, nada muda
    out = {}

    async def corpo_exc(c):
        pa, pb = await _dois_posts(out)()

        async def explode(dest, ids=None):
            raise RuntimeError("rede")
        c.get_messages = explode
        try:
            await post_apagado.conferir()
            out["subiu"] = False
        except RuntimeError:
            out["subiu"] = True
        out["vivos"] = (_vivo(pa), _vivo(pb))
    cenario(corpo_exc)
    r.check(out["vivos"] == (True, True), "08.excecao.nao_mexe", str(out))


def _substituicao(delete, presente):
    """Roda a substituição de mídia do post pa com o `delete` dado. A
    conferência por id responde `presente` para pa."""
    from telethon.errors import FloodWaitError
    out = {}

    async def corpo(c):
        a = msg(PROMOTOM, P_111543, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(a)
        pa = _post_de(a)
        perguntas = []
        chamadas = {"n": 0}

        async def delete_messages(dest, ids):
            chamadas["n"] += 1
            return delete(chamadas["n"], ids, FloodWaitError)

        async def get_messages(dest, ids=None):
            perguntas.append(list(ids))
            return [_M(i) for i in ids if presente]
        c.delete_messages = delete_messages
        c.get_messages = get_messages
        post_apagado._CONFIRMAR_S = 0.0
        post_apagado.registrar_suspeita()
        n0 = c.novos
        montada = await montar(a)
        with _Espelho() as esp:
            out["sent"] = await saida._substituir_post_com_midia(pa, montada)
            out["anotado"] = apagados_pelo_worker.foi_o_worker(pa)
            await _drenar()
            out["espelho"] = list(esp.removidos)
        out["novos"] = c.novos - n0
        out["perguntas"] = perguntas
        e = db_get_post(pa) or {}
        out["status"] = e.get("delete_status")
        out["pa"] = pa
    cenario(corpo)
    return out


def test_09_substituicao_so_reenvia_com_exclusao_provada(r):
    from _telethon_fakes import afetadas

    def _flood(seg):
        def f(n, ids, FW):
            if n == 1:
                try:
                    erro = FW(request=None, capture=seg)     # Telethon
                except TypeError:
                    erro = FW(seg)                           # bancada
                raise erro
            raise RuntimeError("delete recusado")
        return f
    casos = {
        # (delete, a mensagem ainda está no canal?)
        "pts_zero": (lambda n, ids, FW: afetadas(ids, 0), False),
        "vazio": (lambda n, ids, FW: [], True),
        "desconhecido": (lambda n, ids, FW: True, True),
        "excecao": (lambda n, ids, FW: (_ for _ in ()).throw(RuntimeError("rede")), True),
        "flood_e_excecao": (_flood(0), True),
        "flood_longo": (_flood(40), True),
    }
    for nome, (delete, presente) in casos.items():
        out = _substituicao(delete, presente)
        pa = out["pa"]
        r.check(out["sent"] is None and out["novos"] == 0, f"09.{nome}.nao_reenvia",
                f"sent={out['sent']} novos={out['novos']}")
        r.check(not out["anotado"], f"09.{nome}.desmarcado")
        r.check([pa] in out["perguntas"], f"09.{nome}.conferencia_por_id", str(out["perguntas"]))
        if presente:
            r.check(out["status"] is None and out["espelho"] == [], f"09.{nome}.no_ar_nada_muda",
                    f"status={out['status']} espelho={out['espelho']}")
        else:
            r.check(out["status"] == "ok" and out["espelho"] == [pa], f"09.{nome}.sumiu_silenciado",
                    f"status={out['status']} espelho={out['espelho']}")
    out = _substituicao(lambda n, ids, FW: afetadas(ids, 1), False)
    r.check(out["sent"] is not None and out["novos"] == 1 and out["anotado"],
            "09.confirmada.reenvia", f"sent={out['sent']} novos={out['novos']}")


def test_10_message_id_invalid_nao_prova_exclusao(r):
    from telethon.errors import MessageIdInvalidError
    casos = {"sem_midia": (None, False, True), "midia_sem_exigir": (b"img", False, True),
             "midia_exigindo": (b"img", True, True)}
    for nome, (imagem, exigir, trocar) in casos.items():
        for presente in (True, False):
            out = {}

            async def corpo(c, imagem=imagem, exigir=exigir, trocar=trocar, presente=presente):
                a = msg(PROMOTOM, P_111543, urls_longas=TUTORIAL_P, plat=ML)
                await publicar(a)
                pa = _post_de(a)
                edits = {"n": 0}

                async def edit_message(dest, msg_id, texto, parse_mode=None, file=None):
                    edits["n"] += 1
                    raise MessageIdInvalidError(request=None)

                async def get_messages(dest, ids=None):
                    return [_M(i) for i in ids if presente]
                c.edit_message = edit_message
                c.get_messages = get_messages
                post_apagado._CONFIRMAR_S = 0.0
                post_apagado.registrar_suspeita()
                with _Espelho():
                    res = await saida.editar_msg(pa, "texto novo", imagem,
                                                 exigir_imagem=exigir, trocar_midia=trocar)
                    out["vivo_logo_apos"] = _vivo(pa)
                    await _drenar()
                out["res"] = (res.ok, res.midia_aplicada)
                out["edits"] = edits["n"]
                out["vivo"] = _vivo(pa)
            cenario(corpo)
            tag = f"10.{nome}.{'no_ar' if presente else 'sumiu'}"
            r.check(out["edits"] == 1 and out["res"] == (False, False), f"{tag}.uma_chamada_sem_retry",
                    f"edits={out['edits']} res={out['res']}")
            r.check(out["vivo_logo_apos"], f"{tag}.sumiu_so_depois_de_confirmar")
            r.check(out["vivo"] == presente, f"{tag}.confirmacao_decide", f"vivo={out['vivo']}")


def test_11_evolucao_com_substituicao_nao_ressuscita_o_apagado(r):
    """O dono apagou o post e o aviso ainda não chegou: a evolução tenta
    editar (MessageIdInvalid) e cai na substituição — que não reenvia."""
    from types import SimpleNamespace
    from dataclasses import replace
    from telethon.errors import MessageIdInvalidError
    from _telethon_fakes import afetadas
    from pipeline import publicacao_aplicadores as ap
    for exigir in (True, False):
        out = {}

        async def corpo(c, exigir=exigir):
            a = msg(PROMOTOM, P_111543, urls_longas=TUTORIAL_P, plat=ML)
            await publicar(a)
            pa = _post_de(a)
            apagados = {pa}                       # o dono apagou no Telegram

            async def edit_message(dest, msg_id, texto, parse_mode=None, file=None):
                if msg_id in apagados:
                    raise MessageIdInvalidError(request=None)
                return _M(msg_id)

            async def delete_messages(dest, ids):
                lista = ids if isinstance(ids, list) else [ids]
                return afetadas(lista, sum(i not in apagados for i in lista))

            async def get_messages(dest, ids=None):
                return [_M(i) for i in ids if i not in apagados]
            c.edit_message, c.delete_messages, c.get_messages = edit_message, delete_messages, get_messages
            post_apagado._CONFIRMAR_S = 0.0
            post_apagado.registrar_suspeita()
            s = msg(SAMUEL, S_121892, urls_longas=TUTORIAL_P, plat=ML)
            montada = replace(await montar(s), imagem=b"img")
            d = SimpleNamespace(trocar_midia=True, exigir_imagem=exigir, permite_substituir=True,
                                novo_score=9, motivo="EVOLUI", motivo_midia="TROCA/upgrade",
                                score_atual=6, na_janela=True)
            estado = db_get_post(pa)
            n0 = c.novos
            with _Espelho() as esp:
                await ap._aplicar_evolucao(montada, s, d, estado, pa, 0,
                                           list(db_ofertas_de_post(pa)), "teste", exibidas=None)
                await _drenar()
                out["espelho"] = list(esp.removidos)
            out["novos"] = c.novos - n0
            out["status"] = (db_get_post(pa) or {}).get("delete_status")
            out["pa"] = pa
        cenario(corpo)
        tag = f"11.exigir_{exigir}"
        r.check(out["novos"] == 0, f"{tag}.nenhum_post_novo", f"novos={out['novos']}")
        r.check(out["status"] == "ok" and out["espelho"] == [out["pa"]], f"{tag}.confirmado_e_silenciado",
                f"status={out['status']} espelho={out['espelho']}")


def test_12_espelho_nunca_dispara_o_principal(r):
    from telethon.errors import MessageIdInvalidError
    out = {}

    async def corpo(c):
        a = msg(PROMOTOM, P_111543, urls_longas=TUTORIAL_P, plat=ML)
        await publicar(a)
        pa = _post_de(a)
        perguntas, edits = [], {"n": 0}

        async def edit_message(dest, msg_id, texto, parse_mode=None, file=None, **_k):
            edits["n"] += 1
            raise MessageIdInvalidError(request=None)

        async def delete_messages(dest, ids):
            raise MessageIdInvalidError(request=None)

        async def get_messages(dest, ids=None):
            perguntas.append(ids)
            return []
        c.edit_message, c.delete_messages, c.get_messages = edit_message, delete_messages, get_messages
        post_apagado.registrar_suspeita()
        # edição no canal de cupons (o espelho do pa) com o mesmo id do principal
        res = await saida._editar_inner_no_sem(pa, "t", None, destino="canal_cupons_teste")
        # apagar o espelho falha: engolido pelo próprio espelho
        await espelho_cupons._apagar_mensagem(pa)
        await _drenar()
        out.update(res=(res.ok, res.midia_aplicada), edits=edits["n"], perguntas=perguntas,
                   vivo=_vivo(pa), em_voo=len(post_apagado._TAREFAS))
    cenario(corpo)
    r.check(out["res"] == (False, False), "12.edicao_do_espelho_falha", str(out["res"]))
    r.check(out["perguntas"] == [] and out["vivo"], "12.principal_intocado",
            f"perguntas={out['perguntas']} vivo={out['vivo']}")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "FRENTE M · post apagado à mão pelo dono"))
