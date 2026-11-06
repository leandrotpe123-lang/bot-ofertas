"""
COMENTÁRIOS — repasse seletivo como RESPOSTA ao nosso post (Frente 2).

Política (pipeline.comentario_sinal, pura) e orquestração (pipeline.comentarios)
com banco REAL (origem_post, post_estado, comentario_repasse), lock_post real
e o envio simulado (saida.responder trocado por um espião — sem rede).

Caso real que guia tudo (04/11/2026, UTC):
  03:18:07  promotom/113150 (Fritadeira Wap) → nosso post 34670
  03:18:14  fadapromos/14472 (mesma Fritadeira) → DUP_SILENCIOSO no 34670
  03:18:27  fadapromos/14474 "Precinho"          (resposta a 14472)
  03:18:33  promotom/113151 "Isso é preção de vdd" (resposta a 113150)
  → os dois apontam para o 34670: sai UM (o primeiro), o outro não.

  01  sinais aceitos: preço, urgência, esgotado, voltou, baixou no PIX,
      cupom liberado/bug — texto preservado
  02  recusados: comentário genérico, autopromoção ("Esse aí forrou... saiu
      primeiro aqui!"), @, link de loja, link para outro post, pergunta,
      primeira pessoa, palavrão, sinal negado, longo
  03  link t.me para o PRÓPRIO post respondido sai; o sinal fica
  04  ponta a ponta: "Precinho" vira resposta ao post da âncora (UM envio)
  05  fontes: Nerd e Oferta Convertida (teste) fora; post que não é do canal
      (usuário comum) fora; mensagem velha fora (resposta a OUTRO chat: com o
      Telethon real, em test_comentarios_telethon)
  06  destaque já no post não repete e NÃO ocupa a vaga
  07  concorrentes (o caso real 34670): o primeiro elegível ganha; o
      recusado antes dele não ocupa a vaga
  08  evento duplicado (em voo e depois de concluído): um envio só
  09  reinício: memória zerada, mesmo comentário de novo → nada; reserva
      sem confirmação (caiu no meio) → nada (no máximo uma vez)
  10  sem destino: espera a âncora e desiste sem enviar; âncora que chega
      durante a espera (post segurado e solto) → envia
  11  post fundido/apagado → não envia
  12  falha no envio → registra FALHOU, a vaga continua do comentário
  13  comentário com mídia: vai só o texto
  14  comentário nunca vira oferta: nenhum post novo; o módulo não toca o
      pipeline de publicação

    python3 tests/test_comentarios.py
"""
from __future__ import annotations

import ast
import asyncio
import datetime as dt
import os
import sys
import time
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import RAIZ, preparar, rodar  # noqa: E402

preparar()

from database import _init_db                                   # noqa: E402

_init_db()

from database_conexao import _db                                # noqa: E402
from pipeline import comentario_sinal as sinal                  # noqa: E402
from pipeline import comentarios, exclusao, saida               # noqa: E402
from globals import _init_globals                               # noqa: E402

PROMOTOM, FADA, FADA_CUPONS = -1001825680721, -1002389711530, -1002050488946
SAMUEL, FUMOTOM = -1001768101197, -1003775401737
NERD, CONVERTIDA = -1001465877129, -1003817694320

_SEQ = {"dest": 50000, "msg": 600000}


def novo_dest() -> int:
    _SEQ["dest"] += 1
    return _SEQ["dest"]


def novo_msg() -> int:
    _SEQ["msg"] += 1
    return _SEQ["msg"]


def post(dest: int, texto: str = "🔥 Fritadeira Elétrica Wap 10L\n\n✅ R$ 235\nlink",
         fused=None, apagado=None) -> int:
    with _db() as db:
        db.execute("INSERT OR REPLACE INTO post_estado(msg_id_dest,texto,ts,fused_into,"
                   "delete_status) VALUES(?,?,?,?,?)", (dest, texto, time.time(), fused, apagado))
    return dest


def ancora(chat: int, msg_id: int, dest: int) -> None:
    with _db() as db:
        db.execute("INSERT OR REPLACE INTO origem_post(chat,msg_id,dest,ts) VALUES(?,?,?,?)",
                   (str(chat), msg_id, dest, time.time()))


def evento(chat: int, alvo, texto: str, *, mid=None, post_canal=True, idade_s=5.0,
           peer=None, media=None):
    rt = None if alvo is None else types.SimpleNamespace(reply_to_msg_id=alvo,
                                                          reply_to_peer_id=peer)
    m = types.SimpleNamespace(
        id=mid or novo_msg(), reply_to=rt, post=post_canal, message=texto, media=media,
        date=dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=idade_s))
    return types.SimpleNamespace(chat_id=chat, message=m)


class Espiao:
    """O lugar do saida.responder: guarda (dest, texto) e devolve um id."""
    def __init__(self, erro=None):
        self.enviados, self.erro, self.id = [], erro, 9000

    async def __call__(self, dest, texto):
        if self.erro:
            raise self.erro
        self.enviados.append((dest, texto))
        self.id += 1
        return types.SimpleNamespace(id=self.id)


DESFECHOS: list = []        # o que cada tarefa de repasse devolveu, em ordem


def rodar_eventos(eventos, espiao, espera=(0.01, 0.01)):
    """Entrega os eventos ao handler (na ordem) e espera o segundo plano."""
    real_resp, real_esp = saida.responder, comentarios.ESPERA_ANCORA_S
    saida.responder, comentarios.ESPERA_ANCORA_S = espiao, espera
    exclusao._POST_LOCKS.clear()
    exclusao._POST_LOCKS_TS.clear()

    async def corpo():
        res = []
        _init_globals()               # primitivas asyncio no loop do teste
        for ev in eventos:
            if callable(ev):            # um passo entre eventos (ex.: criar a âncora)
                ev()
                continue
            res.append(comentarios.ao_receber(ev))
            while comentarios._TAREFAS:
                DESFECHOS.extend(await asyncio.gather(*list(comentarios._TAREFAS)))
        return res
    try:
        return asyncio.run(corpo())
    finally:
        saida.responder, comentarios.ESPERA_ANCORA_S = real_resp, real_esp


def aceito(texto, canal="promotom", alvo=1):
    return sinal.avaliar(texto, canal, alvo).aceito


# ── 01–03: a política ─────────────────────────────────────────────
def test_01_sinais_aceitos(r):
    casos = {
        "Precinho": "preco", "PREÇÃO": "preco", "Isso é preção de vdd": "preco",
        "Bom preço": "preco", "Ótimo preço no TUF com RTX 5060!": "preco", "Mt barato": "preco",
        "De graça": "preco", "CORREEE": "urgencia", "Vai esgotar!": "urgencia",
        "Corre que esgota rápido!": "urgencia", "Só vai": "urgencia", "Aproveita!": "urgencia",
        "Esgotado ❌": "estoque", "VOLTOU!": "estoque", "Ativou": "estoque",
        "Baixou no PIX": "preco_mudou", "Baixou": "preco_mudou",
        "Liberado para resgate": "cupom_bug", "BUG": "cupom_bug",
    }
    falhas = {t: sinal.avaliar(t, "promotom", 1) for t, fam in casos.items()
              if not (sinal.avaliar(t, "promotom", 1).aceito
                      and fam in sinal.avaliar(t, "promotom", 1).familias)}
    r.check(falhas == {}, "01.sinais", str(falhas))
    d = sinal.avaliar("Isso é preção de vdd", "promotom", 113150)
    r.check(d.texto == "Isso é preção de vdd", "01.texto_preservado", repr(d.texto))


def test_02_recusados(r):
    casos = {
        "Raridade": "sem_sinal_objetivo", "TESTEM!": "sem_sinal_objetivo",
        "Cupom top!": "sem_sinal_objetivo", "Os comentários bugaram 😓": "grupo_falando_de_si",
        "Esse aí forrou. 32GB DDR5 6000MHz Corsair RGB que saiu a 800 conto. "
        "Essa saiu primeiro aqui!": "grupo_falando_de_si",
        "Esse aí forrou... saiu primeiro aqui!": "grupo_falando_de_si",
        "Esgotado! Ativem as notificações 🔔": "grupo_falando_de_si",
        "⚠️ Ainda vai ativar! Aviso aqui no grupo": "grupo_falando_de_si",
        "Muuita coisa boa, aproveitem @fadapromos": "arroba",
        "Precinho https://amzn.to/3AbCdEf": "link",
        "Precinho t.me/fumotom": "link",
        "1 CENTAVO https://t.me/fadapromos/11521": "link_outro_post",
        "Receberam o R$30 OFF também?": "pergunta", "Precinho?": "pergunta",
        "Adicionei +1 cupom": "primeira_pessoa", "Comprei 10, bom preço": "primeira_pessoa",
        "Eita carai, precinho": "palavrao", "Esse parece que n ativou": "sinal_negado",
        "Precinho " + "x" * 90: "longo", "": "vazio",
    }
    falhas = {t[:40]: sinal.avaliar(t, "promotom", 1).motivo for t, m in casos.items()
              if sinal.avaliar(t, "promotom", 1).aceito
              or sinal.avaliar(t, "promotom", 1).motivo != m}
    r.check(falhas == {}, "02.recusas_com_motivo", str(falhas))


def test_03_link_do_proprio_post_sai(r):
    d = sinal.avaliar("40% liberado para resgate!\nhttps://t.me/fadadoscupons/15928",
                      "fadadoscupons", 15928)
    r.check(d.aceito and d.texto == "40% liberado para resgate!", "03.proprio_post", repr(d))
    d2 = sinal.avaliar("40% liberado para resgate!\nhttps://t.me/fadadoscupons/15928",
                       "fadadoscupons", 15000)
    r.check(not d2.aceito and d2.motivo == "link_outro_post", "03.outro_post", repr(d2))


# ── 04–14: a orquestração ─────────────────────────────────────────
def test_04_ponta_a_ponta(r):
    dest = post(novo_dest())
    ancora(PROMOTOM, 113150, dest)
    esp = Espiao()
    res = rodar_eventos([evento(PROMOTOM, 113150, "Isso é preção de vdd")], esp)
    r.check(res == ["agendado"] and esp.enviados == [(dest, "Isso é preção de vdd")],
            "04.uma_resposta_ao_post", f"{res} {esp.enviados}")
    with _db() as db:
        row = db.execute("SELECT estado,resposta FROM comentario_repasse WHERE dest=?",
                         (dest,)).fetchone()
    r.check(row == ("enviado", esp.id), "04.fato_com_prova", str(row))
    outros = {"Esgotado ❌": FADA_CUPONS, "VOLTOU!": FADA, "Baixou no PIX": PROMOTOM,
              "Vai esgotar!": SAMUEL, "Liberado para resgate": FUMOTOM}
    esp2 = Espiao()
    evs = []
    for texto, chat in outros.items():
        d, alvo = post(novo_dest()), novo_msg()
        ancora(chat, alvo, d)
        evs.append(evento(chat, alvo, texto))
    rodar_eventos(evs, esp2)
    r.check(sorted(t for _, t in esp2.enviados) == sorted(outros), "04.cada_sinal",
            str(esp2.enviados))


def test_05_fontes_e_formas_fora(r):
    dest = post(novo_dest())
    for chat in (NERD, CONVERTIDA):
        ancora(chat, 777, dest)
    ancora(PROMOTOM, 778, dest)
    esp = Espiao()
    res = rodar_eventos([
        evento(NERD, 777, "PRECINHO"),
        evento(CONVERTIDA, 777, "Precinho"),
        evento(PROMOTOM, 778, "Precinho", post_canal=False),
        evento(PROMOTOM, 778, "Precinho", idade_s=3600),
        evento(PROMOTOM, None, "Precinho"),
    ], esp)
    r.check(res[:2] == ["fonte_fora", "fonte_fora"], "05.nerd_e_teste_fora", str(res))
    r.check(res[2] == "nao_e_post_do_canal", "05.usuario_comum_fora", str(res))
    r.check(res[3] == "velho" and res[4] == "nao_e_resposta", "05.velho_e_nao_resposta",
            str(res))
    r.check(esp.enviados == [], "05.nada_enviado_pelos_fora", str(esp.enviados))


def test_06_destaque_ja_no_post(r):
    dest = post(novo_dest(), "🔥 Precinho! Fritadeira Elétrica Wap\n\n✅ R$ 235")
    ancora(PROMOTOM, 801, dest)
    ancora(FADA, 802, dest)
    esp = Espiao()
    rodar_eventos([evento(PROMOTOM, 801, "Precinho"), evento(FADA, 802, "Vai esgotar!")], esp)
    r.check(esp.enviados == [(dest, "Vai esgotar!")], "06.repetido_nao_sai_e_nao_ocupa",
            str(esp.enviados))


def test_07_concorrentes_caso_real_34670(r):
    dest = post(novo_dest())
    ancora(PROMOTOM, 113150, dest)
    ancora(FADA, 14472, dest)            # a duplicata do Fada também ancora no post
    esp = Espiao()
    DESFECHOS.clear()
    rodar_eventos([
        evento(FADA, 14472, "Cupom top!"),                   # recusado: não ocupa
        evento(FADA, 14472, "Precinho"),                     # 03:18:27 — ganha
        evento(PROMOTOM, 113150, "Isso é preção de vdd"),    # 03:18:33 — vaga ocupada
    ], esp)
    r.check(esp.enviados == [(dest, "Precinho")], "07.primeiro_elegivel_ganha",
            str(esp.enviados))
    r.check(DESFECHOS == ["enviado", "vaga_ocupada"], "07.segundo_vaga_ocupada",
            str(DESFECHOS))


def test_08_evento_duplicado(r):
    dest = post(novo_dest())
    ancora(SAMUEL, 901, dest)
    esp = Espiao()
    ev = evento(SAMUEL, 901, "CORREEE")
    real_resp, real_esp = saida.responder, comentarios.ESPERA_ANCORA_S
    saida.responder, comentarios.ESPERA_ANCORA_S = esp, (0.01,)
    exclusao._POST_LOCKS.clear()

    async def em_voo():
        _init_globals()               # primitivas asyncio no loop do teste
        a = comentarios.ao_receber(ev)
        b = comentarios.ao_receber(ev)               # mesmo update, antes de concluir
        await asyncio.gather(*list(comentarios._TAREFAS))
        c = comentarios.ao_receber(ev)               # depois de concluído
        await asyncio.gather(*list(comentarios._TAREFAS))
        return a, b, c
    try:
        a, b, c = asyncio.run(em_voo())
    finally:
        saida.responder, comentarios.ESPERA_ANCORA_S = real_resp, real_esp
    r.check((a, b) == ("agendado", "em_voo") and c == "agendado", "08.desfechos", f"{a} {b} {c}")
    r.check(esp.enviados == [(dest, "CORREEE")], "08.um_envio", str(esp.enviados))


def test_09_reinicio(r):
    dest = post(novo_dest())
    ancora(PROMOTOM, 1001, dest)
    esp = Espiao()
    ev = evento(PROMOTOM, 1001, "Precin", mid=1002)
    rodar_eventos([ev], esp)
    comentarios._EM_VOO.clear()                      # processo novo: memória zerada
    comentarios._TAREFAS.clear()
    rodar_eventos([evento(PROMOTOM, 1001, "Precin", mid=1002)], esp)
    r.check(esp.enviados == [(dest, "Precin")], "09.depois_do_reinicio_nada", str(esp.enviados))
    dest2 = post(novo_dest())
    ancora(FADA, 1003, dest2)
    from database_comentarios import db_comentario_reservar
    r.check(db_comentario_reservar(str(FADA), 1004, dest2, "Precinho", time.time())
            == "reservado", "09.caiu_entre_reserva_e_envio")
    esp2 = Espiao()
    rodar_eventos([evento(FADA, 1003, "Precinho", mid=1004)], esp2)
    r.check(esp2.enviados == [], "09.no_maximo_uma_vez", str(esp2.enviados))


def test_10_sem_destino_e_ancora_tardia(r):
    esp = Espiao()
    res = rodar_eventos([evento(FUMOTOM, 1101, "Esgotado")], esp)
    r.check(res == ["agendado"] and esp.enviados == [], "10.sem_destino_nao_sai", str(esp.enviados))
    dest = post(novo_dest())
    esp2 = Espiao()
    ev = evento(FUMOTOM, 1102, "Esgotado")

    async def tardia():
        _init_globals()               # primitivas asyncio no loop do teste
        real_resp, real_esp = saida.responder, comentarios.ESPERA_ANCORA_S
        saida.responder, comentarios.ESPERA_ANCORA_S = esp2, (0.02, 0.02, 0.05, 0.05)
        exclusao._POST_LOCKS.clear()
        try:
            comentarios.ao_receber(ev)
            await asyncio.sleep(0.03)
            ancora(FUMOTOM, 1102, dest)               # a curadoria soltou o post
            await asyncio.gather(*list(comentarios._TAREFAS))
        finally:
            saida.responder, comentarios.ESPERA_ANCORA_S = real_resp, real_esp
    asyncio.run(tardia())
    r.check(esp2.enviados == [(dest, "Esgotado")], "10.ancora_tardia_envia", str(esp2.enviados))


def test_11_post_fundido_ou_apagado(r):
    d1, d2 = post(novo_dest(), fused=12345), post(novo_dest(), apagado="pendente")
    ancora(PROMOTOM, 1201, d1)
    ancora(PROMOTOM, 1202, d2)
    esp = Espiao()
    rodar_eventos([evento(PROMOTOM, 1201, "Precin"), evento(PROMOTOM, 1202, "Precin")], esp)
    r.check(esp.enviados == [], "11.nao_envia", str(esp.enviados))


def test_12_falha_no_envio(r):
    dest = post(novo_dest())
    ancora(SAMUEL, 1301, dest)
    ancora(SAMUEL, 1302, dest)
    rodar_eventos([evento(SAMUEL, 1301, "PREÇÃO")], Espiao(erro=RuntimeError("rede caiu")))
    with _db() as db:
        row = db.execute("SELECT estado FROM comentario_repasse WHERE dest=?", (dest,)).fetchone()
    esp = Espiao()
    rodar_eventos([evento(SAMUEL, 1302, "CORREEE")], esp)
    r.check(row == ("falhou",) and esp.enviados == [], "12.falhou_segura_a_vaga",
            f"{row} {esp.enviados}")


def test_13_midia_vai_so_o_texto(r):
    dest = post(novo_dest())
    ancora(FADA_CUPONS, 1401, dest)
    esp = Espiao()
    rodar_eventos([evento(FADA_CUPONS, 1401, "Esgotado ❌", media=object())], esp)
    r.check(esp.enviados == [(dest, "Esgotado ❌")], "13.so_texto", str(esp.enviados))


def test_14_nunca_vira_oferta(r):
    with _db() as db:
        antes = db.execute("SELECT COUNT(*) FROM post_estado").fetchone()[0] \
            - db.execute("SELECT COUNT(*) FROM post_estado WHERE msg_id_dest>50000").fetchone()[0]
    dest = post(novo_dest())
    ancora(PROMOTOM, 1501, dest)
    rodar_eventos([evento(PROMOTOM, 1501, "De graça")], Espiao())
    with _db() as db:
        depois = db.execute("SELECT COUNT(*) FROM post_estado").fetchone()[0] \
            - db.execute("SELECT COUNT(*) FROM post_estado WHERE msg_id_dest>50000").fetchone()[0]
    r.check(antes == depois, "14.nenhum_post_novo", f"{antes} {depois}")
    arvore = ast.parse(open(os.path.join(RAIZ, "pipeline", "comentarios.py"),
                            encoding="utf-8").read())
    importados = {n.module for n in ast.walk(arvore) if isinstance(n, ast.ImportFrom) and n.module}
    importados |= {a.name for n in ast.walk(arvore) if isinstance(n, ast.ImportFrom)
                   for a in n.names}
    proibidos = {"publicacao", "orchestrator", "orchestrator_pipeline", "normalizacao",
                 "processar", "pipeline.publicacao", "pipeline.orchestrator"}
    r.check(not (importados & proibidos), "14.nao_toca_o_pipeline", str(importados & proibidos))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "COMENTÁRIOS · repasse seletivo · caso real 34670"))
