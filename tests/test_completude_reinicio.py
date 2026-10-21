"""COMPLETUDE DA ENTRADA ATRAVÉS DO REINÍCIO — contra o Telethon REAL e o
SQLite REAL (o banco temporário do harness).

O caso de produção (21/10/2026, UTC):

    03:03:31  @promotom 111717 chega como NOVA → post 30480
    04:22:34  deploy: processo novo; o estado da completude (só memória)
              começa vazio
    10:11:01  edição velha da 111715 (EDIT_ANTIGO) ← 1º update visto
    10:11:03  edição velha da 111716 (EDIT_ANTIGO)
    11:58:18  111718 chega → "[BURACO] ids=[111717] entre 111716 e 111718"
    11:58:21  "[RECUPERADA] id=111717 idade=8h54m — o Telegram não
              entregou" → NOVA_ANTIGA

A 111717 TINHA sido entregue e publicada antes do reinício: o buraco era
falso, e a frase sobre o transporte, uma hipótese dita como fato.

O QUE ESTE ARQUIVO PROVA
  01  o caso de produção: depois do reinício, nada é buscado, nenhum
      buraco nem recuperação (falha no estado só em memória)
  02  controle: buraco genuíno no mesmo processo segue recuperado UMA vez,
      com a proveniência de quem observou continuamente
  03  buraco genuíno que ATRAVESSA o reinício (mensagem da janela sem bot)
      é detectado e buscado UMA vez
  04  buraco pendente quando o processo morre é retomado no boot seguinte,
      UMA busca (um "maior visto" sozinho o perderia)
  05  reinício no MEIO da busca: o id em curso é retomado UMA vez e
      entregue UMA vez
  06  desfecho final (entregue ou ausente) não se repete depois do reinício
  07  edição antiga como 1º evento depois do boot: nem buraco, nem busca,
      nem escrita
  08  falha de GRAVAÇÃO: a recuperação do processo segue, nada levanta, um
      aviso por sequência de falhas e um quando volta
  09  falha ao CARREGAR: comportamento antigo (base no 1º id visto), mas a
      proveniência diz que a base não foi restaurada e que a mensagem é
      anterior ao início — sem afirmar nada sobre o transporte
  10  a completude não afirma perda de transporte em lugar nenhum
  11  estado salvo malformado ou de canal fora da monitoração: ignorado
  12  persistência mínima: uma linha por canal (maior + pendentes), limitada
      pela janela de memória
  13  mensagem genuinamente ausente num buraco que atravessa o reinício:
      AUSENTE uma vez, e não volta a ser buscada no reinício seguinte
  14  gravação que falhou e reinício em seguida (resíduo aceito): a
      contagem salva ficou para trás, os ids do meio são buscados UMA vez e
      a proveniência mostra que são anteriores ao início

Execução (standalone, stdlib + Telethon):
    python3 tests/test_completude_reinicio.py
"""
from __future__ import annotations

import asyncio
import datetime as dt
import importlib
import logging
import os
import pathlib
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import telethon  # noqa: E402  — REAL, antes do harness
from telethon import TelegramClient, utils  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402
from telethon.tl import types  # noqa: E402

assert hasattr(telethon.TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

from _harness_e5 import preparar, RAIZ  # noqa: E402

preparar()

import database  # noqa: E402
import eventos  # noqa: E402
import globals as g  # noqa: E402
from pipeline import completude  # noqa: E402

database._init_db()

PROMOTOM_ID, SAMUEL_ID = 1825680721, 1768101197


def _canal(cid, user):
    return types.Channel(id=cid, title=user, photo=types.ChatPhotoEmpty(),
                         date=dt.datetime.now(dt.timezone.utc), access_hash=77,
                         username=user, broadcast=True)


PROMOTOM = _canal(PROMOTOM_ID, "promotom")
SAMUEL = _canal(SAMUEL_ID, "samuelf3lipepromo")
PRO, SAM = utils.get_peer_id(PROMOTOM), utils.get_peer_id(SAMUEL)


class Cliente:
    """TelegramClient REAL, sem rede: só `get_messages(entidade, ids=)` — a
    fronteira que a completude usa — é trocado."""

    def __init__(self, atraso=0.0):
        self.tc = TelegramClient(StringSession(), 1, "0" * 32)
        self.tc._mb_entity_cache.set_self_user(424242, False, 0)
        self.servidor = {}
        self.atraso = atraso
        self.chamadas = []
        self.tc.get_messages = self._get_messages

    async def _get_messages(self, entidade, ids):
        peer = utils.get_peer_id(entidade)
        self.chamadas.append((peer, list(ids)))
        if self.atraso:
            await asyncio.sleep(self.atraso)
        return [self.servidor.get((peer, i)) for i in ids]

    def publicar(self, canal, mid, idade_s=30.0):
        d = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=idade_s)
        m = types.Message(id=mid, peer_id=types.PeerChannel(canal.id), date=d,
                          message=f"oferta {mid}", post=True)
        m._finish_init(self.tc, {utils.get_peer_id(canal): canal}, None)
        self.servidor[(utils.get_peer_id(canal), mid)] = m
        return m


class Entrada:
    """Ponto de entrada espião — o papel de `processar`."""

    def __init__(self):
        self.recebidos = []

    async def __call__(self, evento, is_edit=False):
        self.recebidos.append((evento.chat_id, evento.message.id, is_edit))


class _Log(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.linhas = []

    def emit(self, rec):
        self.linhas.append((rec.levelname, rec.getMessage()))


LOG = _Log()
logging.getLogger("INGESTAO").addHandler(LOG)

EVENTOS = []


def _espiao(tipo, coletor, local=None, minimo=None):
    """Captura o evento como a coleta o veria (o coletor é chamado aqui)."""
    try:
        corr, dados = coletor()
    except Exception as e:                          # noqa: BLE001
        corr, dados = {}, {"erro_coletor": repr(e)}
    EVENTOS.append({"tipo": tipo, "local": local, "corr": corr, "dados": dados})
    return 0


eventos.emitir_de = _espiao


def _limpar_banco():
    try:
        with database._db() as db:
            db.execute("DELETE FROM completude_canal")
    except Exception:                               # noqa: BLE001 — base antiga: sem tabela
        pass


def _linhas_salvas():
    try:
        with database._db() as db:
            return db.execute(
                "SELECT chat, maior, pendentes FROM completude_canal ORDER BY chat").fetchall()
    except Exception:                               # noqa: BLE001
        return None


def ligar(cliente, entrada, fontes=(PROMOTOM, SAMUEL), espera=0.05):
    completude._ESPERA_S = espera
    g._encerrando = False
    completude.instalar(cliente.tc, list(fontes), entrada)


def novo_cenario(cliente, entrada, **kw):
    """Processo do zero: banco sem estado da completude, módulo recarregado."""
    _limpar_banco()
    importlib.reload(completude)
    ligar(cliente, entrada, **kw)
    LOG.linhas.clear()
    EVENTOS.clear()


async def reiniciar(cliente, entrada, **kw):
    """O processo morre e outro sobe: as tasks morrem junto (sem desfecho),
    a conexão fecha e reabre (o MESMO arquivo, como o volume) e o módulo
    volta ao estado de import."""
    for c in list(completude._canais.values()):
        t = getattr(c, "tarefa", None)
        if t is not None and not t.done():
            t.cancel()
    await asyncio.sleep(0.01)
    database._fechar_db()
    database._init_db()
    importlib.reload(completude)
    ligar(cliente, entrada, **kw)


async def esperar(limite=3.0):
    fim = time.monotonic() + limite
    while time.monotonic() < fim:
        await asyncio.sleep(0.02)
        vivas = [c.tarefa for c in completude._canais.values()
                 if c.tarefa is not None and not c.tarefa.done()]
        if not vivas:
            return


def rodar(corpo):
    return asyncio.run(corpo())


def _ev(tipo):
    return [e for e in EVENTOS if e["tipo"] == tipo]


def _textos(marca):
    return [m for _, m in LOG.linhas if marca in m]


# ══════════════════════════════════════════════════════════════════
def t01_caso_111717_depois_do_reinicio(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        novo_cenario(cli, ent)
        for mid in (111715, 111716, 111717):        # processo A: chegam como NOVAS
            cli.publicar(PROMOTOM, mid, idade_s=3600)
            completude.observar(PRO, mid)
        await esperar()
        await reiniciar(cli, ent)                   # deploy
        LOG.linhas.clear()
        EVENTOS.clear()
        completude.observar(PRO, 111715)            # edição velha (1º update visto)
        completude.observar(PRO, 111716)            # edição velha
        cli.publicar(PROMOTOM, 111718, idade_s=1)
        completude.observar(PRO, 111718)            # nova
        await esperar()
        return cli, ent
    cli, ent = rodar(corpo)
    r.ok(cli.chamadas == [], f"nenhuma busca depois do reinício ({cli.chamadas})")
    r.ok(ent.recebidos == [], f"nada reentregue ({ent.recebidos})")
    r.ok(_textos("[BURACO]") == [] and _textos("[RECUPERADA]") == [],
         f"nenhum buraco/recuperação no log ({_textos('[BURACO]') + _textos('[RECUPERADA]')})")
    r.ok(_ev("origem.buraco") == [] and _ev("origem.recuperada") == [],
         f"nenhum evento de buraco/recuperada ({EVENTOS})")


def t02_controle_buraco_genuino_no_mesmo_processo(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        novo_cenario(cli, ent)
        await asyncio.sleep(1.1)                    # a mensagem nasce DEPOIS do início
        completude.observar(SAM, 200)
        cli.publicar(SAMUEL, 201, idade_s=0)
        completude.observar(SAM, 202)
        await esperar()
        return cli, ent
    cli, ent = rodar(corpo)
    r.ok(cli.chamadas == [(SAM, [201])], f"UMA busca, só da 201 ({cli.chamadas})")
    r.ok(ent.recebidos == [(SAM, 201, False)], f"entregue UMA vez, como nova ({ent.recebidos})")
    rec = _ev("origem.recuperada")
    d = rec[0]["dados"] if rec else {}
    r.ok(len(rec) == 1 and d.get("anterior_ao_inicio") is False,
         f"proveniência: observação contínua, posterior ao início ({rec})")
    r.ok(d.get("base_restaurada") is False, f"base criada neste processo ({d})")


def t03_buraco_genuino_atravessa_o_reinicio(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        novo_cenario(cli, ent)
        completude.observar(SAM, 300)
        await esperar()
        cli.publicar(SAMUEL, 301, idade_s=4)        # postada na janela sem bot
        await reiniciar(cli, ent)
        LOG.linhas.clear()
        EVENTOS.clear()
        completude.observar(SAM, 302)
        await esperar()
        return cli, ent
    cli, ent = rodar(corpo)
    r.ok(cli.chamadas == [(SAM, [301])], f"a 301 é buscada UMA vez ({cli.chamadas})")
    r.ok(ent.recebidos == [(SAM, 301, False)], f"entregue UMA vez ({ent.recebidos})")
    rec = _ev("origem.recuperada")
    d = rec[0]["dados"] if rec else {}
    r.ok(d.get("anterior_ao_inicio") is True and d.get("base_restaurada") is True,
         f"proveniência: anterior ao início, base restaurada ({rec})")


def t04_pendente_sobrevive_ao_reinicio(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        novo_cenario(cli, ent, espera=5.0)          # o processo morre DENTRO da espera
        completude.observar(PRO, 400)
        cli.publicar(PROMOTOM, 401)
        cli.publicar(PROMOTOM, 402)
        completude.observar(PRO, 403)
        await asyncio.sleep(0.05)
        antes = list(cli.chamadas)
        await reiniciar(cli, ent)
        await esperar()
        return cli, ent, antes
    cli, ent, antes = rodar(corpo)
    r.ok(antes == [], f"o processo A morreu antes de buscar ({antes})")
    r.ok(cli.chamadas == [(PRO, [401, 402])],
         f"o boot seguinte busca os pendentes UMA vez ({cli.chamadas})")
    r.ok(sorted(ent.recebidos) == [(PRO, 401, False), (PRO, 402, False)],
         f"cada um entregue UMA vez ({ent.recebidos})")


def t05_reinicio_no_meio_da_busca(r):
    async def corpo():
        cli, ent = Cliente(atraso=5.0), Entrada()   # a busca pendura
        novo_cenario(cli, ent)
        completude.observar(SAM, 500)
        cli.publicar(SAMUEL, 501)
        completude.observar(SAM, 502)
        await asyncio.sleep(0.3)                    # já dentro do get_messages
        em_curso = list(cli.chamadas)
        cli.atraso = 0.0
        await reiniciar(cli, ent)
        await esperar()
        return cli, ent, em_curso
    cli, ent, em_curso = rodar(corpo)
    r.ok(em_curso == [(SAM, [501])], f"A morreu com a busca em curso ({em_curso})")
    r.ok(cli.chamadas == [(SAM, [501]), (SAM, [501])],
         f"retomada UMA vez no boot seguinte ({cli.chamadas})")
    r.ok(ent.recebidos == [(SAM, 501, False)], f"entregue UMA vez ({ent.recebidos})")


def t06_desfecho_final_nao_repete(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        novo_cenario(cli, ent)
        completude.observar(SAM, 600)
        cli.publicar(SAMUEL, 601)
        completude.observar(SAM, 602)               # 601 entregue
        completude.observar(PRO, 700)
        completude.observar(PRO, 702)               # 701 não existe: AUSENTE
        await esperar()
        n_antes = len(cli.chamadas)
        await reiniciar(cli, ent)
        completude.observar(SAM, 603)
        completude.observar(PRO, 703)
        await esperar()
        return cli, ent, n_antes
    cli, ent, n_antes = rodar(corpo)
    r.ok(n_antes == 2, f"duas buscas no processo A ({cli.chamadas[:n_antes]})")
    r.ok(len(cli.chamadas) == n_antes, f"nada repetido depois do reinício ({cli.chamadas})")
    r.ok(ent.recebidos == [(SAM, 601, False)], f"a 601 entregue UMA vez ({ent.recebidos})")


def t07_edicao_antiga_primeiro_evento_depois_do_boot(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        novo_cenario(cli, ent)
        for mid in range(800, 806):
            completude.observar(SAM, mid)
        await reiniciar(cli, ent)
        salvas_antes = _linhas_salvas()
        completude.observar(SAM, 790)               # edição de mensagem velha
        salvas_depois = _linhas_salvas()
        completude.observar(SAM, 806)
        await esperar()
        return cli, salvas_antes, salvas_depois
    cli, antes, depois = rodar(corpo)
    r.ok(cli.chamadas == [], f"nenhuma busca ({cli.chamadas})")
    r.ok(antes is not None and antes == depois,
         f"a edição velha não muda o estado salvo ({antes} → {depois})")


def t08_falha_de_gravacao(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        novo_cenario(cli, ent)
        orig = getattr(completude, "db_completude_gravar", None)

        def quebrada(*a, **k):
            raise RuntimeError("disco cheio")
        completude.db_completude_gravar = quebrada
        erros = []
        try:
            for mid in (900, 902, 903, 905):
                try:
                    completude.observar(SAM, mid)
                except Exception as e:              # noqa: BLE001
                    erros.append(repr(e))
            cli.publicar(SAMUEL, 901)
            cli.publicar(SAMUEL, 904)
            await esperar()
        finally:
            if orig is not None:
                completude.db_completude_gravar = orig
        completude.observar(SAM, 906)               # a gravação voltou
        return cli, ent, erros
    cli, ent, erros = rodar(corpo)
    r.ok(erros == [], f"observar nunca levanta ({erros})")
    r.ok(sorted(ent.recebidos) == [(SAM, 901, False), (SAM, 904, False)],
         f"a recuperação do processo segue ({ent.recebidos})")
    avisos = [m for lv, m in LOG.linhas if lv == "WARNING" and "estado" in m and "grav" in m]
    r.ok(len(avisos) == 1, f"UM aviso para a sequência de falhas ({avisos})")
    r.ok(any("grava" in m and "voltou" in m for _, m in LOG.linhas),
         f"aviso de que voltou ({[m for _, m in LOG.linhas][-3:]})")


def t09_falha_ao_carregar(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        novo_cenario(cli, ent)
        for mid in (111715, 111716, 111717):
            cli.publicar(PROMOTOM, mid, idade_s=3600)
            completude.observar(PRO, mid)
        await esperar()
        for c in list(completude._canais.values()):
            if c.tarefa is not None and not c.tarefa.done():
                c.tarefa.cancel()
        database._fechar_db()
        database._init_db()
        importlib.reload(completude)
        completude.db_completude_carregar = lambda chats: None    # banco indisponível
        LOG.linhas.clear()
        EVENTOS.clear()
        ligar(cli, ent)
        completude.observar(PRO, 111715)
        completude.observar(PRO, 111716)
        cli.publicar(PROMOTOM, 111718, idade_s=1)
        completude.observar(PRO, 111718)
        await esperar()
        return cli, ent
    cli, ent = rodar(corpo)
    r.ok(any(lv == "WARNING" and "estado" in m and "indispon" in m for lv, m in LOG.linhas),
         f"aviso de base não restaurada ({LOG.linhas[:3]})")
    r.ok(cli.chamadas == [(PRO, [111717])],
         f"sem base salva, o comportamento antigo: busca a 111717 ({cli.chamadas})")
    rec = _ev("origem.recuperada")
    d = rec[0]["dados"] if rec else {}
    r.ok(d.get("base_restaurada") is False and d.get("anterior_ao_inicio") is True,
         f"proveniência: base NÃO restaurada, mensagem anterior ao início ({rec})")
    rec_log = _textos("[RECUPERADA]")
    r.ok(rec_log and all("não entregou" not in m for m in rec_log),
         f"o log não afirma perda de transporte ({rec_log})")


def t10_nenhuma_afirmacao_de_transporte(r):
    fonte = (pathlib.Path(RAIZ) / "pipeline" / "completude.py").read_text(encoding="utf-8")
    r.ok("não entregou" not in fonte and "nao entregou" not in fonte,
         "completude.py não diz que o Telegram não entregou")


def t11_estado_malformado_ou_canal_fora(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        novo_cenario(cli, ent)
        try:
            with database._db() as db:
                db.execute("INSERT OR REPLACE INTO completude_canal(chat,maior,pendentes,ts)"
                           " VALUES(?,?,?,?)", (SAM, 1000, "lixo{", time.time()))
                # JSON válido, tipo errado: sem a checagem, a comparação com o
                # maior levantaria dentro do instalar e derrubaria o boot.
                db.execute("INSERT OR REPLACE INTO completude_canal(chat,maior,pendentes,ts)"
                           " VALUES(?,?,?,?)", (PRO, 2000, '["1999"]', time.time()))
                db.execute("INSERT OR REPLACE INTO completude_canal(chat,maior,pendentes,ts)"
                           " VALUES(?,?,?,?)", (-1009999999999, 5, "[3]", time.time()))
        except Exception:                           # noqa: BLE001 — base antiga
            pass
        erro = None
        try:
            await reiniciar(cli, ent)
        except Exception as e:                      # noqa: BLE001
            erro = repr(e)
        canais = set(completude._canais)
        completude.observar(SAM, 1003)              # sem base para o SAM: só inicia
        completude.observar(PRO, 2005)              # sem base para a PRO: só inicia
        await esperar()
        return cli, canais, erro
    cli, canais, erro = rodar(corpo)
    r.ok(erro is None, f"estado malformado não derruba o instalar ({erro})")
    r.ok(cli.chamadas == [], f"linha malformada não inventa buraco ({cli.chamadas})")
    r.ok(canais == set(),
         f"nada restaurado de malformado ou de fora da monitoração ({canais})")
    avisos = [m for lv, m in LOG.linhas if lv == "WARNING" and "malformad" in m]
    r.ok(len(avisos) == 2, f"um aviso por linha malformada ({avisos})")


def t12_persistencia_minima(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        novo_cenario(cli, ent, espera=5.0)
        completude.observar(SAM, 10000)
        completude.observar(SAM, 10004)             # pendentes 10001..10003
        meio = _linhas_salvas()
        completude.observar(SAM, 10002)             # chegou sozinho
        depois = _linhas_salvas()
        for mid in range(10005, 10005 + 2 * completude._JANELA_IDS + 5):
            completude.observar(SAM, mid)
        fim = _linhas_salvas()
        for c in completude._canais.values():
            if c.tarefa is not None:
                c.tarefa.cancel()
        await asyncio.sleep(0)
        return meio, depois, fim
    meio, depois, fim = rodar(corpo)
    r.ok(meio == [(SAM, 10004, "[10001, 10002, 10003]")], f"maior + pendentes ({meio})")
    r.ok(depois == [(SAM, 10004, "[10001, 10003]")], f"o que chegou sai dos pendentes ({depois})")
    r.ok(fim is not None and len(fim) == 1 and fim[0][2] == "[]",
         f"uma linha por canal; pendente fora da janela é podado ({fim})")


def t13_ausente_atravessando_o_reinicio(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        novo_cenario(cli, ent)
        completude.observar(PRO, 1200)
        await reiniciar(cli, ent)                   # a 1201 nunca existiu no canal
        LOG.linhas.clear()
        EVENTOS.clear()
        completude.observar(PRO, 1202)
        await esperar()
        eventos_b = list(EVENTOS)
        await reiniciar(cli, ent)
        completude.observar(PRO, 1203)
        await esperar()
        return cli, ent, eventos_b
    cli, ent, eventos_b = rodar(corpo)
    r.ok(cli.chamadas == [(PRO, [1201])], f"buscada UMA vez, nunca mais ({cli.chamadas})")
    r.ok(ent.recebidos == [], f"nada entregue ({ent.recebidos})")
    aus = [e for e in eventos_b if e["tipo"] == "origem.recuperacao_falhou"
           and e["dados"].get("motivo") == "AUSENTE"]
    r.ok(len(aus) == 1 and aus[0]["corr"].get("msg") == 1201, f"AUSENTE uma vez ({eventos_b})")


def t14_gravacao_falhou_e_reiniciou(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        novo_cenario(cli, ent)
        completude.observar(SAM, 1099)              # gravado
        orig = completude.db_completude_gravar
        completude.db_completude_gravar = lambda *a, **k: False
        try:
            for mid in (1100, 1101, 1102):          # vistos, mas não gravados
                cli.publicar(SAMUEL, mid, idade_s=40)
                completude.observar(SAM, mid)
        finally:
            completude.db_completude_gravar = orig
        await reiniciar(cli, ent)
        LOG.linhas.clear()
        EVENTOS.clear()
        completude.observar(SAM, 1103)
        await esperar()
        return cli, ent
    cli, ent = rodar(corpo)
    r.ok(cli.chamadas == [(SAM, [1100, 1101, 1102])],
         f"a contagem salva ficou para trás: os do meio buscados UMA vez ({cli.chamadas})")
    rec = _ev("origem.recuperada")
    r.ok(len(rec) == 3 and all(e["dados"].get("anterior_ao_inicio") is True
                               and e["dados"].get("base_restaurada") is True for e in rec),
         f"proveniência: anteriores ao início, base restaurada ({rec})")
    r.ok(all("não observada" in m for m in _textos("[RECUPERADA]")),
         f"o log diz o fato ({_textos('[RECUPERADA]')})")


# ── runner ────────────────────────────────────────────────────────
class Resultado:
    def __init__(self):
        self.n, self.falhas = 0, []

    def ok(self, cond, desc):
        self.n += 1
        if not cond:
            self.falhas.append(desc)


def main() -> int:
    testes = [(k, v) for k, v in sorted(globals().items())
              if k.startswith("t") and k[1:3].isdigit() and callable(v)]
    print(f"Telethon REAL {telethon.__version__}")
    tot = falhos = 0
    for nome, fn in testes:
        r = Resultado()
        try:
            fn(r)
        except Exception:
            r.falhas.append("EXCEÇÃO:\n" + traceback.format_exc())
        tot += r.n
        print(f"  {'ok  ' if not r.falhas else 'FALHA'} {nome:46s} {r.n} asserts")
        for f in r.falhas:
            print(f"        -> {f}")
        falhos += bool(r.falhas)
    print(f"TESTES: {len(testes) - falhos}/{len(testes)} verdes | ASSERCOES: {tot} | FALHAS: {falhos}")
    return 1 if falhos else 0


if __name__ == "__main__":
    sys.exit(main())
