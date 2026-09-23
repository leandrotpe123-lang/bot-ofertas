"""COMPLETUDE DA ENTRADA — contra o Telethon REAL.

O caso de produção (23/09, 15:00–15:11, horário de Brasília):

    @samuelf3lipepromo 117875  Kabum          chegou SÓ a edição
    @samuelf3lipepromo 117876  LISTA dos cupons DECORACAO2309 /
                               SUPERPETSHOP / CUIDADO20OFF
                               → NÃO CHEGOU: nem nova, nem edição
    @samuelf3lipepromo 117877  Magalu         chegou SÓ a edição
    @promotom          110093  SUPERPETSHOP só com /sec/  → post 23328

O post ficou no /sec/ porque a lista nunca entrou no pipeline.

O QUE ESTE ARQUIVO PROVA
  01  main.py: completude ligada antes dos handlers; os dois handlers
      observam o id antes de entrar no pipeline
  02  o caso de produção: 117877 revela o buraco, 117876 é buscada UMA
      vez e entregue ao ponto de entrada como NOVA
  03  update atrasado que chega dentro da espera: nenhuma busca
  04  primeiro id do canal não busca histórico
  05  edição de mensagem antiga não abre buraco
  06  buraco grande não é recuperado
  07  apagada e mensagem de serviço não entram no pipeline
  08  busca que falha (FloodWait real) não derruba nada e não repete
  09  canais independentes; canal não monitorado ignorado
  10  update original que chega depois da recuperação: nada é buscado
      de novo
  11  buraco aberto DURANTE uma busca também é atendido
  12  encerramento do processo: nenhuma busca
  13  memória limitada
  14  falha do ponto de entrada fica contida
  15  a mensagem recuperada atravessa a INGESTÃO real como a do handler
  16  e a ADMISSÃO real (processar): entra como nova; velha demais cai
      na trava NOVA_ANTIGA, como qualquer mensagem nova
  17  ponta a ponta pelo despacho REAL do Telethon e pelos handlers de
      main.py: as edições de 117875 e 117877 entram, a 117876 é buscada
      e entregue como nova

Execução (standalone, stdlib + Telethon; sem Telethon instalado,
apontar PYTHONPATH para a fonte, como em test_sonda_entrega.py):
    python3 tests/test_completude_entrada.py
"""
from __future__ import annotations

import ast
import asyncio
import datetime as dt
import logging
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import telethon  # noqa: E402  — REAL, antes do harness
from telethon import TelegramClient, errors, utils  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402
from telethon.tl import types  # noqa: E402

assert hasattr(telethon.TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

from _harness_e5 import preparar, RAIZ  # noqa: E402

preparar()   # telethon real preservado; só aiohttp (ausente aqui) é simulado

import globals as g  # noqa: E402
from pipeline import completude  # noqa: E402

SAMUEL_ID, PROMOTOM_ID, OUTRO_ID = 1768101197, 1825680721, 1111111111


def _canal(cid, user):
    return types.Channel(id=cid, title=user, photo=types.ChatPhotoEmpty(),
                         date=dt.datetime.now(dt.timezone.utc), access_hash=77,
                         username=user, broadcast=True)


SAMUEL = _canal(SAMUEL_ID, "samuelf3lipepromo")
PROMOTOM = _canal(PROMOTOM_ID, "promotom")
SAM, PRO, OUT = (utils.get_peer_id(SAMUEL), utils.get_peer_id(PROMOTOM),
                 utils.get_peer_id(_canal(OUTRO_ID, "outro")))

TEXTO_117876 = (
    "🔥 Cupom Mercado Livre\n\n"
    "🎟 10% OFF, Limite de R$ 20 OFF: DECORACAO2309\n"
    "👉 Lista: https://meli.la/2isJwzH\n\n\n"
    "🎟 20% OFF, Limite de R$ 40 OFF: SUPERPETSHOP, CUIDADO20OFF\n"
    "👉 Lista: https://meli.la/25Bp1DL\n\nanúncio")


class Cliente:
    """TelegramClient REAL, sem rede. A única coisa trocada é a
    resposta de `get_messages(entidade, ids=)` — a fronteira de rede
    que a completude usa. As mensagens são do Telethon real, com
    `_finish_init` feito pelo próprio cliente."""

    def __init__(self, servidor=None, erro=None, atraso=0.0):
        self.tc = TelegramClient(StringSession(), 1, "0" * 32)
        self.tc._mb_entity_cache.set_self_user(424242, False, 0)
        self.servidor = servidor or {}      # (peer, id) -> Message
        self.erro, self.atraso = erro, atraso
        self.chamadas = []
        self.tc.get_messages = self._get_messages

    async def _get_messages(self, entidade, ids):
        peer = utils.get_peer_id(entidade)
        self.chamadas.append((peer, list(ids)))
        if self.atraso:
            await asyncio.sleep(self.atraso)
        if self.erro:
            raise self.erro
        return [self.servidor.get((peer, i)) for i in ids]


def mensagem(cliente, canal, mid, texto="oferta", idade_s=30.0, servico=False):
    d = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=idade_s)
    if servico:
        m = types.MessageService(id=mid, peer_id=types.PeerChannel(canal.id), date=d,
                                 action=types.MessageActionPinMessage())
    else:
        m = types.Message(id=mid, peer_id=types.PeerChannel(canal.id), date=d,
                          message=texto, post=True)
    m._finish_init(cliente.tc, {utils.get_peer_id(canal): canal}, None)
    return m


class Entrada:
    """Ponto de entrada espião — o papel de `processar`."""
    def __init__(self, erro=None):
        self.recebidos, self.erro = [], erro

    async def __call__(self, evento, is_edit=False):
        self.recebidos.append((evento.chat_id, evento.message.id, is_edit, evento))
        if self.erro:
            raise self.erro


class _Log(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.linhas = []

    def emit(self, rec):
        self.linhas.append(rec.getMessage())


LOG = _Log()
logging.getLogger("INGESTAO").addHandler(LOG)


def ligar(cliente, entrada, fontes=(SAMUEL, PROMOTOM)):
    completude._ESPERA_S = 0.05
    completude.instalar(cliente.tc, list(fontes), entrada)
    LOG.linhas.clear()
    g._encerrando = False


async def esperar():
    for _ in range(40):
        await asyncio.sleep(0.02)
        tarefas = [c.tarefa for c in completude._canais.values()
                   if c.tarefa is not None and not c.tarefa.done()]
        if not tarefas:
            return


def rodar(corpo):
    return asyncio.run(corpo())


# ══════════════════════════════════════════════════════════════════
def t01_main_liga_e_observa(r):
    arv = ast.parse((RAIZ_P / "main.py").read_text(encoding="utf-8"))
    prep = next(n for n in ast.walk(arv)
                if isinstance(n, ast.AsyncFunctionDef) and n.name == "_preparar_processo")
    corpo = [ast.unparse(n) for n in prep.body]
    i_inst = next(i for i, s in enumerate(corpo)
                  if s == "completude.instalar(client, fontes, processar)")
    i_reg = next(i for i, s in enumerate(corpo) if s == "_registrar_handlers(fontes)")
    r.ok(i_inst == i_reg - 1, "instalar imediatamente antes de _registrar_handlers")

    reg = next(n for n in ast.walk(arv)
               if isinstance(n, ast.FunctionDef) and n.name == "_registrar_handlers")
    handlers = {n.name: n for n in ast.walk(reg) if isinstance(n, ast.AsyncFunctionDef)}
    r.ok(set(handlers) == {"on_new", "on_edit"}, f"os dois handlers da casa ({set(handlers)})")
    for nome, fn in handlers.items():
        tente = fn.body[0]
        r.ok(isinstance(tente, ast.Try), f"{nome}: corpo em try")
        primeiras = [ast.unparse(s) for s in tente.body]
        r.ok(primeiras[0] == "completude.observar(event.chat_id, event.message.id)",
             f"{nome}: observa ANTES de processar ({primeiras[0]!r})")
        r.ok(primeiras[1].startswith("await processar(event, is_edit="),
             f"{nome}: processar logo em seguida ({primeiras[1]!r})")
    r.ok(sum(1 for n in ast.walk(arv) if isinstance(n, ast.Call)
             and ast.unparse(n.func) == "completude.instalar") == 1,
         "instalar chamado UMA vez no processo")


def t02_caso_de_producao(r):
    async def corpo():
        cli = Cliente()
        cli.servidor[(SAM, 117876)] = mensagem(cli, SAMUEL, 117876, TEXTO_117876, idade_s=70)
        ent = Entrada()
        ligar(cli, ent)
        completude.observar(SAM, 117874)          # nova
        completude.observar(SAM, 117874)          # edição
        completude.observar(SAM, 117875)          # SÓ a edição chegou
        completude.observar(SAM, 117877)          # SÓ a edição chegou → buraco
        completude.observar(SAM, 117877)
        await esperar()
        return cli, ent
    cli, ent = rodar(corpo)
    r.ok(cli.chamadas == [(SAM, [117876])], f"UMA busca, só da 117876 ({cli.chamadas})")
    r.ok([(c, i, e) for c, i, e, _ in ent.recebidos] == [(SAM, 117876, False)],
         f"entregue UMA vez, como NOVA ({[(c, i, e) for c, i, e, _ in ent.recebidos]})")
    ev = ent.recebidos[0][3] if ent.recebidos else None
    r.ok(ev is not None and "SUPERPETSHOP" in ev.message.text
         and "meli.la/25Bp1DL" in ev.message.text, "conteúdo da lista intacto")
    r.ok(any("[BURACO]" in l and "117876" in l for l in LOG.linhas), "log do buraco")
    r.ok(any("[RECUPERADA]" in l and "117876" in l for l in LOG.linhas), "log da recuperação")


def t03_atrasado_chega_sozinho(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        ligar(cli, ent)
        completude.observar(SAM, 100)
        completude.observar(SAM, 102)
        completude.observar(SAM, 101)             # chegou dentro da espera
        await esperar()
        return cli, ent
    cli, ent = rodar(corpo)
    r.ok(cli.chamadas == [] and ent.recebidos == [], f"nenhuma busca ({cli.chamadas})")


def t04_primeiro_id_nao_busca_historico(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        ligar(cli, ent)
        completude.observar(SAM, 5000)
        completude.observar(PRO, 90000)
        await esperar()
        return cli
    cli = rodar(corpo)
    r.ok(cli.chamadas == [], "boot não busca o que veio antes")


def t05_edicao_antiga_nao_e_buraco(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        ligar(cli, ent)
        completude.observar(SAM, 200)
        completude.observar(SAM, 201)
        completude.observar(SAM, 150)             # edição tardia de mensagem velha
        completude.observar(SAM, 202)
        await esperar()
        return cli
    cli = rodar(corpo)
    r.ok(cli.chamadas == [], f"sem busca ({cli.chamadas})")


def t06_buraco_grande_nao_recupera(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        ligar(cli, ent)
        completude.observar(SAM, 300)
        completude.observar(SAM, 300 + completude._MAX_BURACO + 2)
        await esperar()
        grande = list(cli.chamadas)
        completude.observar(SAM, 300 + completude._MAX_BURACO + 4)   # buraco de 1 depois
        await esperar()
        return grande, cli
    grande, cli = rodar(corpo)
    r.ok(grande == [], "buraco grande: nenhuma busca")
    r.ok(any("[BURACO_GRANDE]" in l for l in LOG.linhas), "registrado como BURACO_GRANDE")
    r.ok(cli.chamadas == [(SAM, [300 + completude._MAX_BURACO + 3])],
         f"contagem segue do maior visto ({cli.chamadas})")


def t07_apagada_e_servico_nao_entram(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        cli.servidor[(SAM, 402)] = mensagem(cli, SAMUEL, 402, servico=True)
        ligar(cli, ent)
        completude.observar(SAM, 400)
        completude.observar(SAM, 403)             # 401 apagada, 402 serviço
        await esperar()
        completude.observar(SAM, 404)
        await esperar()
        return cli, ent
    cli, ent = rodar(corpo)
    r.ok(cli.chamadas == [(SAM, [401, 402])], f"uma busca só ({cli.chamadas})")
    r.ok(ent.recebidos == [], "nada entregue ao pipeline")
    r.ok(any("[AUSENTE_NO_CANAL]" in l and "401" in l for l in LOG.linhas), "apagada registrada")


def t08_busca_falha_nao_derruba(r):
    async def corpo():
        cli = Cliente(erro=errors.FloodWaitError(request=None, capture=30))
        ent = Entrada()
        ligar(cli, ent)
        completude.observar(SAM, 500)
        completude.observar(SAM, 502)
        await esperar()
        cli.erro = None
        cli.servidor[(SAM, 504)] = mensagem(cli, SAMUEL, 504)
        completude.observar(SAM, 505)             # novo buraco depois da falha
        await esperar()
        return cli, ent
    cli, ent = rodar(corpo)
    r.ok(cli.chamadas == [(SAM, [501]), (SAM, [503, 504])],
         f"501 não é repetida; o buraco seguinte é atendido ({cli.chamadas})")
    r.ok([i for _, i, _, _ in ent.recebidos] == [504], "só a 504 entra")
    r.ok(any("[RECUPERACAO_FALHOU]" in l and "FloodWaitError" in l for l in LOG.linhas),
         "falha registrada com a classe do erro")


def t09_canais_independentes(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        cli.servidor[(PRO, 11)] = mensagem(cli, PROMOTOM, 11)
        ligar(cli, ent)
        completude.observar(SAM, 10)
        completude.observar(PRO, 10)
        completude.observar(OUT, 10)
        completude.observar(OUT, 50)              # não monitorado
        completude.observar(SAM, 11)
        completude.observar(PRO, 12)
        await esperar()
        return cli, ent
    cli, ent = rodar(corpo)
    r.ok(cli.chamadas == [(PRO, [11])], f"só o buraco do promotom ({cli.chamadas})")
    r.ok([(c, i) for c, i, _, _ in ent.recebidos] == [(PRO, 11)], "entregue no canal certo")
    r.ok(OUT not in completude._canais, "canal não monitorado nem é rastreado")


def t10_original_atrasado_depois_da_recuperacao(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        cli.servidor[(SAM, 601)] = mensagem(cli, SAMUEL, 601)
        ligar(cli, ent)
        completude.observar(SAM, 600)
        completude.observar(SAM, 602)
        await esperar()
        completude.observar(SAM, 601)             # o update original chega tarde
        completude.observar(SAM, 603)
        await esperar()
        return cli, ent
    cli, ent = rodar(corpo)
    r.ok(cli.chamadas == [(SAM, [601])], f"601 buscada uma vez só ({cli.chamadas})")
    r.ok(len(ent.recebidos) == 1, "a completude entrega uma vez; o repetido é da idempotência")


def t11_buraco_durante_a_busca(r):
    async def corpo():
        cli, ent = Cliente(atraso=0.1), Entrada()
        for i in (701, 704):
            cli.servidor[(SAM, i)] = mensagem(cli, SAMUEL, i)
        ligar(cli, ent)
        completude.observar(SAM, 700)
        completude.observar(SAM, 702)
        await asyncio.sleep(0.08)                 # a busca da 701 está em voo
        completude.observar(SAM, 703)
        completude.observar(SAM, 705)             # novo buraco: 704
        await esperar()
        await asyncio.sleep(0.3)
        return cli, ent
    cli, ent = rodar(corpo)
    r.ok(cli.chamadas == [(SAM, [701]), (SAM, [704])], f"as duas buscas ({cli.chamadas})")
    r.ok(sorted(i for _, i, _, _ in ent.recebidos) == [701, 704], "as duas entregues")


def t12_encerramento_nao_busca(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        ligar(cli, ent)
        completude.observar(SAM, 800)
        completude.observar(SAM, 802)
        g._encerrando = True
        await esperar()
        g._encerrando = False
        return cli
    cli = rodar(corpo)
    r.ok(cli.chamadas == [], "processo encerrando: nenhuma requisição")


def t13_memoria_limitada(r):
    async def corpo():
        cli, ent = Cliente(), Entrada()
        ligar(cli, ent)
        for i in range(1, 5001):
            completude.observar(SAM, i)
        return completude._canais[SAM]
    canal = rodar(corpo)
    r.ok(len(canal.vistos) <= 2 * completude._JANELA_IDS + 1, f"vistos={len(canal.vistos)}")
    r.ok(min(canal.vistos) >= canal.maior - 2 * completude._JANELA_IDS,
         "só a janela recente é lembrada")
    r.ok(canal.maior == 5000 and not canal.pendentes, "estado íntegro")


def t14_falha_do_ponto_de_entrada_contida(r):
    async def corpo():
        cli, ent = Cliente(), Entrada(erro=RuntimeError("pipeline caiu"))
        for i in (901, 903):
            cli.servidor[(SAM, i)] = mensagem(cli, SAMUEL, i)
        ligar(cli, ent)
        completude.observar(SAM, 900)
        completude.observar(SAM, 902)
        await esperar()
        completude.observar(SAM, 904)
        await esperar()
        return ent
    ent = rodar(corpo)
    r.ok([i for _, i, _, _ in ent.recebidos] == [901, 903], "segue entregando depois da falha")
    r.ok(all(isinstance(completude.observar(SAM, 905), type(None)) for _ in (0,)),
         "observar nunca levanta")


def t15_ingestao_real(r):
    from pipeline.ingestao import ingerir
    from pipeline import identidade

    async def corpo():
        cli = Cliente()
        m = mensagem(cli, SAMUEL, 117876, TEXTO_117876)
        identidade._CHAT_USERNAME.pop(SAM, None)
        return await ingerir(completude.EventoRecuperado(m))
    bruta = rodar(corpo)
    r.ok(bruta.msg_id == 117876 and bruta.chat == str(SAM), f"id e chat canônico ({bruta.chat})")
    r.ok(bruta.links == ["https://meli.la/2isJwzH", "https://meli.la/25Bp1DL"],
         f"as duas listas ({bruta.links})")
    r.ok("SUPERPETSHOP" in bruta.texto and not bruta.tem_midia and not bruta.is_reply,
         "texto e flags")
    from pipeline.identidade import username_de
    r.ok(username_de(str(SAM)) == "samuelf3lipepromo", "get_chat do evento resolve o canal")


def t16_admissao_real(r):
    from pipeline import orchestrator
    admitidos = []

    async def espiao(ev, is_edit):
        admitidos.append((ev.message.id, is_edit))

    original = orchestrator._enfileirar
    orchestrator._enfileirar = espiao
    try:
        async def corpo():
            cli = Cliente()
            cli.servidor[(SAM, 1001)] = mensagem(cli, SAMUEL, 1001, idade_s=70)
            cli.servidor[(SAM, 1003)] = mensagem(cli, SAMUEL, 1003, idade_s=400)
            ligar(cli, orchestrator.processar)
            completude.observar(SAM, 1000)
            completude.observar(SAM, 1002)
            completude.observar(SAM, 1004)
            await esperar()
        rodar(corpo)
    finally:
        orchestrator._enfileirar = original
    r.ok(admitidos == [(1001, False)], f"recente admitida como NOVA; velha barrada ({admitidos})")


def t17_despacho_real_pelos_handlers_da_casa(r):
    """Ponta a ponta com o despacho REAL do Telethon: os handlers de
    main.py recebem as EDIÇÕES da 117875 e da 117877 (como em produção),
    a completude percebe a 117876 e a entrega ao ponto de entrada."""
    import types as _t
    try:
        import yarl  # noqa: F401
    except ImportError:
        _y = _t.ModuleType("yarl")
        _y.URL = str
        sys.modules["yarl"] = _y
    import main

    recebidos = []

    async def processar_espiao(ev, is_edit=False):
        recebidos.append((ev.message.id, is_edit, type(ev).__name__))

    def editada(cli, mid, texto):
        agora = dt.datetime.now(dt.timezone.utc)
        m = types.Message(id=mid, peer_id=types.PeerChannel(SAMUEL_ID),
                          date=agora - dt.timedelta(seconds=4), message=texto,
                          post=True, edit_date=agora)
        u = types.UpdateEditChannelMessage(message=m, pts=1, pts_count=1)
        u._entities = {SAM: SAMUEL}
        return u

    orig_cli, orig_proc = main.client, main.processar

    async def corpo():
        cli = Cliente()
        cli.servidor[(SAM, 117876)] = mensagem(cli, SAMUEL, 117876, TEXTO_117876, idade_s=70)
        main.client, main.processar = cli.tc, processar_espiao
        completude._ESPERA_S = 0.05
        g._encerrando = False
        completude.instalar(cli.tc, [SAMUEL], processar_espiao)
        main._registrar_handlers([SAMUEL])
        await cli.tc._dispatch_update(editada(cli, 117875, "kabum"))
        await cli.tc._dispatch_update(editada(cli, 117877, "magalu"))
        await esperar()
        return cli
    try:
        cli = rodar(corpo)
    finally:
        main.client, main.processar = orig_cli, orig_proc
    r.ok(cli.chamadas == [(SAM, [117876])], f"busca da 117876 ({cli.chamadas})")
    r.ok(recebidos == [(117875, True, "Event"), (117877, True, "Event"),
                       (117876, False, "EventoRecuperado")],
         f"edições pelos handlers; a perdida entra como NOVA ({recebidos})")


# ── runner ────────────────────────────────────────────────────────
import pathlib  # noqa: E402
RAIZ_P = pathlib.Path(RAIZ)


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
