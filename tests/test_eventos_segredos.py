"""
F1.2-A1 — SEGREDOS E URLs: nenhum evento emitido pela A1 contém URL em
claro. A barreira é fail-closed (coleta.mascarar_urls, antes de todo
reg.emitir): nenhuma URL com caminho, query, fragmento, credencial ou
identificador de afiliado sai em claro, conheça o classificador o domínio
ou não. Host sozinho é metadado permitido.

  01  classificador — só metadado: motivo de URL limpa, afiliada, com
      parâmetro, código curto, credencial, IP, longa ou inválida
  02  barreira em texto: pedaço com caminho, query, fragmento, credencial,
      "=" ou "%" vira a máscara, inclusive URL que o classificador acha
      limpa; pontuação final fica; texto comum e host sozinho intactos;
      idempotente; as barras Unicode são as do banco Unicode
  03  canários em todo lugar (valor, chave, lista, aninhado, corr, objeto
      do Telethon, bytes, exceção, URL partida pelo corte) → nada sensível
      no anel; chave plataforma|url|<url> vira plataforma|url|h12
  04  ambiente com valores-canário → o pacote não os lê nem os emite
  05  representar_url: sempre {host, h12, motivo, plataforma}, NUNCA a URL
  06  h12: algoritmo, entrada e falta de normalização; impressão digital
      probabilística (colisão possível), não segredo nem segurança; a
      chave usa o h12 da URL ORIGINAL (antes do corte e da barreira),
      igual ao do link
  07  INVARIANTE: cada forma (com e sem esquema, "limpa" para o
      classificador, encurtador, markdown, duas URLs juntas, colada em
      texto, parênteses, query, fragmento, @, =, %, barra fullwidth e de
      outro alfabeto) em valor, chave, lista, estrutura aninhada, corr e
      chave plataforma|url| → nenhum evento do anel tem canário, "://" ou
      pedaço com separador; metadados estruturais (chat, msg, midia_key,
      datas, ids) saem intactos
  08  previa (≤ 200, espaços colapsados, barreira) e origem.recebida
      canônico: previa, texto_h12, texto_len, links e metadados, sem o
      texto integral
  09  sorteio determinístico de textos com URLs coladas por qualquer
      separador (nunca levanta, sem canário, idempotente) e custo linear
      em texto patológico de 16 KiB

    python tests/test_eventos_segredos.py
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import sys
import time
import unicodedata

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar  # noqa: E402
preparar()

import eventos                                   # noqa: E402
from eventos import catalogo, coleta             # noqa: E402

CANARIO = "canario"
M = coleta._MASCARA

LIMPAS = [
    "https://www.amazon.com.br/dp/B0ABCDEF12",
    "https://shopee.com.br/Produto-Legal-i.123456.789",
    "https://produto.mercadolivre.com.br/MLB-1234567890-produto-legal-_JM",
    "https://www.magazineluiza.com.br/produto/p/abc123/te/tvsm",
    "https://t.me/canal",
    "https://www.netshoes.com.br/tenis-corrida",
]

PROTEGIDAS = {
    "https://www.amazon.com.br/dp/B0ABCDEF12?tag=canario-20": "PARAMETROS",
    "https://www.amazon.com.br/dp/B0ABCDEF12/ref=as_li_canario": "PARAMETROS",
    "https://amzn.to/CanArio9": "AFILIADA",
    "amzn.to/CanArio9": "AFILIADA",
    "AMZN.TO/CanArio9": "AFILIADA",
    "https://s.shopee.com.br/CANARIO1": "AFILIADA",
    "https://mercadolivre.com/sec/1canario": "AFILIADA",
    "https://www.magazinevoce.com.br/magazinecanario/p/123": "AFILIADA",
    "https://bit.ly/3CaNaRi0": "AFILIADA",
    "https://loja.com.br/r/canario": "AFILIADA",
    "https://loja.com.br/aff/canario": "AFILIADA",
    "https://www.leoind.com.br/CanAr10": "CODIGO",
    "https://t.me/+CanArioInvite": "CODIGO",
    "www.loja.com/x?aff_id=CANARIO": "PARAMETROS",
    "https://loja.com.br/p#canario": "PARAMETROS",
    "https://loja.com.br/caf%C3%A9canario": "PARAMETROS",
    "https://user:canario@host.com/x": "CREDENCIAL",
    "http://192.168.0.10/canario": "INVALIDA",
    "https://[::1]/canario": "INVALIDA",
    "http://localhost:8080/canario": "INVALIDA",
    "http://intranet/canario": "INVALIDA",
    "http://worker.railway.internal:8080/v1/eventos?c=canario": "INVALIDA",
    "http://worker.railway.internal:8080/canario": "INVALIDA",
    "https://loja.com.br/" + "canario" * 80: "LONGA",
    "https://loja.com:99999/canario": "INVALIDA",
}

# A especificação da barreira, escrita aqui de novo, independente do
# código: pedaço entre espaços que, sem a pontuação final, tenha separador
# de URL — também na forma NFKC — ou barra de outro alfabeto.
SEPARADORES = "/?#@=%" + chr(92)
FINAL = '.,;:!?%)]}"' + "'" + chr(0x2026) + chr(0xBB) + chr(0x201D) + chr(0x2019)
BARRAS = frozenset(c for c in map(chr, range(0x80, 0x110000))
                   if "SOLIDUS" in unicodedata.name(c, "") or "SLASH" in unicodedata.name(c, ""))


def _anel():
    eventos.desligar()
    return eventos.instalar(20000, 64 << 20)


def _bruto(reg) -> bytes:
    return b"".join(reg.ler(None, 100000, 1 << 30)["cargas"])


def _eventos(reg) -> list:
    return [json.loads(c) for c in reg.ler(None, 100000, 1 << 30)["cargas"]]


def _proibidos(bruto: bytes) -> list:
    baixo = bruto.lower()
    return [p for p in (CANARIO.encode(), b"access_hash", b"file_reference", b"tag=",
                        b"aff_id", b"senha", b"://") if p in baixo]


def _strings(x):
    """Toda string de um evento: chave e valor, em qualquer nível."""
    if type(x) is str:
        yield x
    elif type(x) is dict:
        for k, v in x.items():
            yield k
            yield from _strings(v)
    elif type(x) is list:
        for v in x:
            yield from _strings(v)


def _pedacos_com_url(s: str) -> list:
    """Pedaços que ainda poderiam levar caminho, query, fragmento ou
    credencial de URL — no que sai da coleta, sempre vazio."""
    ruins = []
    for p in s.split():
        n = p.rstrip(FINAL)
        if not BARRAS.isdisjoint(n) or any(c in unicodedata.normalize("NFKC", n)
                                           for c in SEPARADORES):
            ruins.append(p)
    return ruins


def _violacoes(reg) -> list:
    """Pedaço com URL possível em QUALQUER evento do anel (tipo, corr, dados)."""
    achados = []
    for ev in _eventos(reg):
        for s in [ev["tipo"], *_strings(ev.get("corr")), *_strings(ev.get("dados"))]:
            achados.extend(_pedacos_com_url(s))
    return achados


def test_01_classificador_so_metadado(r):
    limpas_erradas = [u for u in LIMPAS if coleta._classificar(u)[0] is not None]
    r.check(limpas_erradas == [], "01.limpas", str(limpas_erradas))
    erradas = [(u, coleta._classificar(u)[0], m) for u, m in PROTEGIDAS.items()
               if coleta._classificar(u)[0] != m]
    r.check(erradas == [], "01.protegidas_com_motivo", str(erradas))
    r.check({m for m in PROTEGIDAS.values()} <= catalogo.ENUMS["motivo_url_protegida"],
            "01.motivos_do_catalogo")


TEXTO_COMUM = ["Sr.Fulano pagou R$1.299,90", "50% OFF hoje", "Quer economizar?",
               "C&A e P&G", "às 14:30", "Cupom: PROMO10!", "versão 1.2.3.4",
               "[10:30:15] localhost", "loja.com.br e amzn.to",
               "texto" + chr(0x3002) + "texto", "Oferta 🔥 imperdível" + chr(0x2026)]


def test_02_barreira_em_texto(r):
    m = coleta.mascarar_urls
    r.check(all(m(f"veja {u} agora") == f"veja {M} agora" for u in LIMPAS),
            "02.limpa_para_o_classificador_tambem_mascarada")
    saida = m("Oferta! https://amzn.to/CanArio9, veja www.loja.com/x?aff_id=CANARIO. "
              "[clique](https://amzn.to/CanArio9)")
    r.check(saida == f"Oferta! {M}, veja {M}. {M})", "02.pontuacao_final_fica", saida)
    r.check(CANARIO not in saida.lower() and "aff_id" not in saida, "02.sem_canario", saida)
    mudou = [(t, m(t)) for t in TEXTO_COMUM if m(t) != t]
    r.check(mudou == [], "02.texto_comum_e_host_sozinho_intactos", str(mudou))
    r.check(m(saida) == saida and m("") == "", "02.idempotente")
    r.check(coleta._BARRAS_UNICODE == BARRAS and len(BARRAS) == 45,
            "02.barras_unicode_do_banco_unicode", str(len(BARRAS)))


class _MensagemTelethon:
    """Imita o que um Message do Telethon mostra se impresso."""
    id = 110093
    access_hash = 8811223344556677
    file_reference = b"canario-file-ref"

    def __repr__(self):
        return (f"Message(id=110093, access_hash={self.access_hash}, "
                f"file_reference={self.file_reference!r}, texto='canario')")
    __str__ = __repr__


def test_03_canarios_em_todo_lugar(r):
    reg = _anel()
    urls = list(PROTEGIDAS)
    texto_longo = "x" * 4080 + " https://amzn.to/CanArio9 fim"     # partida pelo corte
    dados = {
        "texto": " ".join(urls) + " e " + " ".join(LIMPAS),
        "lista": urls,
        "aninhado": {"a": {"b": {"c": urls[:3]}}},
        "chave_url": {urls[2]: "valor", "amazon|url|" + urls[0]: 1},
        "ofertas": ["amazon|url|" + urls[0], "amazon|pid|B0ABCDEF12"],
        "mensagem": _MensagemTelethon(),
        "bytes": b"canario-bytes",
        "longo": texto_longo,
    }
    eventos.emitir_de("origem.recebida", lambda: ({"chat": "-100", "link": urls[3]}, dados),
                      local="t.03")
    eventos.emitir_de("origem.recebida", lambda: (_ for _ in ()).throw(
        RuntimeError("canario https://amzn.to/CanArio9")), local="t.03.erro",
        minimo=lambda: {"origem": urls[0]})
    with eventos.execucao(lambda: {"url": urls[0], "msg": 1}):
        eventos.emitir_de("post.publicado", lambda: ({}, {"t": urls[1]}), local="t.03.exec")
    bruto = _bruto(reg)
    r.check(_proibidos(bruto) == [] and _violacoes(reg) == [], "03.nada_sensivel_no_anel",
            str((_proibidos(bruto), _violacoes(reg)[:5])))
    r.check(all(u.encode() not in bruto for u in LIMPAS), "03.limpas_tambem_fora")
    d = _eventos(reg)[0]["dados"]
    r.check(d["mensagem"] == "<objeto:_MensagemTelethon>" and d["bytes"] == "<bytes:13>",
            "03.objeto_e_bytes_sem_conteudo", str((d["mensagem"], d["bytes"])))
    chave = "amazon|url|" + coleta.h12(urls[0])
    r.check(d["ofertas"] == [chave, "amazon|pid|B0ABCDEF12"] and chave in d["chave_url"],
            "03.chave_de_url_vira_h12", str((d["ofertas"], list(d["chave_url"]))))
    r.check(eventos.saude_coleta()["urls_protegidas"] > 0, "03.contador")


def test_04_ambiente_com_canarios(r):
    nomes = ("TELEGRAM_SESSION", "API_PRIVADA_SEGREDO", "ML_COOKIES", "SHOPEE_SECRET",
             "AMAZON_SECRET_KEY")
    antes = {n: os.environ.get(n) for n in nomes}
    try:
        for n in nomes:
            os.environ[n] = f"{CANARIO}-{n.lower()}-valor"
        reg = _anel()
        eventos.emitir_de("origem.recebida", lambda: ({"msg": 1}, {"t": "oi"}), local="t.04")
        with eventos.execucao(lambda: {"msg": 2}):
            pass
        extra = json.dumps([eventos.saude_coleta(), eventos.resumo_catalogo()]).encode()
        r.check(_proibidos(_bruto(reg) + extra) == [], "04.ambiente_nunca_entra")
    finally:
        for n, v in antes.items():
            if v is None:
                os.environ.pop(n, None)
            else:
                os.environ[n] = v
    fontes = "".join(open(os.path.join(os.path.dirname(eventos.__file__), a),
                          encoding="utf-8").read() for a in ("coleta.py", "catalogo.py"))
    r.check("environ" not in fontes and "getenv" not in fontes, "04.pacote_nao_le_ambiente")


def test_05_representar_url(r):
    campos = {"host", "h12", "motivo", "plataforma"}
    for u in LIMPAS + list(PROTEGIDAS):
        rep = coleta.representar_url(u, plataforma="amazon")
        txt = json.dumps(rep, ensure_ascii=False)
        r.check(set(rep) == campos and rep["h12"] == coleta.h12(u.strip())
                and rep["motivo"] == PROTEGIDAS.get(u) and rep["plataforma"] == "amazon"
                and u not in txt and CANARIO not in txt.lower() and not _pedacos_com_url(txt),
                f"05.{u[:30]}", str(rep))
    r.check(coleta.representar_url("https://loja.com/x", plataforma="x/y")["plataforma"] is None
            and coleta.representar_url("https://loja.com/x", plataforma="Amazon")["plataforma"] is None
            and coleta.representar_url("https://loja.com/x")["plataforma"] is None,
            "05.plataforma_validada")
    r.check(coleta.representar_url(None) == {"host": None, "h12": None, "motivo": "INVALIDA",
                                             "plataforma": None}, "05.nao_texto")


def test_06_h12(r):
    def ref(s):
        return hashlib.sha256(s.encode("utf-8", "surrogatepass")).hexdigest()[:12]
    amostras = ["https://amzn.to/CanArio9", "", "a" + chr(0xD800) + "b", "é", "🔥 texto"]
    r.check(all(coleta.h12(s) == ref(s) and len(coleta.h12(s)) == 12
                and set(coleta.h12(s)) <= set("0123456789abcdef") for s in amostras),
            "06.algoritmo_sha256_utf8_12_hex")
    nfc, nfd = unicodedata.normalize("NFC", "é"), unicodedata.normalize("NFD", "é")
    r.check(coleta.h12("A") != coleta.h12("a") and coleta.h12(" x") != coleta.h12("x")
            and coleta.h12(nfc) != coleta.h12(nfd), "06.sem_normalizacao")
    r.check(coleta.representar_url("  https://amzn.to/CanArio9  ")["h12"]
            == coleta.h12("https://amzn.to/CanArio9"), "06.url_sem_os_espacos_das_pontas")
    # A chave usa o h12 da URL ORIGINAL, calculado na captura — antes do
    # corte (chave longa) e da barreira — e bate com o h12 do link.
    u = "https://www.amazon.com.br/dp/B0ABCDEF12?tag=canario-20"
    longa = "https://www.loja.com.br/" + "p" * 5000 + "?tag=canario"
    reg = _anel()
    eventos.emitir_de("origem.recebida", lambda: ({"msg": 1}, {
        "ofertas": ["amazon|url|" + u, "loja|url|" + longa], "amazon|url|" + u: 1,
        "link": eventos.representar_url(u)}), local="t.06")
    d = _eventos(reg)[-1]["dados"]
    r.check(d.get("ofertas") == ["amazon|url|" + coleta.h12(u), "loja|url|" + coleta.h12(longa)]
            and ("amazon|url|" + coleta.h12(u)) in d
            and d.get("link", {}).get("h12") == coleta.h12(u),
            "06.chave_com_h12_da_url_original", str(d)[:300])
    doc = (coleta.__doc__ or "") + (coleta.h12.__doc__ or "")
    r.check("IMPRESSÃO DIGITAL" in doc and "colisão é possível" in doc.lower()
            and "NÃO é segredo" in doc and "mecanismo de segurança" in doc, "06.documentado")


FORMAS = {
    "com_esquema": "https://www.amazon.com.br/dp/B0ABCDEF12?tag=canario-20",
    "limpa_para_o_classificador": "https://www.netshoes.com.br/canario-tenis",
    "sem_esquema": "www.netshoes.com.br/canario-tenis",
    "encurtador": "amzn.to/CanArio9",
    "encurtador_com_esquema": "https://bit.ly/canario9",
    "markdown": "[oferta](https://loja.com.br/canario)",
    "markdown_url_no_texto": "[loja.com.br/canario](loja.com.br/canario)",
    "duas_juntas": "https://a.co/canario1https://b.co/canario2",
    "duas_com_virgula": "loja.com/canario1,loja.com/canario2",
    "colada_em_texto": "Oferta!amzn.to/canario🔥",
    "colada_em_dois_pontos": "Link:https://loja.com/canario",
    "parenteses": "(https://loja.com.br/canario)",
    "parenteses_depois": "loja.com/canario(oferta)",
    "query": "loja.com?ref=canario",
    "fragmento": "loja.com#canario",
    "credencial": "user:canario@host.com/x",
    "arroba": "canario@host.com",
    "parametro_solto": "tag=canario-20",
    "url_codificada": "https%3A%2F%2Famzn.to%2Fcanario",
    "barra_fullwidth": "amzn.to" + chr(0xFF0F) + "canario9",
    "barra_de_divisao": "loja.com" + chr(0x2215) + "canario",
    "barra_de_fracao": "loja.com" + chr(0x2044) + "canario",
    "barra_invertida_fullwidth": "loja.com" + chr(0xFF3C) + "canario",
    "interrogacao_fullwidth": "loja.com" + chr(0xFF1F) + "ref" + chr(0xFF1D) + "canario",
    "arroba_fullwidth": "canario" + chr(0xFF20) + "host.com",
}


def test_07_invariante_nenhuma_url_em_claro(r):
    r.check(coleta._classificar(FORMAS["limpa_para_o_classificador"])[0] is None,
            "07.o_classificador_acha_limpa")
    reg = _anel()
    for nome, f in FORMAS.items():
        dados = {
            "frase": f"veja {f} agora",
            "valor": f,
            f: "como chave",
            "lista": ["antes", f, "depois"],
            "aninhado": {"n1": {"n2": [f, {"n3": f}]}},
            "ofertas": ["amazon|url|" + f],
        }
        eventos.emitir_de("origem.recebida", lambda d=dados, f=f: ({"chat": -100, "ref": f}, d),
                          local="t.07." + nome)
    ev = _eventos(reg)
    r.check(len(ev) == len(FORMAS), "07.um_evento_por_forma", str(len(ev)))
    r.check(_proibidos(_bruto(reg)) == [], "07.nenhum_canario_nem_esquema",
            str(_proibidos(_bruto(reg))))
    r.check(_violacoes(reg) == [], "07.nenhum_pedaco_com_separador", str(_violacoes(reg)[:5]))
    erradas = [n for (n, f), e in zip(FORMAS.items(), ev)
               if e["dados"].get("ofertas") != ["amazon|url|" + coleta.h12(f)]]
    r.check(erradas == [], "07.chave_de_url_por_h12", str(erradas))
    # o que é metadado estrutural sai intacto
    reg = _anel()
    corr = {"chat": -1001825680721, "msg": 110093}
    meta = {"chat_txt": "-1001825680721", "midia_key": "photo:5012345678901234567",
            "doc_key": "doc:42", "date": "2026-10-06T22:15:00+00:00", "edit_date": 1791324900,
            "post": 70021, "grouped_id": 13579, "host": "www.amazon.com.br",
            "h12": coleta.h12("x"), "plataforma": "amazon", "previa": "Fone R$ 129,90 50% OFF",
            "ofertas": ["amazon|pid|B0ABCDEF12", "shopee|item|123.456", "ml|mlb|MLB1234567890",
                        "amazon|url|" + coleta.h12("https://amzn.to/x")]}
    eventos.emitir_de("origem.recebida", lambda: (corr, meta), local="t.07.meta")
    e = _eventos(reg)[-1]
    r.check({k: e["dados"].get(k) for k in meta} == meta and e["corr"].get("chat") == corr["chat"]
            and e["corr"].get("msg") == corr["msg"], "07.metadados_estruturais_intactos",
            str(e)[:400])


def test_08_previa_e_origem_recebida(r):
    t = ("🔥 OFERTA   Fone Bluetooth XYZ por R$ 129,90\nhttps://amzn.to/CanArio9\n"
         "[veja](https://www.netshoes.com.br/canario-tenis) Cupom: PROMO10 válido hoje!\n") * 6
    p = eventos.previa(t)
    r.check(0 < len(p) <= 200 and p.endswith(coleta._MARCA) and "\n" not in p and "  " not in p,
            "08.previa_curta_e_colapsada", repr(p))
    r.check(CANARIO not in p.lower() and "://" not in p and M in p and not _pedacos_com_url(p),
            "08.previa_com_barreira", p)
    r.check(eventos.previa(p) == p and eventos.previa(None) == "" and eventos.previa("") == ""
            and eventos.previa("  curta\n e limpa ") == "curta e limpa", "08.previa_bordas")
    links = [("https://amzn.to/CanArio9", "amazon"),
             ("https://www.netshoes.com.br/canario-tenis", "netshoes")]
    reg = _anel()
    eventos.emitir_de("origem.recebida", lambda: (
        {"chat": -1001825680721, "msg": 110093},
        {"via": "TELEGRAM", "is_edit": False, "date": "2026-10-06T22:15:00+00:00",
         "midia": "photo", "midia_key": "photo:5012345678901234567",
         "previa": eventos.previa(t), "texto_h12": eventos.h12(t), "texto_len": len(t),
         "links": [eventos.representar_url(u, plataforma=pl) for u, pl in links]}),
        local="t.08")
    ev = _eventos(reg)[-1]
    d = ev["dados"]
    r.check("texto" not in d and d.get("previa") == p and d.get("texto_h12") == coleta.h12(t)
            and d.get("texto_len") == len(t), "08.sem_texto_integral", str(d)[:300])
    r.check(d.get("links") == [
        {"host": "amzn.to", "h12": coleta.h12(links[0][0]), "motivo": "AFILIADA",
         "plataforma": "amazon"},
        {"host": "www.netshoes.com.br", "h12": coleta.h12(links[1][0]), "motivo": None,
         "plataforma": "netshoes"}], "08.links_por_referencia", str(d.get("links")))
    r.check(ev["corr"].get("chat") == -1001825680721 and ev["corr"].get("msg") == 110093
            and d.get("midia_key") == "photo:5012345678901234567"
            and d.get("date") == "2026-10-06T22:15:00+00:00", "08.metadados_intactos")
    r.check(_proibidos(_bruto(reg)) == [] and _violacoes(reg) == [], "08.nada_em_claro")


# Colas entre pedaços: espaço, pontuação, emoji, markdown — e nada ("").
_COLAS = [" ", "", "\n", "\t", chr(0x201C), chr(0x201D), '"', "'", "<", ">", "🔥", "|",
          chr(0x2026), "](", " - ", ": ", ", ", ". ", "! ", "? ", "(", ")", "[", "]", "*",
          "_", "~", ".", ",", ":", "-"]
_LIMPOS = ["Sr.Fulano", "R$1.299,90", "Oferta", "50%", "Quer?", "C&A", "amzn.to",
           "loja.com.br", "🔥"]
_PATOLOGICOS = ["a:", "a.a.", "[", "[::", "@", "a@", "1.1.1.", "localhost:", "a", "-", "](",
                chr(0x1F600), "x.co/](", "u:p@", "a.co/x", "ab.cd ", "[a:b:c", "_a.b_",
                "a:b@c.de/", "a" + chr(0x3002), "x:" + "y" * 100 + " ", "1.2.3.4/", "[::1]",
                "a_", "a.b_c.d_", chr(0x201C) + "a.b/C1" + chr(0x201D), "a.b/c](", "a:a@",
                "::@", "a.b" + chr(0x2026), "/ ", "a/ ", chr(0xFF0F), "x" + chr(0x2215) + " ",
                "é/ ", "=%"]


def _cronometrar(f, x) -> float:
    t0 = time.perf_counter()
    f(x)
    return time.perf_counter() - t0


def test_09_sorteio_e_custo_linear(r):
    rng = random.Random(20261007)
    pecas = list(FORMAS.values()) + _LIMPOS
    ruins = []
    for _ in range(3000):
        partes = []
        for _ in range(rng.randrange(1, 6)):
            partes.append(rng.choice(_COLAS))
            partes.append(rng.choice(pecas))
        partes.append(rng.choice(_COLAS))
        t = "".join(partes)
        try:
            m = coleta.mascarar_urls(t)
        except Exception as e:                         # noqa: BLE001
            ruins.append((t, repr(e)))
            continue
        if (CANARIO in m.lower() or "://" in m or _pedacos_com_url(m)
                or coleta.mascarar_urls(m) != m):
            ruins.append((t, m))
    r.check(ruins == [], "09.sorteio_sem_fuga_e_idempotente", f"{len(ruins)}: {ruins[:3]}")
    lentos = []
    for bloco in _PATOLOGICOS:
        t = (bloco * 16384)[:16384]
        melhor = min(_cronometrar(coleta.mascarar_urls, t) for _ in range(2))
        if melhor > 0.25:
            lentos.append((bloco[:12], round(melhor * 1000)))
    r.check(lentos == [], "09.custo_linear_16KiB", f"ms: {lentos}")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "EVENTOS · segredos e URLs fora dos eventos · F1.2-A1"))
