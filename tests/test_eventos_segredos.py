"""
F1.2-A1 — SEGREDOS E URLs: nada sensível entra num evento.

  01  classificação: URL limpa vai em claro; afiliada, com parâmetro,
      código curto, credencial, IP, longa ou inválida → protegida
  02  máscara em texto: ⟨url:host:h12⟩ no lugar; pontuação final fica;
      h12 é o sha256 truncado da URL exata; idempotente; sem esquema e
      dentro de link markdown também; texto comum intacto
  03  canários em TODO lugar (valor, chave, lista, aninhado, corr, chave
      de identidade …|url|…, objeto do Telethon, bytes, exceção, URL
      partida pelo corte de tamanho) → nenhum canário, access_hash,
      file_reference, tag= ou aff_id no anel; URLs limpas seguem legíveis
  04  ambiente com valores-canário de sessão, segredo e cookie → o pacote
      não os lê nem os emite (eventos, saúde da coleta, catálogo)
  05  representar_url: {url} só se pode ir em claro; senão
      {h12, host, motivo}, sem a URL
  06  h12: 12 hexadecimais, estável, igualdade — e documentado como
      pseudônimo, não segredo nem mecanismo de segurança

    python tests/test_eventos_segredos.py
"""
from __future__ import annotations

import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar  # noqa: E402
preparar()

import eventos                                   # noqa: E402
from eventos import catalogo, coleta             # noqa: E402

CANARIO = "canario"

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


def _anel():
    eventos.desligar()
    return eventos.instalar(20000, 64 << 20)


def _bruto(reg) -> bytes:
    return b"".join(reg.ler(None, 100000, 1 << 30)["cargas"])


def _proibidos(bruto: bytes) -> list:
    baixo = bruto.lower()
    return [p for p in (CANARIO.encode(), b"access_hash", b"file_reference", b"tag=",
                        b"aff_id", b"senha") if p in baixo]


def test_01_classificacao(r):
    limpas_erradas = [u for u in LIMPAS if coleta._classificar(u)[0] is not None]
    r.check(limpas_erradas == [], "01.limpas_em_claro", str(limpas_erradas))
    erradas = [(u, coleta._classificar(u)[0], m) for u, m in PROTEGIDAS.items()
               if coleta._classificar(u)[0] != m]
    r.check(erradas == [], "01.protegidas_com_motivo", str(erradas))
    r.check({m for m in PROTEGIDAS.values()} <= catalogo.ENUMS["motivo_url_protegida"],
            "01.motivos_do_catalogo")


def test_02_mascara_em_texto(r):
    u = "https://amzn.to/CanArio9"
    texto = (f"Oferta! {u}, veja www.loja.com/x?aff_id=CANARIO. Também "
             f"[clique]({u}) e {LIMPAS[0]}. Sr.Fulano pagou R$1.299,90 em amzn.to/CanArio9)")
    m = coleta.mascarar_urls(texto)
    esperado = f"⟨url:amzn.to:{hashlib.sha256(u.encode()).hexdigest()[:12]}⟩"
    r.check(f"! {esperado}," in m and f"]({esperado})" in m, "02.token_e_pontuacao", m)
    r.check(CANARIO not in m.lower() and "aff_id" not in m, "02.sem_canario", m)
    r.check(LIMPAS[0] in m and "Sr.Fulano" in m and "R$1.299,90" in m,
            "02.limpa_e_texto_comum_intactos", m)
    r.check(m.endswith("⟩)") and coleta.mascarar_urls(m) == m, "02.idempotente")
    r.check(coleta.mascarar_urls("sem url nenhuma") == "sem url nenhuma"
            and coleta.mascarar_urls("") == "", "02.sem_url_sem_mudanca")


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
    r.check(_proibidos(bruto) == [], "03.nada_sensivel_no_anel", str(_proibidos(bruto)))
    ev = [json.loads(c) for c in reg.ler(None, 100000, 1 << 30)["cargas"]]
    d = ev[0]["dados"]
    r.check(all(x in d["texto"] for x in LIMPAS), "03.limpas_legiveis")
    r.check(d["mensagem"] == "<objeto:_MensagemTelethon>" and d["bytes"] == "<bytes:13>",
            "03.objeto_e_bytes_sem_conteudo", str((d["mensagem"], d["bytes"])))
    r.check("amazon|pid|B0ABCDEF12" in d["ofertas"]
            and d["ofertas"][0].startswith("amazon|url|⟨url:www.amazon.com.br:"),
            "03.chave_de_identidade_mascarada", str(d["ofertas"]))
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
    for u in LIMPAS:
        r.check(coleta.representar_url(u) == {"url": u}, f"05.limpa.{u[:30]}")
    for u, motivo in PROTEGIDAS.items():
        rep = coleta.representar_url(u)
        r.check(set(rep) == {"h12", "host", "motivo"} and rep["motivo"] == motivo
                and rep["h12"] == coleta.h12(u.strip()) and CANARIO not in json.dumps(rep).lower(),
                f"05.protegida.{u[:30]}", str(rep))
    r.check(coleta.representar_url(None) == {"motivo": "INVALIDA"}, "05.nao_texto")


def test_06_h12(r):
    u = "https://amzn.to/CanArio9"
    h = coleta.h12(u)
    r.check(len(h) == 12 and all(c in "0123456789abcdef" for c in h), "06.formato")
    r.check(h == coleta.h12(u) and h != coleta.h12(u + "x")
            and h == hashlib.sha256(u.encode()).hexdigest()[:12], "06.estavel_e_igualdade")
    r.check(coleta.h12("a\ud800b") == coleta.h12("a\ud800b"), "06.surrogate")
    doc = (coleta.__doc__ or "") + (coleta.h12.__doc__ or "")
    r.check("PSEUDÔNIMO" in doc and "NÃO é segredo" in doc and "mecanismo" in doc,
            "06.documentado_como_pseudonimo")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "EVENTOS · segredos e URLs fora dos eventos · F1.2-A1"))
