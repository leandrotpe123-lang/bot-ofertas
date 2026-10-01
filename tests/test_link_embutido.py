"""
LINK EMBUTIDO — hiperlink da fonte não pode sumir da publicação.

Teste do operador (01/10 00:11 UTC, @ofertasconvertidas 1350 → post
24381): "Cupom Shopee Moda Voltando" com os links como HIPERLINK
EMBUTIDO (MessageEntityTextUrl, sublinhado). A ingestão lê
`message.text`, e o Telethon entrega o hiperlink em Markdown
`[https://…](https://…)`. A extração de URL do bloco (`https?://\\S+`)
capturava `https://…](https://…)` — URL que não está no mapa —, o
bloco era tratado como "link de plataforma que não converteu" e
"-Resgate aqui:" / "-Link carrinho:" e os dois links desapareciam.

Os textos de cada formato são gerados pelo PRÓPRIO Telethon
(`markdown.unparse`), não escritos à mão.

    python tests/test_link_embutido.py
"""
import asyncio
import os
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# Telethon REAL antes do harness: os textos de cada formato saem do
# próprio `markdown.unparse` (o harness só instala o stub se o Telethon
# ainda não estiver importado).
from telethon.extensions import markdown                        # noqa: E402
from telethon.tl.types import (MessageEntityCode,               # noqa: E402
                               MessageEntityTextUrl,
                               MessageEntityUnderline,
                               MessageEntityUrl)
from _harness_e5 import preparar, rodar  # noqa: E402

preparar()

try:
    import yarl  # noqa: F401
except ImportError:
    _yarl = types.ModuleType("yarl")

    class _URL(str):
        def __new__(cls, valor="", **_kw):
            return str.__new__(cls, valor)
    _yarl.URL = _URL
    sys.modules["yarl"] = _yarl

import plataformas                                              # noqa: E402
from pipeline import normalizacao                               # noqa: E402
from pipeline.filtros import filtrar, filtrar_blocos            # noqa: E402
from pipeline.filtros_estrutura import urls_do_bloco            # noqa: E402
from pipeline.ingestao import MensagemBruta, _RE_URL            # noqa: E402
from pipeline.montagem_texto import montar_texto                # noqa: E402

plataformas.inicializar()

# Texto REAL da @ofertasconvertidas 1350 (= fumotom 35130).
RAW = ("Cupom Shopee Moda Voltando!\n\nR$ 15 OFF em R$ 89: MODA15SHO\n\n"
       "-Resgate aqui: \nhttps://s.shopee.com.br/70GSVaUJxC\n\n"
       "-Link carrinho:\nhttps://s.shopee.com.br/7fW9IpEnZG\n\n-Anúncio")
U1, U2 = "https://s.shopee.com.br/70GSVaUJxC", "https://s.shopee.com.br/7fW9IpEnZG"
AFF = {U1: "https://s.shopee.com.br/AFF1aaaaaa", U2: "https://s.shopee.com.br/AFF2bbbbbb"}


def _ent(cls, texto, sub, **kw):
    return cls(offset=texto.index(sub), length=len(sub), **kw)


def formatos():
    """{nome: texto como `message.text` o entrega} — via Telethon."""
    code = _ent(MessageEntityCode, RAW, "MODA15SHO")
    rot = RAW.replace(U1, "Clique aqui").replace(U2, "Ver carrinho")
    return {
        "url_simples": markdown.unparse(RAW, [
            code, _ent(MessageEntityUrl, RAW, U1), _ent(MessageEntityUrl, RAW, U2)]),
        "textourl_rotulo_url": markdown.unparse(RAW, [
            code, _ent(MessageEntityTextUrl, RAW, U1, url=U1),
            _ent(MessageEntityTextUrl, RAW, U2, url=U2)]),
        "textourl_sublinhado": markdown.unparse(RAW, [
            code, _ent(MessageEntityTextUrl, RAW, U1, url=U1),
            _ent(MessageEntityUnderline, RAW, U1),
            _ent(MessageEntityTextUrl, RAW, U2, url=U2),
            _ent(MessageEntityUnderline, RAW, U2)]),
        "textourl_rotulo_texto": markdown.unparse(rot, [
            _ent(MessageEntityCode, rot, "MODA15SHO"),
            _ent(MessageEntityTextUrl, rot, "Clique aqui", url=U1),
            _ent(MessageEntityTextUrl, rot, "Ver carrinho", url=U2)]),
    }


def links_da_ingestao(texto):
    """Mesma extração de `ingestao.ingerir`."""
    return [u.strip().rstrip('.,;)>]}!?') for u in _RE_URL.findall(texto)]


# ══════════════════════════════════════════════════════════════════
def test_01_urls_do_bloco_puro(r):
    r.check(urls_do_bloco(f"veja {U1}.") == [U1 + "."] or
            [u.rstrip(".") for u in urls_do_bloco(f"veja {U1}.")] == [U1],
            "01.url_pura")
    r.check(urls_do_bloco(f"[{U1}]({U1})") == [U1], "01.link_md_rotulo_url",
            str(urls_do_bloco(f"[{U1}]({U1})")))
    r.check(urls_do_bloco(f"[Resgate aqui]({U1})") == [U1], "01.link_md_rotulo_texto",
            str(urls_do_bloco(f"[Resgate aqui]({U1})")))
    r.check(urls_do_bloco(f"{U1} e [x]({U2})") == [U1, U2], "01.varios_na_linha",
            str(urls_do_bloco(f"{U1} e [x]({U2})")))
    r.check(urls_do_bloco("sem link nenhum") == [], "01.sem_link")
    r.check(urls_do_bloco(f"(link: {U1})") == [U1], "01.url_pura_entre_parenteses",
            str(urls_do_bloco(f"(link: {U1})")))
    r.check(urls_do_bloco(f"[{U1}]({U1}) e [{U2}]({U2})") == [U1, U2],
            "01.dois_links_embutidos_na_linha",
            str(urls_do_bloco(f"[{U1}]({U1}) e [{U2}]({U2})")))
    r.check(not any("](" in u or "]" in u for u in
                    urls_do_bloco(f"[{U1}]({U1}) [{U2}]({U2})")), "01.nunca_url_quebrada")


def test_02_formatos_reais_mantem_os_blocos(r):
    """Em TODOS os formatos o filtro de blocos mantém links e rótulos."""
    for nome, texto in formatos().items():
        links = links_da_ingestao(texto)
        mapa = {u: AFF[u] for u in dict.fromkeys(links)}
        saida = filtrar_blocos(filtrar(texto), mapa)
        r.check("-Resgate aqui:" in saida and "-Link carrinho:" in saida,
                f"02.{nome}.rotulos_mantidos", repr(saida))
        r.check(U1 in saida and U2 in saida, f"02.{nome}.links_mantidos", repr(saida))


def test_03_link_que_nao_converteu_continua_saindo(r):
    """Regressão: link de PLATAFORMA sem conversão segue removido — nas
    formas embutida e pura."""
    for nome, texto in formatos().items():
        saida = filtrar_blocos(filtrar(texto), {U1: AFF[U1]})
        r.check(U1 in saida and "-Resgate aqui:" in saida, f"03.{nome}.convertido_fica",
                repr(saida))
        r.check(U2 not in saida and "-Link carrinho:" not in saida,
                f"03.{nome}.nao_convertido_sai", repr(saida))


def test_04_bloco_sem_link_e_auxiliar(r):
    texto = "Titulo\n\nR$ 10 OFF: ABC\n\n-Consulte o regulamento: [aqui](https://exemplo.org/regras)"
    saida = filtrar_blocos(texto, {})
    r.check("Titulo" in saida and "R$ 10 OFF" in saida, "04.sem_link_preservado", repr(saida))
    r.check("exemplo.org" not in saida, "04.auxiliar_sem_ancora_sai", repr(saida))
    texto2 = "Titulo\n\n-Regras da Shopee: [aqui](https://exemplo.org/regras)"
    r.check("exemplo.org" in filtrar_blocos(texto2, {}), "04.auxiliar_ancorado_fica")


def test_05_ponta_a_ponta_normalizacao_e_montagem(r):
    """normalizar() (afiliação simulada, sem rede) + montar_texto(): o
    post sai com os dois rótulos, os dois links AFILIADOS e o cupom."""
    async def falso_resolver(converter, msg_id=0):
        mapa = {u: AFF[u] for u in converter if u in AFF}
        return mapa, dict(mapa), ["shopee"] * len(mapa)

    real = normalizacao.resolver_e_afiliar
    normalizacao.resolver_e_afiliar = falso_resolver
    try:
        for nome, texto in formatos().items():
            bruta = MensagemBruta(msg_id=1350, chat="-1003817694320", texto=texto,
                                  links=links_da_ingestao(texto), tem_midia=False,
                                  media_obj=None, code_entities=["MODA15SHO"])
            norm = asyncio.run(normalizacao.normalizar(bruta))
            r.check(norm is not None, f"05.{nome}.normalizou")
            if norm is None:
                continue
            final = montar_texto(norm)
            r.check(norm.cupons == ["MODA15SHO"], f"05.{nome}.cupom", str(norm.cupons))
            r.check("Resgate aqui" in final and "Link carrinho" in final,
                    f"05.{nome}.rotulos_no_post", repr(final))
            r.check(AFF[U1] in final and AFF[U2] in final, f"05.{nome}.links_afiliados_no_post",
                    repr(final))
            r.check(U1 not in final and U2 not in final, f"05.{nome}.sem_link_original",
                    repr(final))
    finally:
        normalizacao.resolver_e_afiliar = real


if __name__ == "__main__":
    sys.exit(rodar(globals(), "LINK EMBUTIDO · hiperlink da fonte · post 24381"))
