#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════
SONDA — A PROVA DA CANÔNICA
═══════════════════════════════════════════════════════════════════

Pergunta única: qual URL o adapter ML entrega para a etapa de
identidade?

    A) URL original recebida
    B) URL expandida
    C) URL que entra na descoberta
    D) URL descoberta (destino produto/lista)
    E) URL efetivamente ENVIADA ao createLink
    F) URL RETORNADA pelo createLink (long_url)
    G) URL efetivamente usada por derivar_produto

MÉTODO — instrumentar, não reimplementar
    A sonda NÃO refaz os passos do fluxo. Ela ENVOLVE as funções
    reais (`_expandir`, `descoberta.descobrir`, `cliente.criar_link`)
    e anota o que passa. Reimplementar o fluxo mediria uma coisa
    diferente da que roda em produção — erro já cometido nesta
    investigação, e que inventou um defeito que não existia.

    Depois de `afilia` devolver, a canônica vai para o
    `derivar_produto` REAL, com o registry REAL. É o Core de
    verdade respondendo.

CONTROLE
    Um produto `/p/MLB…` conhecido precisa continuar produzindo
    `id_produto = MLB…`. Se o controle falhar, a sonda para e não
    conclui nada — o experimento estaria quebrado, não o sistema.

SEGREDO
    Nenhum cookie, CSRF, ssid, header ou credencial é impresso.
    Só URLs públicas e dados estruturais. A sonda imprime se a
    credencial ESTÁ presente, nunca o valor.

NÃO ALTERA
    Produção, Core, links.py, variáveis. Não publica nada. Todo
    monkeypatch vive dentro deste processo e morre com ele.

COMO RODAR (Railway)
    start command: python -u testes_ml/sonda_canonica.py
    depois, devolver: python main.py
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def log(m: str = "") -> None:
    print(f"[CANONICA] {m}", flush=True)


def bloco(t: str) -> None:
    log("═" * 62)
    log(t)
    log("═" * 62)


# ── Casos ─────────────────────────────────────────────────────────
# Todos já converteram em execuções anteriores, logo não estamos
# testando se convertem — estamos olhando o QUE SAI.
PRODUTO = os.environ.get("SONDA_PRODUTO") or (
    "https://www.mercadolivre.com.br/p/MLB16016316"
)
LISTA = os.environ.get("SONDA_LISTA") or (
    "https://lista.mercadolivre.com.br/_Container_promotions-77-full"
    "?coupon_campaign_id=14194174"
)
VITRINE = os.environ.get("SONDA_VITRINE") or "https://meli.la/1ipL9sf"

CASOS = [
    ("PRODUTO /p/ (CONTROLE)", PRODUTO),
    ("LISTA _Container_ + coupon_campaign_id", LISTA),
    ("VITRINE meli.la (exercita descoberta)", VITRINE),
]


def _curto(u, n=118):
    s = str(u)
    return s if len(s) <= n else s[:n] + "…"


async def rodar() -> int:
    bloco("SONDA DA CANÔNICA — o que chega ao Core?")

    try:
        import globals as g
        import plataformas
        from plataformas.contrato import AUSENTE, Afiliacao
        from plataformas.mercadolivre import afiliacao, cliente, descoberta
        from plataformas.mercadolivre import links, sessao
        from pipeline.normalizacao_identidade import derivar_produto
    except Exception as exc:
        log(f"ERRO ao importar: {type(exc).__name__}: {exc}")
        return 2

    plataformas.inicializar()
    g._init_globals()
    sessao_http = await g._get_session()

    log(f"credencial presente : {sessao.disponivel()}")
    log(f"tag configurada     : {bool(os.environ.get('ML_TAG'))}")
    log("(valores nunca impressos)")
    log()

    # ── Instrumentação: envolve o REAL, não substitui ─────────────
    trilha: dict = {}

    _expandir_real = afiliacao._expandir
    _descobrir_real = descoberta.descobrir
    _criar_real = cliente.criar_link

    async def _expandir_espiao(url, ses):
        out = await _expandir_real(url, ses)
        trilha["B_expandida"] = out
        return out

    async def _descobrir_espiao(url_social, ses):
        trilha["C_entrada_descoberta"] = url_social
        out = await _descobrir_real(url_social, ses)
        trilha["D_destino"] = getattr(out, "url", None)
        trilha["D_tipo"] = getattr(out, "tipo", None)
        return out

    async def _criar_espiao(alvo, credencial, geracao):
        trilha["E_enviada_createlink"] = alvo
        try:
            out = await _criar_real(alvo, credencial, geracao)
        except Exception as exc:
            trilha["F_erro"] = f"{type(exc).__name__}: {exc}"
            raise
        trilha["F_retornada_long_url"] = getattr(out, "canonica", None)
        trilha["F_publicada_short"] = getattr(out, "publicada", None)
        return out

    afiliacao._expandir = _expandir_espiao
    descoberta.descobrir = _descobrir_espiao
    cliente.criar_link = _criar_espiao
    # `afiliacao` importou `descoberta` como módulo, então a troca
    # acima já vale para ele. `cliente` idem.

    controle_ok = False
    resumo = []

    for rotulo, url in CASOS:
        trilha.clear()
        # Isola os casos: sem isto, 3 falhas seguidas abrem o
        # disjuntor e os casos seguintes nem chegam à rede — um
        # caso ruim levaria os outros junto e a rodada se perderia.
        # `_resetar_para_teste` é a afordância de teste do próprio
        # módulo; zera contador de falhas, disjuntor e sessão HTTP.
        cliente._resetar_para_teste()
        bloco(rotulo)
        log(f"A) ORIGINAL                 : {_curto(url)}")
        log(f"   cenario_de              : {links.cenario_de(url)}")
        log(f"   eh_elegivel             : {links.eh_elegivel(url)}")

        # Estas URLs ja converteram em execucoes anteriores, entao o
        # cache pode responder antes de qualquer rede — e aí os
        # passos E e F nao acontecem. Sem dizer isso, um "(nao
        # chamou)" seria lido como defeito quando e so cache.
        from utils.cache_links import consultar_link
        try:
            ja = consultar_link(url)
        except Exception:
            ja = None
        log(f"   veio do CACHE?           : {'SIM' if ja else 'nao'}")

        try:
            resultado = await afiliacao.afilia(url, sessao_http)
        except Exception as exc:
            log(f"   !! afilia levantou: {type(exc).__name__}: {exc}")
            resumo.append((rotulo, "EXCECAO", "", ""))
            log()
            continue

        log(f"B) EXPANDIDA                : "
            f"{_curto(trilha.get('B_expandida', '(nao expandiu)'))}")
        log(f"C) ENTRADA DA DESCOBERTA    : "
            f"{_curto(trilha.get('C_entrada_descoberta', '(nao descobriu)'))}")
        log(f"D) DESTINO DESCOBERTO       : "
            f"{_curto(trilha.get('D_destino', '(nao descobriu)'))}")
        log(f"   tipo                     : {trilha.get('D_tipo', '-')}")
        log(f"E) ENVIADA AO CREATELINK    : "
            f"{_curto(trilha.get('E_enviada_createlink', '(nao chamou)'))}")
        log(f"F) RETORNADA (long_url)     : "
            f"{_curto(trilha.get('F_retornada_long_url', '(sem retorno)'))}")
        if trilha.get("F_erro"):
            log(f"   erro do createLink       : {trilha['F_erro'][:150]}")

        if not isinstance(resultado, Afiliacao):
            log("   >>> afilia devolveu AUSENTE — sem canonica")
            resumo.append((rotulo, "AUSENTE", "", ""))
            log()
            continue

        canonica = resultado.canonica
        log(f"G) CANONICA USADA NA IDENTIDADE:")
        log(f"   {_curto(canonica)}")
        log(f"   publicada (o que vai ao post): "
            f"{_curto(resultado.publicada)}")

        # ── O Core de verdade ─────────────────────────────────────
        ident = links.extrai_identidade(canonica)
        derivado = derivar_produto([canonica])

        log(f"   cenario_de(canonica)     : {links.cenario_de(canonica)}")
        log(f"   id_produto               : {ident.id_produto}")
        log(f"   id_global                : {ident.id_global}")
        log(f"   >>> ids_globais do CORE  : {derivado.ids_globais}")
        log(f"   >>> sku do CORE          : {derivado.sku!r}")

        # A pergunta central, respondida por comparação.
        enviada = trilha.get("E_enviada_createlink")
        if enviada and canonica:
            mesma = links.cenario_de(canonica) == links.cenario_de(enviada)
            log(f"   canonica representa o DESTINO convertido? "
                f"{'SIM' if mesma else 'NAO'}")
            if not mesma:
                log(f"      destino era  : {links.cenario_de(enviada)}")
                log(f"      canonica eh  : {links.cenario_de(canonica)}")

        if "CONTROLE" in rotulo:
            controle_ok = bool(derivado.ids_globais)

        resumo.append((rotulo, links.cenario_de(canonica),
                       str(ident.id_produto), str(derivado.ids_globais)))
        log()

    # ── Veredito ──────────────────────────────────────────────────
    bloco("RESUMO")
    log(f"{'CASO':<40} {'CENARIO CANONICA':<18} {'ids_globais'}")
    log("-" * 88)
    for rotulo, cen, idp, idg in resumo:
        log(f"{rotulo[:40]:<40} {cen[:18]:<18} {idg}")
    log()

    bloco("CONTROLE")
    if controle_ok:
        log("CONTROLE OK — o produto conhecido produziu id_produto.")
        log("O experimento e valido; as demais linhas podem ser lidas.")
    else:
        log("CONTROLE FALHOU — o produto conhecido NAO produziu")
        log("id_produto. O experimento esta quebrado. NAO concluir")
        log("nada das outras linhas ate corrigir a sonda.")
    log()
    log("Nada foi concluido automaticamente. A leitura decide.")
    return 0


def main() -> int:
    try:
        return asyncio.run(rodar())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
