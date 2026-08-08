# pipeline/assunto.py — CLASSIFICAÇÃO DO ASSUNTO DO POST
#
# Responsabilidade ÚNICA: dado o texto (e fatos derivados pela
# normalização), dizer QUAL É A ESPÉCIE DO ASSUNTO do post.
#
# NÃO faz:
#   - derivação de identidade / âncoras     (identidade_oferta)
#   - memória de códigos / cupom_idx        (identidade_oferta, por ora)
#   - decisão de duplicidade / evolução     (deduplicacao / decisao)
#   - I/O de qualquer natureza              (camada PURA)
#
# CONTRATO PÚBLICO (o resto é interno — regex NUNCA saem daqui):
#   eh_post_cupom(texto)                      -> bool
#   eh_post_cashback(texto, tem_sinal)        -> bool
#   eh_post_evento(texto, tem_host)           -> bool
#   eh_lista_cupons(texto)                    -> bool
#   extrair_pct_cashback(texto)               -> str
#   buscar_calendario_comercial(texto)        -> re.Match | None
#
# C1 (refactor puro): nova casa dos detectores, extraídos byte-a-byte de
# identidade_oferta.py. O classificador AINDA NÃO tem autoridade sobre a
# família — ancoras() segue decidindo como antes. A autoridade só será
# concedida em C3, após o endurecimento (C2).
#
# Implementação: pipeline.assunto_especie (espécie do assunto) e
# pipeline.assunto_oferta (descrição da oferta). Este arquivo é
# FACHADA: não contém lógica, regex nem constante.
from __future__ import annotations

# ─────────────────────────────────────────────────────────────────
# API PÚBLICA — o único contrato exposto. As regex e os detectores
# internos (_RE_*, _eh_*) permanecem privados deste módulo.
# ─────────────────────────────────────────────────────────────────
from pipeline.assunto_especie import (
    buscar_calendario_comercial,
    eh_lista_cupons,
    eh_post_cashback,
    eh_post_cupom,
    eh_post_evento,
    extrair_pct_cashback,
)
from pipeline.assunto_oferta import (
    beneficio_do_cupom,
    beneficio_e_de_loja,
    tem_preco_de_item,
    tema_da_campanha,
)
