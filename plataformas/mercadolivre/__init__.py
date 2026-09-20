"""
Plataforma Mercado Livre — composição.

Raiz de composição do pacote: reúne as capacidades dos módulos e
monta a instância `PLATAFORMA`, descoberta pelo Auto Discovery no
boot. Não contém lógica.

═══════════════════════════════════════════════════════════════════
MAPA DE RESPONSABILIDADES
═══════════════════════════════════════════════════════════════════
afiliado.py    identidade e validação de atribuição (folha)
ajustes.py     leitura de ambiente que nunca derruba o boot (folha)
links.py       reconhecimento, classificação, elegibilidade — puro
descoberta.py  destino real de uma vitrine: produto ou lista
sessao.py      estado e validade da credencial — fonte única
cliente.py     HTTP com o createLink
afiliacao.py   fluxo do caso de uso e cache

Direção de dependência (acíclica, unidirecional):

  __init__ → afiliacao → links      → afiliado
                       → descoberta → links
                                    → ajustes
                       → sessao
                       → cliente    → sessao
                                    → afiliado

`links` não conhece rede. `sessao` não conhece HTTP. `cliente` não
conhece cache nem elegibilidade. Nada abaixo de `afiliacao` conhece
o core.

`descoberta` é dono do conhecimento de ESTRUTURA DA PÁGINA; `links`
segue dono do conhecimento de FORMA DE URL. Por isso a descoberta
valida o que extraiu chamando `links.cenario_de`, em vez de
reimplementar "isto parece produto?" — a mesma regra em dois lugares
é a divergência de amanhã.

═══════════════════════════════════════════════════════════════════
ESCOPO — PRODUTO, LISTAGEM E A VITRINE RESOLVIDA
═══════════════════════════════════════════════════════════════════
Elegível para o createLink — os dois comprovados em teste real
contra o endpoint:

    produto    /p/MLB<díg>, /up/MLBU<díg>, MLB-<díg>-..._JM
    listagem   lista.mercadolivre.com.br/_Container_<slug>
                 ?coupon_campaign_id=<id>

A vitrine `/social/<slug>` é PORTA DE ENTRADA, não destino. Ela
continua INELEGÍVEL por si só — o servidor a recusa com
error_code 111 — e nunca é enviada ao createLink. O que a torna
útil é `descoberta`: o documento servido declara, em estrutura
nomeada, qual conteúdo aquela vitrine representa.

    seeMoreLink preenchido  → LISTA, e o valor já é a URL
    seeMoreLink vazio       → PRODUTO em polycards[0].url,
                              com len(polycards) == 1

Medido em 4 alvos reais, 2 de cada. Fora dessa forma exata a
resposta é AUSENTE: o pacote nunca escolhe um candidato por conta
própria — sem título da mensagem, sem similaridade de texto, sem
primeira âncora, sem posição de recomendação.

Reconhecido e classificado, mas NÃO enviado ao createLink:

    /social/<slug>/lists/<uuid> lista de afiliado — recusada
    /sec/XXXX                   link curto de afiliado — não converte

A recusa de /sec/ não é escolha de escopo: é o comportamento do
Programa de Afiliados. Esses casos devolvem AUSENTE e seguem o
fallback existente do core.

═══════════════════════════════════════════════════════════════════
AUTENTICAÇÃO
═══════════════════════════════════════════════════════════════════
Credencial de sessão provisionada externamente, em
ML_SESSION_COOKIE e ML_CSRF_TOKEN.

Não há mecanismo oficial, documentado e estável de renovação
automática da sessão do Programa de Afiliados que esteja comprovado
e que possamos usar no Foguetão. O pacote não contém login,
navegador, proxy nem qualquer tentativa de contornar autenticação.
Expiração é detectada, registrada e alertada; a renovação é manual.

═══════════════════════════════════════════════════════════════════
INVARIANTES
═══════════════════════════════════════════════════════════════════
INV-ML-1  `links` é síncrono, puro e sem I/O.
INV-ML-2  Nada abaixo de `afiliacao` conhece cache ou publicação.
INV-ML-3  URL resolvida pelo cache nunca chega à rede.
INV-ML-4  URL inelegível nunca chega ao `cliente`.
INV-ML-5  Nenhum link é publicado sem tag confirmada pelo servidor.
INV-ML-6  Qualquer falha resulta em AUSENTE; nunca link parcial.
INV-ML-7  Credencial recusada não gera retry nem alerta repetido.
INV-ML-8  Cookie, CSRF e segredos nunca aparecem em log.
INV-ML-9  A descoberta é ANÔNIMA: credencial só entra no createLink.
INV-ML-10 Estrutura fora da forma esperada devolve AUSENTE. O alvo
          vem do LINK e da ESTRUTURA — nunca de heurística.
"""
from __future__ import annotations

from plataformas.contrato import CONTRACT_VERSION, Plataforma

from .afiliacao import afilia
from .afiliado import IDENTIFICADOR
from .links import (
    ENCURTADORES,
    ENCURTADORES_FORCA_GET,
    extrai_identidade,
    reconhece,
)


PLATAFORMA = Plataforma(
    identificador=IDENTIFICADOR,
    versao_contrato=CONTRACT_VERSION,
    reconhece=reconhece,
    extrai_identidade=extrai_identidade,
    afilia=afilia,
    encurtadores=ENCURTADORES,
    encurtadores_forca_get=ENCURTADORES_FORCA_GET,
)


# Superfície pública: o caminho idiomático é sempre via registry.
__all__ = ["PLATAFORMA"]
