"""
Package plataformas — Catálogo Vivo de Plugins de Plataforma.

Esta package é a dona da DESCOBERTA: conhece o próprio diretório,
importlib, pkgutil, os candidatos a plugin e a constante PLATAFORMA.
Ela varre o diretório, importa os plugins e REPORTA cada evento de
formação ao registry. O registry é dono da camada coletiva (catálogo
e formação); a package é dona da descoberta. As responsabilidades não
se misturam.

A descoberta NÃO acontece por efeito colateral de import. Importar
esta package não inicializa o catálogo. O boot do catálogo é uma
DECISÃO EXPLÍCITA do entrypoint, que chama plataformas.inicializar()
como um passo nomeado da inicialização.

═══════════════════════════════════════════════════════════════════
PRINCÍPIO INEGOCIÁVEL
═══════════════════════════════════════════════════════════════════
Adicionar uma plataforma nova deve exigir EXCLUSIVAMENTE:
  1) criar um arquivo dentro de plataformas/;
  2) declarar a constante PLATAFORMA (instância de Plataforma);
  3) obedecer o contrato existente.

Nada além disso. Sem edição em registry, em main, em bootstrap, em
pipeline, em classificação, em utils ou em qualquer outro ponto do
core. Este é o critério de Zero-Touch Extension definido pelos MBs
da casa.

═══════════════════════════════════════════════════════════════════
REGRA ESTRUTURAL PERMANENTE
═══════════════════════════════════════════════════════════════════
Módulos fora desta package nunca devem importar uma plataforma
concreta pelo nome. O único caminho idiomático para consumir uma
plataforma é via registry:

    from plataformas import registry
    plataforma = registry.resolver(url)             # por URL
    plataforma = registry.acessar(identificador)    # por identificador

Capacidades agregadas são compostas iterando
registry.plataformas_registradas() e consultando cada plataforma
pelo seu identificador nominal. Qualquer import direto do tipo
`from plataformas.amazon import ...` em código fora desta package
deve ser tratado como vazamento arquitetural.

═══════════════════════════════════════════════════════════════════
DESCOBERTA AUTOMÁTICA — REGRAS
═══════════════════════════════════════════════════════════════════
Candidato a plugin é qualquer arquivo .py na package que:
  - não comece por '_' (convenção de privado/interno);
  - não seja 'registry.py' nem 'contrato.py'.

Plugin é qualquer candidato cujo módulo, uma vez importado, expõe
um atributo PLATAFORMA que é instância de Plataforma. Módulos
candidatos que não sejam plugin são silenciosamente ignorados
(podem ser helpers internos da package).

═══════════════════════════════════════════════════════════════════
FORMAÇÃO — POSSE DO REGISTRY
═══════════════════════════════════════════════════════════════════
A descoberta NÃO retém a verdade de formação. Cada evento é reportado
ao registry, que é o dono da camada coletiva:
  - sobreviventes  : entram no catálogo via registry.cadastrar;
  - não-plugins    : reportados via registry.registrar_ignorado;
  - import falho    : reportado via registry.registrar_falha_formacao;
  - cadastro recusado : gravado pelo próprio registry em cadastrar.
A formação resultante é consultável em registry.formacao(). Esta
package não guarda cópia: a fonte única é o registry.

═══════════════════════════════════════════════════════════════════
POLÍTICA DE ERRO
═══════════════════════════════════════════════════════════════════
REGISTRY_ENV=dev (padrão):
  qualquer falha de import ou de cadastro interrompe o boot com
  erro explícito identificando o plugin culpado. Defeito de
  contrato deve ser visível.

REGISTRY_ENV=prod:
  falhas são logadas, reportadas à formação e isoladas; a descoberta
  prossegue com os demais plugins. Preserva continuidade operacional
  ao custo de silenciar defeitos.

═══════════════════════════════════════════════════════════════════
DETERMINISMO
═══════════════════════════════════════════════════════════════════
A ordem de registro é alfabética por nome de módulo, reprodutível
entre sistemas operacionais. Como o contrato exige reconhecimento
mutuamente exclusivo entre plataformas (4ª invariante do contrato),
a ordem é semanticamente irrelevante — mas a previsibilidade do log
e do teste vale o custo zero.

═══════════════════════════════════════════════════════════════════
EXECUÇÃO
═══════════════════════════════════════════════════════════════════
A descoberta é disparada por uma chamada explícita a inicializar(),
feita pelo entrypoint no boot. Importar esta package não dispara
nada. inicializar() é idempotente apenas no sentido de que o catálogo
e a formação são estáveis após a primeira execução; não é projetada
para reexecução no mesmo processo. Para testes, o trabalho real está
em _descobrir_plugins, que recebe path e nome de pacote como
parâmetros e pode ser invocada com diretórios mock; os testes
inspecionam o resultado via registry.formacao().

═══════════════════════════════════════════════════════════════════
API PÚBLICA DA PACKAGE
═══════════════════════════════════════════════════════════════════
Símbolos públicos para consumidores externos:
  - inicializar : dispara a descoberta no boot (chamada pelo main)
  - registry    : o catálogo e a camada coletiva
  - contrato    : os tipos do contrato (uso por anotações de tipo)
"""
from __future__ import annotations

import importlib
import os
import pkgutil

from logger import log_sys

from plataformas import registry
from plataformas.contrato import Plataforma


# ── Módulos estruturais que NÃO são plugins ───────────────────────
# Excluídos por nome explícito. Se forem renomeados ou movidos para
# um subpacote, esta lista precisa acompanhar. A alternativa de
# isolar o core da package em plataformas/_core/ é desejável no
# futuro mas não bloqueia esta fundação.
_MODULOS_ESTRUTURAIS = frozenset({"registry", "contrato"})
_ja_inicializado = False


def _eh_candidato_a_plugin(nome_modulo: str) -> bool:
    """
    Verdadeiro se o nome do módulo é candidato a plugin: arquivo .py
    público (não começa por '_') e não é módulo estrutural da package.
    """
    if nome_modulo.startswith("_"):
        return False
    if nome_modulo in _MODULOS_ESTRUTURAIS:
        return False
    return True


def _descobrir_plugins(
    pacote_path,
    pacote_nome: str,
    modo_producao: bool,
) -> None:
    """
    Varre o diretório do pacote e tenta registrar cada módulo
    candidato como plugin.

    Não devolve valor: cada evento de formação é REPORTADO ao
    registry, que é o dono da camada coletiva. Sobreviventes entram
    no catálogo via registry.cadastrar; candidatos sem PLATAFORMA
    válida são reportados via registry.registrar_ignorado; imports
    malsucedidos via registry.registrar_falha_formacao; rejeições de
    cadastro são gravadas pelo próprio registry em cadastrar. A
    formação resultante é consultável em registry.formacao().

    Em modo desenvolvimento, qualquer falha re-lança a exceção
    original após o log, interrompendo o boot. Em modo produção, a
    falha é apenas reportada e a descoberta prossegue com os demais
    plugins.

    A função é parametrizada para ser testável com diretórios mock;
    os testes inspecionam o resultado via registry.formacao().
    """
    # Coleta primeiro, ordena depois — determinismo entre sistemas.
    # Candidatos = módulos .py soltos E sub-pacotes: uma plataforma
    # pode ser um único módulo ou um pacote coeso (ex.: shopee/, com
    # links.py + afiliacao.py + __init__.py expondo PLATAFORMA). A
    # descoberta é agnóstica à estrutura interna — o que qualifica é
    # expor PLATAFORMA (verificado adiante), não ser arquivo único.
    nomes_candidatos = sorted(
        nome
        for _, nome, _ehpkg in pkgutil.iter_modules(pacote_path)
        if _eh_candidato_a_plugin(nome)
    )

    for nome in nomes_candidatos:
        caminho = f"{pacote_nome}.{nome}"

        # 1) Import — falha aqui é código quebrado no plugin (sintaxe,
        #    dependência ausente, exceção em top-level). O registry não
        #    observa imports; reportamos a falha de formação a ele.
        try:
            modulo = importlib.import_module(caminho)
        except Exception as exc:
            motivo = f"import falhou: {type(exc).__name__}: {exc}"
            registry.registrar_falha_formacao(nome, motivo)
            log_sys.error(f"❌ Plugin '{nome}' — {motivo}")
            if not modo_producao:
                raise
            continue

        # 2) Conformidade — o módulo expõe PLATAFORMA? Se não, não é
        #    plugin (pode ser helper interno, cenário legítimo). O
        #    registry não observa isso; reportamos como ignorado.
        plataforma_obj = getattr(modulo, "PLATAFORMA", None)
        if not isinstance(plataforma_obj, Plataforma):
            registry.registrar_ignorado(nome)
            log_sys.debug(
                f"⏭ Módulo '{nome}' sem PLATAFORMA válida — ignorado"
            )
            continue

        # 3) Cadastro — a rejeição de contrato (versão incompatível,
        #    identificador duplicado, capacidade ausente) é detectada e
        #    GRAVADA pelo próprio registry, dono da formação. Aqui só
        #    decidimos halt (dev) ou isolamento (prod).
        try:
            registry.cadastrar(plataforma_obj)
        except registry.ErroCadastroPlataforma:
            if not modo_producao:
                raise
            continue
        except Exception as exc:
            # Falha inesperada, não é rejeição de contrato: o registry
            # não a gravou; reportamos para a formação ficar completa.
            motivo = f"cadastro falhou: {type(exc).__name__}: {exc}"
            registry.registrar_falha_formacao(nome, motivo)
            log_sys.error(f"❌ Plugin '{nome}' — {motivo}")
            if not modo_producao:
                raise
            continue


def inicializar() -> None:
    """
    Dispara a descoberta de plugins — boot explícito do catálogo.

    É a porta pública do boot: o entrypoint chama
    plataformas.inicializar() como um passo nomeado da inicialização.
    """
    global _ja_inicializado

    if _ja_inicializado:
        log_sys.warning(
            "🧩 Auto Discovery já executado neste processo — "
            "rechamada ignorada."
        )
        return

    modo_producao = (
        os.environ.get("REGISTRY_ENV", "dev").lower() == "prod"
    )

    _descobrir_plugins(
        pacote_path=__path__,
        pacote_nome=__name__,
        modo_producao=modo_producao,
    )

    _ja_inicializado = True

    formacao = registry.formacao()

    log_sys.info(
        f"🧩 Auto Discovery | "
        f"registrados={len(formacao['registrados'])} "
        f"ignorados={len(formacao['ignorados'])} "
        f"falhas={len(formacao['falhas'])} "
        f"plataformas={list(formacao['registrados'])}"
          )


# ── API pública da package ────────────────────────────────────────
# Plataformas concretas NÃO são reexportadas. O caminho idiomático
# para acessá-las é sempre via registry.resolver / registry.acessar.
# A descoberta é disparada por inicializar(), nunca por import.
__all__ = ["inicializar", "registry", "contrato"]
