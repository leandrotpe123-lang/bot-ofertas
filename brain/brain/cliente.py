"""Cliente do juiz: adaptador da API + resiliência + tetos + cascata.

  · O SDK `anthropic` só é importado por ClienteAnthropic, na primeira
    chamada real — os testes (CI) usam ClienteFalso e não instalam nada.
  · A chave vem do ambiente (ANTHROPIC_API_KEY) e NUNCA é lida, impressa ou
    registrada por este código: quem a resolve é o próprio SDK.
  · Retry com backoff e jitter em 429/529/5xx/rede (respeita retry-after);
    4xx de pedido não é repetido. Timeout por chamada.
  · Disjuntor: N falhas seguidas → aberto por um tempo → meia-abertura.
  · Tetos diários de chamadas e de US$ (custo medido pelo usage).
  · Qualquer falha devolve INCERTO com o motivo: sem IA, o Brain só
    registra. Nada aqui toca o worker."""
from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from brain import config, juiz, precos


class ErroTransitorio(Exception):
    """429, 529, 5xx, rede ou timeout: vale repetir."""
    def __init__(self, msg: str, retry_after: Optional[float] = None):
        super().__init__(msg)
        self.retry_after = retry_after


class ErroPermanente(Exception):
    """4xx de pedido (400, 401, 403, 404…): repetir não ajuda."""


class ClienteAnthropic:
    """Adaptador real. Converte a resposta do SDK em dict e os erros do SDK
    nas duas exceções acima."""

    def __init__(self, timeout_s: float = config.TIMEOUT_S):
        self._timeout = timeout_s
        self._sdk = None

    def _cliente(self):
        if self._sdk is None:
            import anthropic                      # só aqui: CI não instala
            self._mod = anthropic
            self._sdk = anthropic.Anthropic(timeout=self._timeout, max_retries=0)
        return self._sdk

    def enviar(self, params: dict) -> dict:
        c = self._cliente()
        a = self._mod
        try:
            return c.messages.create(**params).to_dict()
        except a.RateLimitError as e:
            raise ErroTransitorio("429", _retry_after(e)) from None
        except a.APIStatusError as e:
            if e.status_code >= 500:              # inclui 529 (sobrecarga)
                raise ErroTransitorio(str(e.status_code), _retry_after(e)) from None
            raise ErroPermanente(str(e.status_code)) from None
        except (a.APIConnectionError, a.APITimeoutError):
            raise ErroTransitorio("rede") from None


def _retry_after(e) -> Optional[float]:
    try:
        v = e.response.headers.get("retry-after")
        return float(v) if v is not None else None
    except Exception:                               # noqa: BLE001
        return None


class ClienteFalso:
    """Para testes: `responder(params) -> dict` ou exceção."""
    def __init__(self, responder: Callable[[dict], dict]):
        self.responder = responder
        self.chamadas = []

    def enviar(self, params: dict) -> dict:
        self.chamadas.append(params)
        return self.responder(params)


@dataclass
class Disjuntor:
    falhas_max: int = config.DISJUNTOR_FALHAS
    pausa_s: float = config.DISJUNTOR_PAUSA_S
    falhas: int = 0
    aberto_ate: float = 0.0

    def permite(self, agora: float) -> bool:
        return agora >= self.aberto_ate

    def sucesso(self) -> None:
        self.falhas = 0

    def falha(self, agora: float) -> None:
        self.falhas += 1
        if self.falhas >= self.falhas_max:
            self.aberto_ate = agora + self.pausa_s
            self.falhas = 0                       # meia-abertura depois da pausa


@dataclass
class Tetos:
    chamadas_dia: int = config.TETO_CHAMADAS_DIA
    usd_dia: float = config.TETO_USD_DIA
    dia: str = ""
    chamadas: int = 0
    usd: float = 0.0

    def _rolar(self, dia: str) -> None:
        if dia != self.dia:
            self.dia, self.chamadas, self.usd = dia, 0, 0.0

    def permite(self, dia: str) -> bool:
        self._rolar(dia)
        return self.chamadas < self.chamadas_dia and self.usd < self.usd_dia

    def registrar(self, dia: str, usd: float) -> None:
        self._rolar(dia)
        self.chamadas += 1
        self.usd += usd


@dataclass
class Juiz:
    cliente: object
    disjuntor: Disjuntor = field(default_factory=Disjuntor)
    tetos: Tetos = field(default_factory=Tetos)
    relogio: Callable[[], float] = time.time
    dormir: Callable[[float], None] = time.sleep
    tentativas: int = config.TENTATIVAS

    def _dia(self) -> str:
        return time.strftime("%Y-%m-%d", time.gmtime(self.relogio()))

    def julgar(self, caso: juiz.Caso, modelo: str, effort: str) -> juiz.Veredito:
        agora = self.relogio()
        if not self.disjuntor.permite(agora):
            return juiz._incerto(modelo, effort, "disjuntor_aberto")
        if not self.tetos.permite(self._dia()):
            return juiz._incerto(modelo, effort, "teto_diario")
        params = juiz.parametros(caso, modelo, effort)
        for tentativa in range(1, self.tentativas + 1):
            t0 = self.relogio()
            try:
                resp = self.cliente.enviar(params)
            except ErroPermanente as e:
                self.disjuntor.falha(self.relogio())
                return juiz._incerto(modelo, effort, f"erro_permanente_{e}")
            except ErroTransitorio as e:
                if tentativa == self.tentativas:
                    self.disjuntor.falha(self.relogio())
                    return juiz._incerto(modelo, effort, f"erro_transitorio_{e}")
                espera = e.retry_after if e.retry_after is not None else min(
                    config.BACKOFF_MAX_S, config.BACKOFF_BASE_S * 2 ** (tentativa - 1))
                self.dormir(min(config.BACKOFF_MAX_S, espera) * random.uniform(1.0, 1.25))
                continue
            except Exception as e:                  # noqa: BLE001 — nunca derruba
                self.disjuntor.falha(self.relogio())
                return juiz._incerto(modelo, effort, f"erro_{type(e).__name__}")
            lat = (self.relogio() - t0) * 1000
            v = juiz.interpretar(resp, caso, modelo, effort, latencia_ms=lat)
            self.disjuntor.sucesso()
            self.tetos.registrar(self._dia(), v.custo_usd)
            return v
        return juiz._incerto(modelo, effort, "sem_resposta")

    def cascata(self, caso: juiz.Caso, principal=config.MODELO_PRINCIPAL,
                referencia=config.MODELO_REFERENCIA,
                certeza_min: float = config.CASCATA_CERTEZA_MIN):
        """O principal julga; a referência só quando ele dá INCERTO, recusa
        ou certeza baixa. Devolve (veredito final, lista de vereditos)."""
        v1 = self.julgar(caso, *principal)
        if v1.veredito != "INCERTO" and v1.certeza >= certeza_min:
            return v1, [v1]
        v2 = self.julgar(caso, *referencia)
        return v2, [v1, v2]


def custo_total(vereditos) -> float:
    return sum(v.custo_usd for v in vereditos)


__all__ = ["ClienteAnthropic", "ClienteFalso", "Juiz", "Disjuntor", "Tetos",
           "ErroTransitorio", "ErroPermanente", "custo_total", "precos"]
