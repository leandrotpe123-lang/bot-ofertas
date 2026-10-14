"""
CUPOM SEM CÓDIGO — LANÇAMENTO × LIBERAÇÃO (frente CUPOM-SEM-CÓDIGO).

Os dois erros, medidos em TODO o log disponível na Railway (07/10 09:07 →
14/10 09:08 UTC) e na prévia pública dos canais (t.me/<canal>/<id>):
  · FALSO NEGATIVO: a mesma campanha com nomes diferentes vira dois posts.
    Caso 1 (11/10 22:00 UTC): Promotom 111295 `proximo` → post 28391; Fada
    17799 `lojas-oficiais` → post 28392 (reproduzido pelo dono em 14/10 com
    as mensagens 1386 e 1387 → posts 28913 e 28914).
  · FALSO POSITIVO: a série horária da mesma fonte some. 10.10, Promotom
    "+1 30% OFF Mercado Livre": 111092 (23:59:30) SCORE_NAO_EVOLUI contra o
    27046 e 111103 (01:59:20) DUP_SILENCIOSO contra o 27126; Samuel 120304
    (00:04) DUP_SILENCIOSO contra o 27055 (Δ 25,5 min).

A regra nova (pipeline.cupom_lancamento) roda em DOIS modos:
  SOMBRA (padrão) — comportamento IDÊNTICO ao de antes; só registra
    CUPOM_ENCONTRO_SOMBRA / CUPOM_LIBERACAO_SOMBRA / CUPOM_INCERTO.
  ATIVO — a regra decide a família. Estes testes ligam o modo à mão; na
    `main` (sem a regra) os de ATIVO caem.

Textos: prévia pública, texto fiel (sem os links, que a regra não lê). O
veículo de produto do link de tutorial do ML é o dos logs (MLB63220587 da
Promotom/Samuel, MLB26070781 da Fada). O tempo entre mensagens é o real
(Δ dos logs), simulado recuando a estampa dos posts no banco temporário;
onde o Δ é sintético, o teste diz.

ROTULAGEM (honestidade): o que é "mesma campanha" foi julgado pelo texto
público por quem escreveu a regra, nos MESMOS casos que a guiaram. A prova
independente é a marcação S/N do dono mais a sombra em produção.

  01–06  regra pura: nome, radical, benefício, lembrete, escopo, decisão;
         fora da regra: plataforma sem medição, destino, container
  07–09  equivalências das otimizações da D.5 (filtro de posts; acento;
         campo calculado só quando a decisão o lê)
  10     SOMBRA = desligado = sabotado (diferencial em todos os cenários;
         sabotagem na leitura do banco, na decisão e no registro)
  11     SOMBRA registra a decisão certa em cada cenário
  12     SOMBRA não muda os locks (ATIVO estende o mutex da adoção)
  13     a regra recebe o destino e o container que a família usou
  14     D.5: perfil só do candidato e dos posts que podem mudar a decisão
  15     família sem âncora `cupb` (caminho futuro) não é julgada
  20–31  ATIVO: caso 1, série do 10.10, falsos positivos, falsos
         negativos, zona cinzenta = hoje, distintos, lembrete, mesma fonte
         na rajada, testes fechados, corrida, revalidação da adoção
  32     edição nunca entra na regra (nos dois modos)

    python tests/test_cupom_lancamento.py
"""
import asyncio
import logging
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_cupom_sem_codigo import (                             # noqa: E402
    FADA, MOTIVOS, PROMOTOM, SAMUEL, _db, cenario, msg, publicar)
from _harness_e5 import rodar                                   # noqa: E402
import config                                                    # noqa: E402
from logger import log_out                                       # noqa: E402
from pipeline import familia, origem                             # noqa: E402

CHAT = {"promotom": PROMOTOM, "fadadoscupons": FADA, "SamuelF3lipePromo": SAMUEL}
ML = "mercadolivre"
SHP = "shopee"
VEICULO = {   # produto que o link de tutorial do ML resolve (logs de produção)
    "promotom": ["https://produto.mercadolivre.com.br/MLB-63220587-tutorial-_JM"],
    "SamuelF3lipePromo": ["https://produto.mercadolivre.com.br/MLB-63220587-tutorial-_JM"],
    "fadadoscupons": ["https://produto.mercadolivre.com.br/MLB-26070781-tutorial-_JM"],
}

TEXTO = {
    'SamuelF3lipePromo/120080': '🔥 Próximo Cupom 25% OFF Mercado Livre\n\n⚠️ Resgate agora e use 00h\n\n⭐️ Tutorial para resgate:\n1 Entre no APP do Meli aqui:\n2 Clique em "Mais" no rodapé do APP\n3 Depois clique em "Cupons relâmpago"\n4 Resgate o cupom\n\nanúncio',
    'SamuelF3lipePromo/120276': '🔥 Próximo Cupom 30% OFF Mercado Livre\n\n⚠️ Resgate agora e use MEIA-NOITE\n\n⭐️ Tutorial para resgate:\n1. Entre no APP do Meli aqui:\n2. Clique em "Mais" no rodapé do APP\n3. Depois clique em "Cupons relâmpago"\n4. Resgate o cupom\n\nanúncio',
    'SamuelF3lipePromo/120285': '🔥 Próximo Cupom 30% OFF Mercado Livre\n\n⚠️ Resgate agora e use MEIA-NOITE\n\n⭐️ Tutorial para resgate:\n1. Entre no APP do Meli aqui:\n2. Clique em "Mais" no rodapé do APP\n3. Depois clique em "Cupons relâmpago"\n4. Resgate o cupom\n\nanúncio',
    'SamuelF3lipePromo/120304': '🔥 Próximo Cupom 30% OFF Mercado Livre\n\n⚠️ Resgate agora e use MEIA-NOITE\n\n⭐️ Tutorial para resgate:\n1. Entre no APP do Meli aqui:\n2. Clique em "Mais" no rodapé do APP\n3. Depois clique em "Cupons relâmpago"\n4. Resgate o cupom\n\nanúncio',
    'SamuelF3lipePromo/120342': '🔥 Próximo Cupom 30% OFF Mercado Livre\n\n⚠️ Resgate agora e use MEIA-NOITE\n\n⭐️ Tutorial para resgate:\n1. Entre no APP do Meli aqui:\n2. Clique em "Mais" no rodapé do APP\n3. Depois clique em "Cupons relâmpago"\n4. Resgate o cupom\n\nanúncio',
    'SamuelF3lipePromo/120344': '🔥 Lembrando! Cupom Shopee 40/199\n\n🎟 R$ 40 OFF em compras acima de R$ 199\n\n- Ativa à meia-noite! Já gere o link do produto. É só copiar o link de um produto, colar no site ou bot abaixo e clicar em "Gerar link com desconto”. Entre no link gerado após a meia-noite, o cupom irá aparecer na aba de cupons na finalização do pedido.\n\n1️⃣ Gere o link pelo site:\n\n\n2️⃣ Ou mande no chat:\n\n\nanúncio',
    'SamuelF3lipePromo/120345': '🔥 Próximo Cupom 30% OFF Mercado Livre\n\n⚠️ Resgate agora e use MEIA-NOITE\n\n⭐️ Tutorial para resgate:\n1. Entre no APP do Meli aqui:\n2. Clique em "Mais" no rodapé do APP\n3. Depois clique em "Cupons relâmpago"\n4. Resgate o cupom\n\nanúncio',
    'SamuelF3lipePromo/120479': '🔥 Cupons Shopee Contas Selecionados\n\n👉 Veja se recebeu aqui:\n\n\nanúncio',
    'SamuelF3lipePromo/120480': '🔥 Cupom 50% OFF Shopee VIP\n\n👉 Resgate aqui:\n\n\n⭐️ Teste 30 dias Grátis:\n\n\nanúncio',
    'SamuelF3lipePromo/120532': '🔥 Cupom 50% OFF Shopee VIP ainda Ativo!!\n\n👉 Resgate aqui:\n\n\n⭐️ Assine o VIP (Ganhe de 1 a 3 meses Grátis):\n\n\nanúncio',
    'SamuelF3lipePromo/120585': '🔥 Cupom Shopee Esgotando!\n\n🎟 R$ 40 OFF em compras acima de R$ 199\n\n✅ Envie o link do produto que precisa, entre no link gerado e selecione o cupom na finalização do pedido, na aba “Cupom de Desconto"\n\n1️⃣ Gere o link pelo site:\n\n\n2️⃣ Ou Gere no chat:\n\n\nanúncio',
    'SamuelF3lipePromo/120804': '🔥 Próximo Cupom 25% OFF em Entregas Full Mercado Livre\n\n⚠️ Resgate agora e use MEIA-NOITE\n\n⭐️ Tutorial para resgate:\n1. Entre no APP do Meli aqui:\n2. Clique em "Mais" no rodapé do APP\n3. Depois clique em "Cupons relâmpago 10.10"\n4. Resgate o cupom 25% OFF\n\nanúncio',
    'SamuelF3lipePromo/121007': '🔥 Próximo Cupom 20% OFF em Lojas Oficiais Mercado Livre\n\n⚠️ Resgate agora e use MEIA-NOITE\n\n⭐️ Tutorial para resgate:\n1. Entre no APP do Meli aqui:\n2. Clique em "Mais" no rodapé do APP\n3. Depois clique em "Cupons relâmpago"\n4. Resgate o cupom\n\nanúncio',
    'SamuelF3lipePromo/121241': '🔥 Dia de Cupons na Shopee, Começa Meia Noite!\n\n⭐️ Página do Evento:\n\n\n👉 Assine o VIP (1 a 3 meses Grátis):\n\n\nℹ️ Clique em "Confira seu nível" no APP\n\n\nanúncio',
    'fadadoscupons/17631': '🔥 Cupom 25% OFF Mercado Livre Full\n\nResgate agora e use 00h\n\n🚨🚨 TUTORIAL:\n1. Entre no APP 👇\n\n2. Clique em "Mais" no canto inferior direito\n3. Entre em “Cupons relâmpago do 10.10"\n4. Resgate o cupom',
    'fadadoscupons/17674': '🚨 Cupons liberados para quem é VIP Shopee!\n\n✅ Resgate aqui:\n\n\n📵 VIP 1 mês grátis:',
    'fadadoscupons/17685': '🚨 Lembrando! Cupom Shopee 40/199\n\n🎟 R$40 OFF em compras acima de R$199\n\n⏰ Ativa à meia-noite! Já gere o link do produto. É só copiar o link de um produto, colar no site ou bot abaixo e clicar em "Gerar link com desconto”. Entre no link gerado após a meia-noite, o cupom irá aparecer na aba de cupons na finalização do pedido.\n\n1️⃣ Gere o link pelo site:\n\n\n2️⃣ Ou mande no chat:',
    'fadadoscupons/17687': '🚨🚨 Resgatem todos! 5 Cupons 30% OFF para o 10.10 Mercado Livre\n\n✅ Siga o tutorial:\n\n1️⃣ Entre no app do Meli aqui 👇\n\n2️⃣ Clique em "Mais" no rodapé do app\n3️⃣ Depois "Cupons relâmpago do 10.10"\n4️⃣ Resgate os cupons de 30%',
    'fadadoscupons/17690': '🚨 Novo Cupom Shopee VIP\n\n✅ Resgate aqui:\n\n\n🔝 Assine aqui (1 Mês Grátis):',
    'fadadoscupons/17695': '🔥 Cupom 40% OFF para Contas Selecionadas\n\n✅ Veja se recebeu:',
    'fadadoscupons/17696': '🔥 Novos Cupons Shopee Tech\n\n🎟 R$100 OFF em R$850\n\n\n🎟 R$60 OFF em R$450',
    'fadadoscupons/17712': '🔥Ainda ativo! Cupom Shopee VIP\n\n🎟 50% OFF. limite R$20\n\n✅ Resgate aqui:\n\n\n🎁 Ganhe de 1 a 3 meses grátis do VIP:',
    'fadadoscupons/17715': '🚨🚨 Vai sair cupom Shopee\n\n🛒 Prepara:\n\nSem info de valores',
    'fadadoscupons/17727': '🔥 Cupom Shopee Esgotando!\n\n🎟 R$40 OFF em compras acima de R$199\n\n✅ Envie o link do produto que precisa, entre no link gerado e selecione o cupom na finalização do pedido, na aba “Cupom de Desconto"\n\n1️⃣ Gere o link pelo site:\n\n\n2️⃣ Ou Gere no chat:',
    'fadadoscupons/17799': '🔥 Cupom 20% OFF Mercado Livre para Lojas Oficiais\n\nResgate agora e use 00h\n\n🚨🚨 TUTORIAL:\n1. Entre no APP 👇\n\n2. Clique em "Mais" no canto inferior direito\n3. Entre em “Cupons relâmpago do 10.10"\n4. Resgate o cupom',
    'fadadoscupons/17819': '🚨 Amanhã é Dia de Cupons na Shopee\n\n👇 Página do evento:\n\n\n🎁 Resgate cupons de frete:',
    'promotom/110992': 'Próximo Cupom 25% OFF Mercado Livre\n\n•Tutorial para resgate:\n1 Entre no APP do Meli aqui:\n2 Clique em "Mais" no rodapé do APP\n3 Depois clique em "Cupons relâmpago"\n4 Resgate o cupom',
    'promotom/111009': 'Próximo Cupom 25% OFF Mercado Livre\n\n•Tutorial para resgate:\n1 Entre no APP do Meli aqui:\n2 Clique em "Mais" no rodapé do APP\n3 Depois clique em "Cupons relâmpago"\n4 Resgate o cupom',
    'promotom/111079': 'Próximo Cupom 30% OFF Mercado Livre\n\n•Tutorial para resgate:\n1 Entre no APP do Meli aqui:\n2 Clique em "Mais" no rodapé do APP\n3 Depois clique em "Cupons relâmpago"\n4 Resgate o cupom',
    'promotom/111082': 'Cupons Shopee VIP, resgate agora e use meia-noite!\n\n-Resgate aqui:\n\n\nShopee VIP 1 mês grátis:',
    'promotom/111088': 'Cupons Mercado Livre e Shopee, resgate agora e use meia-noite!\n\n•+1 30% OFF Mercado Livre\n-Tutorial para resgate:\n1 Entre no APP do Meli aqui:\n2 Clique em "Mais" no rodapé do APP\n3 Depois clique em "Cupons relâmpago"\n4 Resgate o cupom\n\n•Frete Grátis Shopee\n-Resgate aqui:',
    'promotom/111102': 'Cupons Mercado Livre e Shopee, resgate agora e use meia-noite!\n\n•+1 30% OFF Mercado Livre\n-Tutorial para resgate:\n1 Entre no APP do Meli aqui:\n2 Clique em "Mais" no rodapé do APP\n3 Depois clique em "Cupons relâmpago"\n4 Resgate o cupom\n\n•60% Cashback Shopee\n-Resgate aqui:',
    'promotom/111103': 'Cupons Mercado Livre e Shopee, resgate agora e use meia-noite!\n\n•+1 30% OFF Mercado Livre\n-Tutorial para resgate:\n1 Entre no APP do Meli aqui:\n2 Clique em "Mais" no rodapé do APP\n3 Depois clique em "Cupons relâmpago"\n4 Resgate o cupom\n\n•70% Cashback Shopee\n-Resgate aqui:',
    'promotom/111105': 'Cupons Shopee\n\n-Resgate aqui:\n1\n2',
    'promotom/111161': 'Cupons Shopee Contas Selecionadas - 40% OFF Sem Mínimo!\n\n-Veja se recebeu aqui:',
    'promotom/111162': 'Cupons Shopee\n\nShopee Vip\n-Resgate aqui:\n\n\n-Teste Grátis:\n\n\nOutros Cupons\n-Resgate aqui:\n1\n2',
    'promotom/111169': 'Cupons Shopee\n\nShopee Vip\n-Resgate aqui:\n\n\n-Teste Grátis:\n\n\nOutros Cupons\n-Resgate aqui:\n1\n2',
    'promotom/111237': 'Cupom Mercado Livre\n\n15% OFF, Limite de R$ 60 OFF: OFERTAMELI\n15% OFF, Limite de R$ 40 OFF em DECORAÇÃO E ELETROS: DECOR1010\n\n-Resgate o cupom aqui:\n\n-Anúncio',
    'promotom/111243': 'Cupom Mercado Livre\n\n20% OFF, Limite de R$ 60 OFF em ITENS BEBÊS E PETS: BEBES1010\n\n-Resgate o cupom aqui:\n\n-Anúncio',
    'promotom/111252': 'Cupom Mercado Livre\n\n20% OFF, Limite de R$ 40 OFF em MODA E BELEZA: MODA1010\n\n-Resgate o cupom aqui:\n\n-Anúncio',
    'promotom/111254': 'Cupom Mercado Livre\n\n15% OFF, Limite de R$ 40 OFF em LOJAS OFICIAIS: LOJAS1010\n\n-Resgate o cupom aqui:\n\n-Anúncio',
    'promotom/111257': 'Próximo Cupom 25% OFF Mercado Livre\n\n•Tutorial para resgate:\n1 Entre no APP do Meli aqui:\n2 Clique em "Mais" no rodapé do APP\n3 Depois clique em "Cupons relâmpago"\n4 Resgate o cupom',
    'promotom/111295': 'Próximo Cupom 20% OFF Mercado Livre\n\n•Tutorial para resgate:\n1 Entre no APP do Meli aqui:\n2 Clique em "Mais" no rodapé do APP\n3 Depois clique em "Cupons relâmpago"\n4 Resgate o cupom',
}

# 111092 tem o MESMO texto da 111088 (prévia pública, conferido).
TEXTO["promotom/111092"] = TEXTO["promotom/111088"]
# Δ SINTÉTICO, derivado de texto real: candidato só-assinatura (sem nome)
# para provar a revalidação da adoção no modo ATIVO (teste 31).
TEXTO["sintetico/vip50_sem_nome"] = "🔥 Cupom 50% OFF Shopee\n\n✅ Resgate aqui:"


def _CL():
    """Import tardio: a bancada também roda na `main` (sem o módulo), para
    classificar as quedas; aí só os testes da regra pura caem por import."""
    import pipeline.cupom_lancamento as m
    return m


# ─────────────────────────────────────────────────────────────────
# Cenários reais — (chave do texto, plataforma, segundos desde o anterior)
# ─────────────────────────────────────────────────────────────────
CASO1 = [("promotom/111295", ML, 0), ("fadadoscupons/17799", ML, 64),
         ("SamuelF3lipePromo/121007", ML, 2)]               # 22:00:02 → 22:01:08
SERIE = [("promotom/111088", ML, 0), ("promotom/111092", ML, 59 * 60 + 37),
         ("promotom/111102", ML, 60 * 60 + 5),
         ("promotom/111103", ML, 59 * 60 + 45)]              # 22:59:53 … 01:59:20
E16 = [("SamuelF3lipePromo/120285", ML, 0),
       ("SamuelF3lipePromo/120304", ML, 1530)]               # Δ 25,5 min
F02 = [("promotom/110992", ML, 0), ("fadadoscupons/17631", ML, 198)]
F11 = [("promotom/111088", ML, 0), ("SamuelF3lipePromo/120285", ML, 16)]
F12 = [("promotom/111102", ML, 0), ("SamuelF3lipePromo/120342", ML, 78)]
F14_16 = [("promotom/111103", ML, 0), ("fadadoscupons/17687", ML, 48),
          ("SamuelF3lipePromo/120345", ML, 77)]              # série das 02h
F30 = [("fadadoscupons/17712", SHP, 0), ("SamuelF3lipePromo/120532", SHP, 264)]
F06 = [("promotom/111079", ML, 0), ("SamuelF3lipePromo/120276", ML, 210)]
JUNTAM = {"F02": F02, "F11": F11, "F12": F12, "F14_16": F14_16, "F30": F30}
CINZA = {   # zona cinzenta / sem regra: comportamento de hoje
    "F07": [("promotom/111082", SHP, 0), ("fadadoscupons/17674", SHP, 588)],
    "F45": [("promotom/111257", ML, 0), ("SamuelF3lipePromo/120804", ML, 648)],
    "F24": [("promotom/111169", SHP, 0), ("fadadoscupons/17712", SHP, 948)],
    "E05": [("SamuelF3lipePromo/120080", ML, 0), ("promotom/111009", ML, 1272)],
    "E19": [("promotom/111105", SHP, 0), ("fadadoscupons/17695", SHP, 1164)],
    "E45": [("fadadoscupons/17819", SHP, 0), ("SamuelF3lipePromo/121241", SHP, 894)],
    "F22": [("promotom/111162", SHP, 0), ("SamuelF3lipePromo/120480", SHP, 162)],
}
DISTINTOS = {   # campanhas diferentes: nunca juntam
    "F13": [("SamuelF3lipePromo/120342", ML, 0), ("fadadoscupons/17687", ML, 3552)],
    "F19": [("fadadoscupons/17690", SHP, 0), ("fadadoscupons/17696", SHP, 1194)],
    "F20": [("promotom/111161", SHP, 0), ("promotom/111162", SHP, 150)],
    "F21": [("promotom/111161", SHP, 0), ("SamuelF3lipePromo/120480", SHP, 312)],
    "F26": [("promotom/111169", SHP, 0), ("fadadoscupons/17715", SHP, 3432)],
    "F38": [("promotom/111237", ML, 0), ("promotom/111243", ML, 3588)],
    "F43": [("promotom/111252", ML, 0), ("promotom/111254", ML, 3594)],
}
# Δ SINTÉTICO (20 min; o real foi 12 h): lembrete da MESMA fonte, mesmo cupom.
LEMBRETE = [("fadadoscupons/17685", SHP, 0), ("fadadoscupons/17727", SHP, 1200)]
LEMB_SAM = [("SamuelF3lipePromo/120344", SHP, 0),
            ("SamuelF3lipePromo/120585", SHP, 1200)]
# Δ SINTÉTICO (3 min): a mesma fonte repete o mesmo texto como mensagem nova.
RAJADA_MESMA = [("promotom/111088", ML, 0), ("promotom/111088", ML, 180)]
TODOS = dict({"CASO1": CASO1, "SERIE": SERIE, "E16": E16, "F06": F06,
              "LEMBRETE": LEMBRETE, "LEMB_SAM": LEMB_SAM,
              "RAJADA_MESMA": RAJADA_MESMA}, **JUNTAM, **CINZA, **DISTINTOS)


# ─────────────────────────────────────────────────────────────────
# Infra da bancada
# ─────────────────────────────────────────────────────────────────
_RE_MARCA_NOVA = re.compile(r"\[CUPOM_(?:ENCONTRO|LIBERACAO|INCERTO|LANCAMENTO)")


class _Linhas(logging.Handler):
    """Só as linhas da regra nova (CUPOM_ADOTA e afins são de hoje)."""
    def __init__(self):
        super().__init__()
        self.linhas = []

    def emit(self, rec):
        m = rec.getMessage()
        if _RE_MARCA_NOVA.search(m):
            self.linhas.append(m)


class _Modo:
    """Liga um modo e um conjunto de remendos só durante o cenário."""
    def __init__(self, modo, remendos=()):
        self.modo, self.remendos, self.antes = modo, list(remendos), []

    def __enter__(self):
        self.antigo = getattr(config, "CUPOM_MODO", None)
        config.CUPOM_MODO = self.modo
        for obj, nome, valor in self.remendos:
            self.antes.append((obj, nome, getattr(obj, nome, None)))
            setattr(obj, nome, valor)
        return self

    def __exit__(self, *_):
        for obj, nome, valor in reversed(self.antes):
            setattr(obj, nome, valor)
        if self.antigo is None:
            delattr(config, "CUPOM_MODO")
        else:
            config.CUPOM_MODO = self.antigo


def _recuar(seg):
    """O tempo passa `seg` segundos para todos os posts (só no banco temporário)."""
    with _db() as db:
        db.execute("UPDATE post_estado SET janela_fim = janela_fim - ?,"
                   " ts = ts - ?", (seg, seg))


def _lento(c, s=0.05):
    envio, arquivo, edicao = c.send_message, c.send_file, c.edit_message

    async def send_message(*a, **k):
        await asyncio.sleep(s)
        return await envio(*a, **k)

    async def send_file(*a, **k):
        await asyncio.sleep(s)
        return await arquivo(*a, **k)

    async def edit_message(*a, **k):
        await asyncio.sleep(s)
        return await edicao(*a, **k)
    c.send_message, c.send_file, c.edit_message = send_message, send_file, edit_message


def _norm(chave, plat):
    fonte = chave.split("/")[0]
    chat = CHAT.get(fonte, FADA)
    urls = VEICULO.get(fonte, ()) if plat == ML else ()
    return msg(chat, TEXTO[chave], urls_longas=urls, plat=plat)


def _foto(c, ns):
    """Retrato comparável: ids de post renumerados pela ordem de criação."""
    from database import db_get_post, db_ofertas_de_post
    rank = {m: i for i, m in enumerate(c.ids)}
    estados = []
    for m in c.ids:
        e = db_get_post(m) or {}
        estados.append((e.get("score"), e.get("texto"), e.get("edit_count"),
                        bool(e.get("fused_into")),
                        tuple(sorted(db_ofertas_de_post(m)))))
    return {"novos": c.novos,
            "posts": [rank.get(origem.consultar(n.chat, n.msg_id)) for n in ns],
            "edits": [(rank.get(m, m), t) for m, t in c.edits],
            "deletes": [rank.get(m, m) for m in c.deletes],
            "estados": estados, "motivos": list(MOTIVOS)}


def _cena(passos, modo="SOMBRA", remendos=(), juntos=False):
    h = _Linhas()
    log_out.addHandler(h)
    out = {}
    try:
        with _Modo(modo, remendos):
            async def corpo(c):
                ns = [_norm(k, p) for k, p, _a in passos]
                if juntos:
                    _lento(c)
                    await asyncio.gather(*(publicar(n) for n in ns))
                else:
                    for n, (_k, _p, avanco) in zip(ns, passos):
                        if avanco:
                            _recuar(avanco)
                        await publicar(n)
                out["foto"] = _foto(c, ns)
            cenario(corpo)
    finally:
        log_out.removeHandler(h)
    out["linhas"] = h.linhas
    return out


def _desligado():
    """A família de hoje, sem a regra nova (é o comportamento da `main`)."""
    return [(familia, "lancamento_cupom",
             lambda ofertas, texto, chat, msg_id, familia_hoje, agora=None, **_kw:
             (familia_hoje, False))]


def _explode(*a, **k):
    raise RuntimeError("sabotagem")


def _sabotagens():
    """Cada ponto da regra nova que pode falhar em produção: a leitura do
    banco, a decisão e a escrita da linha. Em SOMBRA nenhum muda nada."""
    return {"banco": [(familia, "db_origens_do_post", _explode)],
            "decisao": [(_CL(), "classificar", _explode)],
            "registro": [(familia, "_registrar_lancamento", _explode)]}


def _marcas(linhas):
    """(marcador, motivo) de cada linha; linha sem motivo (ex.: o erro
    CUPOM_LANCAMENTO) entra com o texto inteiro, para o teste mostrar."""
    out = []
    for l in linhas:
        m, mo = re.search(r"\[(CUPOM_\w+)\]", l), re.search(r"motivo=(\S+)", l)
        out.append((m.group(1) if m else l, mo.group(1) if mo else l))
    return out


# ══════════════════════════════════════════════════════════════════
# 01–05 — regra pura
# ══════════════════════════════════════════════════════════════════
def test_01_nome_sem_tempo_nem_chamada_cada_palavra_com_o_caso(r):
    CL = _CL()
    casos = [  # (texto real, plataforma, nome esperado na comparação)
        (TEXTO["promotom/111295"], ML, ""),                  # proximo
        (TEXTO["SamuelF3lipePromo/121007"], ML, "loj-oficiai"),
        (TEXTO["fadadoscupons/17799"], ML, "loj-oficiai"),
        (TEXTO["promotom/111082"], SHP, "vip"),              # use
        (TEXTO["promotom/111088"], ML, ""),                  # use + shopee
        (TEXTO["fadadoscupons/17712"], SHP, "vip"),          # ainda
        (TEXTO["SamuelF3lipePromo/120532"], SHP, "vip"),     # ainda
        (TEXTO["fadadoscupons/17687"], ML, "10.10"),         # resgatem todos
        (TEXTO["fadadoscupons/17674"], SHP, "vip"),          # quem
        (TEXTO["promotom/111161"], SHP, "cont-selecionad"),   # sem
        (TEXTO["SamuelF3lipePromo/120479"], SHP, "cont-selecionad"),
    ]   # o radical é só para comparar ("lojas" → "loj"); nunca vira chave
    for texto, plat, esperado in casos:
        got = CL.perfil(texto, plat).nome
        r.check(got == esperado, f"01.{texto.splitlines()[0][:40]!r}", got)
    r.check(CL.perfil("🔥 Shopee Figurinhas, ganhe cupons de até R$ 100 OFF",
                      SHP).nome == "figurinh", "01.ganhe",
            CL.perfil("🔥 Shopee Figurinhas, ganhe cupons de até R$ 100 OFF", SHP).nome)


def test_02_radical_e_nomes_que_seguem_diferentes(r):
    CL = _CL()
    n = lambda t, p=SHP: CL.perfil(t, p).nome
    r.check(n("Cupons Shopee Black Friday") != n("Cupons Shopee Black Week"),
            "02.black_friday_x_black_week")
    r.check(n("Cupons Shopee Clube Fidelidade") != n("Cupons Shopee Fidelidade"),
            "02.clube_fidelidade_x_fidelidade")
    r.check(n("Novo Cupom Shopee Tech") != n("Cupom Shopee Tecnologia"),
            "02.tech_x_tecnologia_segue_diferente")
    r.check(n("Cupons Mercado Livre e Shopee, resgate agora", ML) == "",
            "02.outra_plataforma_nao_e_nome", n("Cupons Mercado Livre e Shopee, resgate agora", ML))
    r.check(n("Cupom Mercado Livre e Shopee Moda") == "mod",
            "02.mercado_livre_num_post_shopee_nao_e_nome",
            n("Cupom Mercado Livre e Shopee Moda"))


def test_03_beneficio_e_lembrete(r):
    CL = _CL()
    p = CL.perfil
    r.check(p(TEXTO["promotom/111295"], ML).pct == "20", "03.pct_20")
    r.check(p(TEXTO["fadadoscupons/17685"], SHP).valores == frozenset({"v:40-199"}),
            "03.valor_40_199", str(p(TEXTO["fadadoscupons/17685"], SHP).valores))
    for chave in ("fadadoscupons/17727", "SamuelF3lipePromo/120585",   # esgotando
                  "fadadoscupons/17712", "SamuelF3lipePromo/120532",   # ainda ativo
                  "fadadoscupons/17685", "SamuelF3lipePromo/120344"):  # lembrando
        r.check(p(TEXTO[chave], SHP).lembrete, f"03.lembrete.{chave}")
    for t in ("Cupom Shopee Voltando! R$ 35 OFF em R$ 170",          # 111176
              "Última chance! Shopee Figurinhas",                     # 119837
              "Cupom Shopee Renovado! R$40 OFF em compras acima de R$199"):
        r.check(p(t, SHP).lembrete, f"03.lembrete.{t[:20]}")
    for chave in ("promotom/111088", "SamuelF3lipePromo/120285", "promotom/111295"):
        r.check(not p(TEXTO[chave], ML).lembrete, f"03.serie_nao_e_lembrete.{chave}")


def test_04_escopo_do_candidato(r):
    CL = _CL()
    r.check(CL.candidato(["shopee|cupb|vip"]) == "shopee", "04.cupb")
    r.check(CL.candidato(["shopee|cupb|v:30-299", "shopee|cupb|v:90-899"]) == "shopee",
            "04.varias_assinaturas")
    for fora in (["shopee|cup|X1"], ["mercadolivre|MLB1"], ["shopee|cash|50"],
                 ["shopee|cupb|vip", "shopee|cup|X1"],
                 ["shopee|cupb|vip", "mercadolivre|cupb|vip"], [],
                 ["mercadolivre|dest|lista:1"]):
        r.check(CL.candidato(fora) == "", f"04.fora.{fora}")


def test_05_tabela_de_decisao(r):
    CL = _CL()
    P = CL.Perfil
    R, SUP, T = 480.0, 1800.0, 100000.0
    vip50 = P("vip", "50", frozenset(), False)
    sem = P("", "", frozenset(), False)
    pct50 = P("", "50", frozenset(), False)
    lemb = P("", "50", frozenset(), True)

    def vivo(perfil, idade, fontes=("A",), mid=1):
        return CL.PostVivo(mid, perfil, T - idade, frozenset(fontes))

    def dec(c, chat, vivos, hoje):
        return CL.classificar(c, chat, T, vivos, hoje, R, SUP)
    casos = [
        ("mesma_fonte_liberacao", dec(pct50, "A", [vivo(pct50, 1530)], 1), ("NOVA", "mesma_fonte")),
        ("mesma_fonte_na_rajada", dec(pct50, "A", [vivo(pct50, 180)], 1), ("INCERTO", "mesma_fonte_na_rajada")),
        ("lembrete_mesma_fonte", dec(lemb, "A", [vivo(pct50, 1200)], 1), ("INCERTO", "lembrete_mesma_fonte")),
        ("acima_do_superior", dec(pct50, "B", [vivo(pct50, 3300)], 1), ("NOVA", "acima_do_superior")),
        ("faixa_cinzenta_com_familia", dec(pct50, "B", [vivo(pct50, 900)], 1), ("INCERTO", "faixa_cinzenta")),
        ("junta_sem_familia", dec(pct50, "B", [vivo(vip50, 60)], None), ("JUNTA", "mesmo_lancamento")),
        ("ambiguo", dec(pct50, "B", [vivo(vip50, 60, mid=1), vivo(pct50, 30, mid=2)], None), ("INCERTO", "ambiguo")),
        ("nome_conflitante", dec(P("tech", "50", frozenset(), False), "B", [vivo(vip50, 60)], None), ("MANTER", "sem_lancamento_vivo")),
        ("beneficio_diferente_com_familia", dec(P("vip", "30", frozenset(), False), "B", [vivo(vip50, 60)], 1), ("NOVA", "beneficio_diferente")),
        ("sem_beneficio_sem_familia", dec(sem, "B", [vivo(vip50, 60)], None), ("INCERTO", "sem_beneficio")),
        ("sem_beneficio_com_familia_e_hoje", dec(sem, "B", [vivo(vip50, 60)], 1), ("MANTER", "sem_beneficio")),
        ("valor_nome_x_sem_nome", dec(P("", "", frozenset({"v:30-299"}), False), "B",
                                      [vivo(P("tech", "", frozenset({"v:30-299"}), False), 30)], None),
         ("INCERTO", "valor_nome_x_sem_nome")),
        ("familia_fora_da_regra", dec(pct50, "B", [], 7), ("MANTER", "familia_fora_da_regra")),
    ]
    for nome, d, (acao, motivo) in casos:
        r.check((d.acao, d.motivo) == (acao, motivo), f"05.{nome}", f"{d.acao}/{d.motivo}")


def test_06_plataforma_sem_medicao_fica_fora_sem_ler_o_banco(r):
    # CUPOM_RAJADA_S só tem ML e Shopee (as medidas); Amazon fica fora,
    # mesmo em ATIVO, e nem consulta o banco.
    lidas = []

    def conta(*a, **k):
        lidas.append(a)
        return []
    with _Modo("ATIVO", [(familia, "db_posts_vivos_com_prefixo", conta)]):
        got = familia.lancamento_cupom(["amazon|cupb|prime"], "Cupom Amazon Prime 20% OFF",
                                       "-100", 1, 7, agora=1e9)
        fora = familia.lancamento_cupom(["shopee|cup|X1"], "Cupom Shopee X1", "-100", 2,
                                        None, agora=1e9)
        # Escopo da adoção: destino declarado (FAMILIA_OUTRO_DESTINO) e
        # container de live (Frente LIVE) têm outra autoridade.
        destino = familia.lancamento_cupom(["shopee|cupb|vip"], "Cupom Shopee VIP", "-100",
                                           3, 7, agora=1e9,
                                           destinos=("shopee|dest|loja:1",))
        live = familia.lancamento_cupom(["shopee|cupb|vip"], "Cupom Shopee VIP", "-100",
                                        4, None, agora=1e9,
                                        container="shopee|url|live1")
    r.check(got == (7, False), "06.amazon_fora", str(got))
    r.check(fora == (None, False), "06.com_codigo_fora", str(fora))
    r.check(destino == (7, False), "06.com_destino_fora", str(destino))
    r.check(live == (None, False), "06.com_container_fora", str(live))
    r.check(not lidas, "06.nao_leu_o_banco", str(lidas))


def test_07_filtro_de_posts_nao_muda_a_decisao(r):
    # pode_mudar só serve para não calcular perfil à toa (D.5): sem
    # família, classificar com TODOS os vivos = com os filtrados, numa
    # grade de fonte × idade × nome × benefício × lembrete.
    import itertools
    CL = _CL()
    P = CL.Perfil
    R, SUP, T = 480.0, 1800.0, 100000.0
    perfis = [P("vip", "50", frozenset(), False), P("", "50", frozenset(), False),
              P("tech", "50", frozenset(), False), P("vip", "30", frozenset(), False),
              P("", "", frozenset({"v:30-299"}), False), P("", "", frozenset(), False),
              P("", "50", frozenset(), True)]
    idades = [60, 300, 479, 481, 900, 1799, 1801, 3300]
    postes = [CL.PostVivo(i, pf, T - idade, frozenset({fonte}))
              for i, (pf, idade, fonte) in enumerate(
                  itertools.product(perfis, idades, ("A", "B")))]
    casos = 0
    for c in perfis:
        for k in (1, 2, 3):
            for grupo in itertools.combinations(postes[::5], k):
                cheio = CL.classificar(c, "A", T, list(grupo), None, R, SUP)
                filtrado = [p for p in grupo
                            if CL.pode_mudar("A", T, p.nascimento, p.fontes, SUP)]
                fil = CL.classificar(c, "A", T, filtrado, None, R, SUP)
                casos += 1
                if cheio != fil:
                    r.check(False, "07.diferente", f"{c} {grupo} {cheio} {fil}")
                    return
    r.check(casos > 1000, "07.grade_grande", str(casos))
    r.check(True, "07.todos_iguais", str(casos))


def test_08_sem_acento_rapido_igual_ao_de_referencia(r):
    import unicodedata
    CL = _CL()

    def referencia(s):
        return "".join(c for c in unicodedata.normalize("NFD", s)
                       if unicodedata.category(c) != "Mn")
    amostras = list(TEXTO.values()) + [
        "", "ascii puro", "Última chance! Ç Ã Õ É ü ñ", "⚠️ Lembrando!",
        "🔥Ainda ativo", "a\u0301\u0302\u20dd", "1\ufe0f\u20e3 Gere", "İstanbul",
        "".join(chr(i) for i in range(0x80, 0x2000))]
    for s in amostras:
        r.check(CL._sem_acento(s) == referencia(s), f"08.{s[:16]!r}")
        r.check(CL._sem_acento(s.lower()) == referencia(s.lower()),
                f"08.lower.{s[:16]!r}")


def test_09_decisao_so_le_os_campos_que_usa(r):
    # D.5: o perfil vindo do texto calcula cada campo na primeira leitura.
    # A DECISÃO (sem a linha de registro, que imprime tudo) não lê o que
    # não usa: mesma fonte só o lembrete do candidato; pct × pct, nunca a
    # assinatura de valor.
    CL = _CL()
    T0 = 100000.0

    def lidos(pf):
        return sorted(k for k, v in pf._campos.items() if v is not CL._FALTA)
    c = CL.Perfil(texto=TEXTO["SamuelF3lipePromo/120304"], plat=ML)
    p = CL.Perfil(texto=TEXTO["SamuelF3lipePromo/120285"], plat=ML)
    d = CL.classificar(c, "S", T0, [CL.PostVivo(1, p, T0 - 1530, frozenset({"S"}))],
                       1, 480.0, 1800.0)
    r.check(d.acao == "NOVA", "09.mesma_fonte.nova", d.acao)
    r.check(lidos(c) == ["lembrete"], "09.mesma_fonte.candidato_so_lembrete", str(lidos(c)))
    r.check(lidos(p) == [], "09.mesma_fonte.post_nada", str(lidos(p)))
    c = CL.Perfil(texto=TEXTO["fadadoscupons/17799"], plat=ML)
    p = CL.Perfil(texto=TEXTO["promotom/111295"], plat=ML)
    d = CL.classificar(c, "F", T0, [CL.PostVivo(1, p, T0 - 64, frozenset({"P"}))],
                       None, 480.0, 1800.0)
    r.check(d.acao == "JUNTA", "09.pct_x_pct.junta", d.acao)
    r.check("valores" not in lidos(c) + lidos(p), "09.pct_x_pct.sem_valores",
            f"{lidos(c)} {lidos(p)}")
    r.check(c == CL.Perfil(c.nome, c.pct, c.valores, c.lembrete),
            "09.preguicoso_igual_ao_explicito")


# ══════════════════════════════════════════════════════════════════
# 10–12 — SOMBRA: comportamento idêntico, decisão registrada
# ══════════════════════════════════════════════════════════════════
def test_10_sombra_igual_a_desligado_igual_a_sabotado(r):
    acertos = {}   # a sabotagem precisa ter disparado (senão o teste é vazio)
    for nome, passos in sorted(TODOS.items()):
        sombra = _cena(passos, "SOMBRA")["foto"]
        desl = _cena(passos, "SOMBRA", _desligado())["foto"]
        r.check(sombra == desl, f"10.{nome}.sombra_igual_desligado",
                f"{sombra['novos']} × {desl['novos']}")
        try:
            sabotagens = _sabotagens()
        except ImportError:
            sabotagens = {"sem_modulo": None}
        for onde, remendos in sorted(sabotagens.items()):
            cena = _cena(passos, "SOMBRA", remendos) if remendos else {}
            r.check(cena.get("foto") == desl,
                    f"10.{nome}.sabotado_{onde}_igual_desligado")
            acertos[onde] = acertos.get(onde, 0) + sum(
                "[CUPOM_LANCAMENTO]" in l for l in cena.get("linhas", ()))
    for onde, n in sorted(acertos.items()):
        r.check(n > 0, f"10.sabotagem_{onde}_disparou", str(n))


def test_11_sombra_registra_a_decisao(r):
    esperado = {
        "CASO1": [("CUPOM_ENCONTRO_SOMBRA", "mesmo_lancamento"),
                  ("CUPOM_INCERTO", "ambiguo")],
        "SERIE": [("CUPOM_LIBERACAO_SOMBRA", "mesma_fonte"),
                  ("CUPOM_LIBERACAO_SOMBRA", "mesma_fonte")],
        "E16": [("CUPOM_LIBERACAO_SOMBRA", "mesma_fonte")],
        "F07": [("CUPOM_INCERTO", "faixa_cinzenta")],
        "F45": [("CUPOM_INCERTO", "faixa_cinzenta")],
        "E05": [("CUPOM_INCERTO", "faixa_cinzenta")],
        "E19": [("CUPOM_INCERTO", "faixa_cinzenta")],
        # A família de hoje já junta (mesma chave `vip`) e a regra não se
        # opõe (sem benefício no post): silêncio, nada a calibrar.
        "F22": [],
        "LEMBRETE": [("CUPOM_INCERTO", "lembrete_mesma_fonte")],
        "RAJADA_MESMA": [("CUPOM_INCERTO", "mesma_fonte_na_rajada")],
    }
    for nome, marcas in esperado.items():
        got = _marcas(_cena(TODOS[nome], "SOMBRA")["linhas"])
        r.check(got == marcas, f"11.{nome}", str(got))
    for nome in DISTINTOS:
        got = _marcas(_cena(DISTINTOS[nome], "SOMBRA")["linhas"])
        r.check(not any(m == "CUPOM_ENCONTRO_SOMBRA" for m, _ in got),
                f"11.distinto_nunca_encontra.{nome}", str(got))
    for nome in JUNTAM:
        got = _marcas(_cena(JUNTAM[nome], "SOMBRA")["linhas"])
        r.check(("CUPOM_ENCONTRO_SOMBRA", "mesmo_lancamento") in got,
                f"11.junta_registrado.{nome}", str(got))
    linha = _cena(CASO1, "SOMBRA")["linhas"][0]
    r.check("http" not in linha and "Tutorial" not in linha,
            "11.linha_sem_url_nem_texto", linha)


def test_12_sombra_nao_muda_os_locks(r):
    # Os mutexes de adoção de hoje: só-geral e só-assinatura serializam por
    # `<plat>|cupb|*`; nome declarado não. Em SOMBRA, idêntico; em ATIVO,
    # todo candidato só-`cupb` entra no mesmo mutex.
    casos = [(["shopee|cupb|vip"], [], ["shopee|cupb|*"]),
             (["mercadolivre|cupb|proximo"], [], ["mercadolivre|cupb|*"]),
             (["shopee|cupb|geral"], ["shopee|cupb|*"], ["shopee|cupb|*"]),
             (["shopee|cupb|p:50"], ["shopee|cupb|*"], ["shopee|cupb|*"]),
             (["shopee|cupb|vip", "shopee|cup|X1"], [], []),
             (["mercadolivre|MLB1"], [], [])]
    for ofs, sombra, ativo in casos:
        with _Modo("SOMBRA"):
            got_s = familia.mutexes_de_adocao(ofs)
        with _Modo("ATIVO"):
            got_a = familia.mutexes_de_adocao(ofs)
        r.check(got_s == sombra, f"12.sombra.{ofs}", str(got_s))
        r.check(got_a == ativo, f"12.ativo.{ofs}", str(got_a))


def test_14_perfil_so_dos_posts_que_podem_mudar_a_decisao(r):
    # D.5: o perfil (a parte cara) só é calculado para o candidato e para
    # os posts que podem mudar a decisão — o da família, ou, sem família,
    # os de OUTRA fonte nascidos há ≤ SUPERIOR.
    from database import db_get_post
    CL = _CL()

    def roda(antes, candidato):
        textos, out = [], {}
        orig = CL.perfil

        def conta(texto, plat):
            textos.append(texto)
            return orig(texto, plat)

        async def corpo(c):
            for k, p, avanco in antes:
                if avanco:
                    _recuar(avanco)
                await publicar(_norm(k, p))
            k, p, avanco = candidato
            _recuar(avanco)
            n = _norm(k, p)
            CL.perfil = conta
            try:
                await publicar(n)
            finally:
                CL.perfil = orig
            out["posts"] = {m: (db_get_post(m) or {}).get("texto") for m in c.ids}
            out["cand"] = n.texto_analise
        with _Modo("SOMBRA"):
            cenario(corpo)
        return textos, out
    # (a) sem família: post velho (42 min) de outra fonte fica sem perfil;
    #     o recente (2 min) de outra fonte é calculado.
    textos, out = roda([("promotom/110992", ML, 0), ("fadadoscupons/17799", ML, 2400)],
                       ("SamuelF3lipePromo/121007", ML, 120))
    velho, recente = list(out["posts"].values())[:2]
    r.check(out["cand"] in textos and recente in textos, "14.a.candidato_e_recente",
            str(len(textos)))
    r.check(velho not in textos, "14.a.velho_sem_perfil")
    # (b) sem família e só post da MESMA fonte ou velho: perfil nenhum.
    textos, _ = roda([("promotom/110992", ML, 0), ("fadadoscupons/17799", ML, 2400)],
                     ("fadadoscupons/17631", ML, 120))
    r.check(textos == [], "14.b.nenhum_perfil", str(len(textos)))
    # (c) com família: só o post da família (e o candidato).
    textos, out = roda([("fadadoscupons/17799", ML, 0),
                        ("SamuelF3lipePromo/120285", ML, 30)],
                       ("SamuelF3lipePromo/120304", ML, 1530))
    outro, da_familia = list(out["posts"].values())[:2]
    r.check(da_familia in textos and out["cand"] in textos, "14.c.familia_e_candidato",
            str(len(textos)))
    r.check(outro not in textos, "14.c.outro_post_sem_perfil")


def test_15_familia_sem_ancora_cupb_fica_como_hoje(r):
    # Defesa: hoje a família de um candidato só-`cupb` sempre tem âncora
    # `cupb` (prova no comentário de lancamento_cupom). Se um caminho
    # futuro der família de PRODUTO, a regra não a julga — nem em ATIVO
    # (mesma fonte, 20 min depois, seria NOVA e quebraria a família).
    out = {}

    async def corpo(c):
        n = msg(PROMOTOM, "Fone Bluetooth XYZ por R$ 99 no Mercado Livre",
                urls_longas=VEICULO["promotom"], plat=ML)
        await publicar(n)
        post = origem.consultar(n.chat, n.msg_id)
        _recuar(1200)
        h = _Linhas()
        log_out.addHandler(h)
        try:
            out["res"] = familia.lancamento_cupom(
                ["mercadolivre|cupb|geral"], TEXTO["promotom/111295"], PROMOTOM,
                999, post)
        finally:
            log_out.removeHandler(h)
        out["post"], out["linhas"] = post, h.linhas
    with _Modo("ATIVO"):
        cenario(corpo)
    r.check(out["post"] is not None, "15.post_de_produto_existe")
    r.check(out["res"] == (out["post"], False), "15.familia_de_hoje", str(out["res"]))
    r.check(not out["linhas"], "15.sem_linha", str(out["linhas"]))


def test_13_regra_recebe_o_mesmo_escopo_da_familia(r):
    # Fiação: o destino e o container que a família usou chegam à regra
    # (sem isso, o teste 06 provaria a função e não o pipeline). Os
    # valores são forçados na entrada do pipeline (enriquecimento e
    # container) para que um valor constante na chamada não passe.
    import dataclasses
    import test_cupom_sem_codigo as base
    from pipeline import publicacao
    orig_fam, orig_regra = familia.post_da_familia, familia.lancamento_cupom
    orig_enr = base.enriquecer
    com_destino = [(base, "enriquecer", lambda n: dataclasses.replace(
        orig_enr(n), destinos=("mercadolivre|dest|sonda",)))]
    com_live = [(publicacao, "_container_de",
                 lambda norm, ofertas: "mercadolivre|url|sonda")]
    for nome, extra, esperado in (("comum", [], ((), "")),
                                  ("destino", com_destino,
                                   (("mercadolivre|dest|sonda",), "")),
                                  ("live", com_live, ((), "mercadolivre|url|sonda"))):
        vistos = _escopos(CASO1, extra, orig_fam, orig_regra)
        r.check(len(vistos["regra"]) == 3, f"13.{nome}.tres_chamadas", str(vistos))
        r.check(vistos["regra"] == vistos["familia"], f"13.{nome}.mesmo_escopo",
                str(vistos))
        r.check(all(v == esperado for v in vistos["regra"]),
                f"13.{nome}.valor_chegou", str(vistos["regra"]))


def _escopos(passos, extra, orig_fam, orig_regra):
    vistos = {"familia": [], "regra": []}

    def fam(ofertas, dest_fix=None, destinos=(), titulo="", container=""):
        vistos["familia"].append((tuple(destinos), container))
        return orig_fam(ofertas, dest_fix, destinos, titulo=titulo,
                        container=container)

    def regra(*a, destinos=None, container=None, **k):
        vistos["regra"].append((tuple(destinos) if destinos is not None
                                else None, container))
        return orig_regra(*a, destinos=destinos or (),
                          container=container or "", **k)
    _cena(passos, "SOMBRA", [(familia, "post_da_familia", fam),
                             (familia, "lancamento_cupom", regra)] + extra)
    return vistos


# ══════════════════════════════════════════════════════════════════
# 20–31 — ATIVO (os que mudam o resultado caem na `main`)
# ══════════════════════════════════════════════════════════════════
def test_20_ativo_caso1_um_post(r):
    f = _cena(CASO1, "ATIVO")["foto"]
    r.check(f["novos"] == 1, "20.um_post", f"novos={f['novos']} posts={f['posts']}")
    r.check(f["posts"] == [0, 0, 0], "20.tres_mensagens_no_mesmo_post", str(f["posts"]))


def test_21_ativo_serie_do_10_10_um_post_por_liberacao(r):
    f = _cena(SERIE, "ATIVO")["foto"]
    r.check(f["novos"] == 4, "21.quatro_posts", f"novos={f['novos']} motivos={f['motivos']}")
    r.check(f["posts"] == [0, 1, 2, 3], "21.cada_liberacao_no_seu_post", str(f["posts"]))


def test_22_ativo_mesma_fonte_25_min_depois_e_liberacao(r):
    f = _cena(E16, "ATIVO")["foto"]
    r.check(f["novos"] == 2, "22.dois_posts", f"novos={f['novos']}")


def test_23_ativo_falsos_negativos_juntam(r):
    for nome, passos in JUNTAM.items():
        f = _cena(passos, "ATIVO")["foto"]
        r.check(f["novos"] == 1, f"23.{nome}.um_post",
                f"novos={f['novos']} posts={f['posts']}")


def test_24_regressao_f06_continua_junto(r):
    # Na produção o Samuel 120276 nasceu como PRODUTO (texto original
    # indisponível) e virou o post 27031. Com o texto atual a `main` já
    # junta: regressão, não reprodução — o caso real fica na sombra.
    for modo in ("SOMBRA", "ATIVO"):
        f = _cena(F06, modo)["foto"]
        r.check(f["novos"] == 1, f"24.{modo}.um_post", f"novos={f['novos']}")


def test_25_ativo_zona_cinzenta_e_o_comportamento_de_hoje(r):
    for nome, passos in CINZA.items():
        hoje = _cena(passos, "SOMBRA", _desligado())["foto"]
        ativo = _cena(passos, "ATIVO")["foto"]
        r.check(ativo == hoje, f"25.{nome}.igual_a_hoje",
                f"{ativo['novos']} × {hoje['novos']}")


def test_26_ativo_distintos_nunca_juntam(r):
    for nome, passos in DISTINTOS.items():
        f = _cena(passos, "ATIVO")["foto"]
        r.check(f["novos"] == 2, f"26.{nome}.dois_posts", f"novos={f['novos']}")


def test_27_ativo_lembrete_da_mesma_fonte_nao_e_liberacao(r):
    for nome, passos in (("fada", LEMBRETE), ("samuel", LEMB_SAM)):
        hoje = _cena(passos, "SOMBRA", _desligado())["foto"]
        ativo = _cena(passos, "ATIVO")["foto"]
        r.check(ativo["novos"] == 1 and ativo == hoje, f"27.{nome}.um_post_como_hoje",
                f"{ativo['novos']} × {hoje['novos']}")


def test_28_ativo_mesma_fonte_dentro_da_rajada_fica_como_hoje(r):
    hoje = _cena(RAJADA_MESMA, "SOMBRA", _desligado())["foto"]
    ativo = _cena(RAJADA_MESMA, "ATIVO")["foto"]
    r.check(ativo == hoje and ativo["novos"] == 1, "28.um_post_como_hoje",
            f"{ativo['novos']} × {hoje['novos']}")


def test_29_ativo_preserva_os_testes_fechados(r):
    t08 = [("t08a", "🔥 Cupons Shopee Tech\n\n🎟 R$30 OFF em R$299", FADA),
           ("t08b", "🔥 Cupons Shopee\n\n🎟 R$30 OFF em R$299", SAMUEL)]
    t10a = [("t10a", "🔥 Novos Cupons Shopee Fidelidade (APP)\n\nVários cupons", FADA),
            ("t10b", "🔥 Novos Cupons Shopee\n\n🎟 R$30 OFF em R$299\n🎟 R$90 OFF em R$899", SAMUEL)]
    t10b = [("t10c", "🔥 Novo Cupom Shopee Tech\n\nResgate", PROMOTOM),
            ("t10d", "Cupons Shopee\n\n-Resgate aqui:", FADA)]
    seco = "Cupons Shopee\n\n-Resgate aqui:"
    tech = "🔥 Novo Cupom Shopee Tech\n\n🎟 R$100 OFF em R$999"
    tE1 = [("e1", seco, PROMOTOM), ("e2", tech, FADA)]
    tE2 = [("e3", tech, PROMOTOM), ("e4", seco, FADA)]
    for nome, passos in (("08", t08), ("10.nome_x_assinatura", t10a),
                         ("10.nome_x_geral", t10b), ("E.seco_tech", tE1),
                         ("E.tech_seco", tE2)):
        for k, t, _c in passos:
            TEXTO[f"fechado/{k}"] = t
        chats = {f"fechado/{k}": c for k, _t, c in passos}
        cena = [(f"fechado/{k}", SHP, 0) for k, _t, _c in passos]
        orig = _norm

        def _norm_fechado(chave, plat, _orig=orig, _chats=chats):
            if chave in _chats:
                return msg(_chats[chave], TEXTO[chave], plat=plat)
            return _orig(chave, plat)
        globals()["_norm"] = _norm_fechado
        try:
            f = _cena(cena, "ATIVO")["foto"]
        finally:
            globals()["_norm"] = orig
        r.check(f["novos"] == 2, f"29.{nome}.dois_posts", f"novos={f['novos']}")


def test_30_ativo_corrida_mesmo_lancamento_um_post(r):
    passos = [("promotom/111295", ML, 0), ("fadadoscupons/17799", ML, 0)]
    f = _cena(passos, "ATIVO", juntos=True)["foto"]
    r.check(f["novos"] == 1, "30.um_post", f"novos={f['novos']} posts={f['posts']}")
    r.check(len(set(f["posts"])) == 1, "30.mesmo_post", str(f["posts"]))


def test_31_ativo_familia_da_regra_nao_cai_na_revalidacao_da_adocao(r):
    # Post nomeado (Samuel 120532, `vip-ainda`) + candidato só-assinatura
    # (p:50) de outra fonte: a regra junta; a revalidação da ADOÇÃO
    # recusaria e a busca giraria até REDIRECIONAMENTO_ESGOTADO.
    passos = [("SamuelF3lipePromo/120532", SHP, 0),
              ("sintetico/vip50_sem_nome", SHP, 120)]
    h = _Linhas()
    esgotado = []

    class _Esg(logging.Handler):
        def emit(self, rec):
            if "REDIRECIONAMENTO_ESGOTADO" in rec.getMessage():
                esgotado.append(rec.getMessage())
    he = _Esg()
    log_out.addHandler(he)
    try:
        f = _cena(passos, "ATIVO")["foto"]
    finally:
        log_out.removeHandler(he)
    r.check(f["novos"] == 1 and f["posts"] == [0, 0], "31.um_post",
            f"novos={f['novos']} posts={f['posts']}")
    r.check(not esgotado, "31.sem_redirecionamento_esgotado", str(esgotado))


def test_32_edicao_nunca_entra_na_regra(r):
    # Caso 1 com a 2ª mensagem chegando como EDIÇÃO de origem não
    # vinculada: a regra só olha mensagem nova — fica o comportamento de
    # hoje (aqui, dois posts) e nenhuma linha nova, nos dois modos.
    from test_cupom_sem_codigo import enriquecer, montar, publicacao
    passos = [("promotom/111295", ML), ("fadadoscupons/17799", ML)]

    def roda(modo, remendos=()):
        h = _Linhas()
        log_out.addHandler(h)
        out = {}
        try:
            with _Modo(modo, remendos):
                async def corpo(c):
                    ns = [_norm(k, p) for k, p in passos]
                    await publicar(ns[0])
                    _recuar(64)
                    await publicacao.enviar(await montar(ns[1]), ns[1],
                                            enr=enriquecer(ns[1]), is_edit=True)
                    out["foto"] = _foto(c, ns)
                cenario(corpo)
        finally:
            log_out.removeHandler(h)
        return out["foto"], h.linhas
    hoje, _ = roda("SOMBRA", _desligado())
    for modo in ("SOMBRA", "ATIVO"):
        foto, linhas = roda(modo)
        r.check(foto == hoje, f"32.{modo}.igual_a_hoje",
                f"{foto['novos']} × {hoje['novos']}")
        r.check(not linhas, f"32.{modo}.sem_linha", str(linhas))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CUPOM SEM CÓDIGO · lançamento × liberação"))
