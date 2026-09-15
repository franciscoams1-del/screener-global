"""
screener_backend.py
===================
Motor de filtro (batch job). Roda fora do Streamlit, via terminal ou cron.

Pipeline:
    1. Monta o universo (listas curadas + expansao automatica por indices).
    2. Baixa 2 anos de precos em LOTE (yf.download / group_by="ticker").
    3. Filtro tecnico (Minervini) sobre os precos -> corta ~95% do universo.
    4. Fundamentos (ROIC / ROIIC LTM) SOMENTE nos sobreviventes.
    5. Grava winners_data.json para o app.py consumir.

Uso:
    python screener_backend.py                 # universo completo
    python screener_backend.py --quick         # so listas curadas (rapido)
    python screener_backend.py --markets US,DE # subconjunto de mercados
    python screener_backend.py --no-expand     # nao busca constituintes na web

Cron sugerido (diario, 22h BRT / apos fechamento de NY):
    0 22 * * 1-5 cd /caminho/do/projeto && /usr/bin/python3 screener_backend.py >> screener.log 2>&1
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import sys
import time
import urllib.error
import urllib.request
from io import StringIO
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------------
# CONFIGURACAO
# ---------------------------------------------------------------------------

OUTPUT_FILE = "winners_data.json"
CACHE_DIR = "cache"
FUNDAMENTALS_CACHE = os.path.join(CACHE_DIR, "fundamentals")
FUNDAMENTALS_TTL_DAYS = 15          # balanco nao muda todo dia; cache agressivo
FAILURE_TTL_HOURS = 6               # falha NAO e cacheada por 15 dias: pode ser
                                    # bloqueio temporario do Yahoo, nao ausencia de dado

PRICE_PERIOD = "2y"
BATCH_SIZE = 100                    # tickers por chamada de yf.download
REPAIR_PRICES = False               # repair=True tem bugs conhecidos que levantam
                                    # excecao e derrubam o lote inteiro. Manter OFF.
MIN_BATCH_RETRY = 20                # lote que falha e dividido ao meio ate este piso
PAUSE_BETWEEN_BATCHES = 1.5         # segundos (evita 429 / IP ban)
PRICE_THREADS = 8                   # threads internas do yfinance por lote

FUNDAMENTAL_WORKERS = 5             # conservador de proposito: evita bloqueio
FUNDAMENTAL_JITTER = (0.4, 1.2)     # sleep aleatorio por requisicao

# --- Criterios do screener -------------------------------------------------
MIN_ROIC = 0.15                     # ROIC LTM > 15%
REQUIRE_ROIIC_ABOVE_ROIC = True     # ROIIC LTM > ROIC LTM
ROIIC_CAP = 9.99                    # teto p/ nao poluir o JSON com +infinito
TREAT_CAPITAL_RELEASE_AS_PASS = True  # delta IC <= 0 e delta NOPAT > 0 -> passa

# Extras do Trend Template do Minervini (desligue se quiser so o basico)
REQUIRE_MA200_RISING = True         # MM200 subindo nos ultimos ~1 mes
MIN_PCT_ABOVE_52W_LOW = 0.30        # >= 30% acima da minima de 52 semanas
MAX_PCT_BELOW_52W_HIGH = 0.25       # <= 25% abaixo da maxima de 52 semanas

MIN_PRICE_LOCAL = 1.0               # descarta penny stocks (moeda local)
MIN_AVG_TURNOVER_LOCAL = 1_000_000  # volume medio 50d * preco, em moeda local
MIN_HISTORY_BARS = 220              # precisa de historico p/ MM200 confiavel

# Setores em que ROIC/ROIIC classico nao faz sentido (capital regulatorio).
EXCLUDED_SECTORS = {"Financial Services", "Real Estate", "Utilities"}
EXCLUDE_SECTORS_ENABLED = True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("screener")

# Contador de motivos de descarte. Sem isso, um resultado vazio e indistinguivel
# de um bug: o log final mostra exatamente onde o funil estrangulou.
REJECTIONS: Counter = Counter()
DOWNLOAD_ERRORS: Counter = Counter()
_LOCK = threading.Lock()


def reject(reason: str) -> None:
    with _LOCK:
        REJECTIONS[reason] += 1


def reject_report() -> list[tuple[str, int]]:
    return sorted(REJECTIONS.items(), key=lambda kv: kv[1], reverse=True)


# ---------------------------------------------------------------------------
# 1. UNIVERSO DE TICKERS
# ---------------------------------------------------------------------------
# Listas curadas (nucleo confiavel, ~700 papeis). A expansao automatica via
# constituintes de indice (funcao expand_universe) leva isso para alguns
# milhares. Sufixos do Yahoo Finance ja aplicados.

US = [  # NYSE + Nasdaq (large/mega cap, sem sufixo)
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "AVGO", "TSLA", "BRK-B", "LLY",
    "JPM", "V", "XOM", "UNH", "MA", "COST", "HD", "PG", "JNJ", "WMT", "ABBV", "NFLX",
    "CRM", "BAC", "ORCL", "MRK", "KO", "CVX", "AMD", "PEP", "TMO", "LIN", "ADBE",
    "CSCO", "ACN", "MCD", "ABT", "WFC", "PM", "IBM", "GE", "TXN", "QCOM", "NOW",
    "DHR", "CAT", "INTU", "VZ", "AMGN", "ISRG", "NEE", "DIS", "SPGI", "RTX", "AMAT",
    "PFE", "UBER", "GS", "UNP", "CMCSA", "LOW", "PGR", "T", "HON", "ELV", "BLK",
    "SYK", "BKNG", "TJX", "COP", "VRTX", "MS", "LRCX", "REGN", "C", "ADP", "MDT",
    "PANW", "BSX", "MU", "SCHW", "CB", "ADI", "KLAC", "MMC", "PLD", "FI", "SBUX",
    "CI", "MDLZ", "SO", "DE", "GILD", "ETN", "BMY", "INTC", "ANET", "AMT", "ICE",
    "SHW", "DUK", "ZTS", "WM", "CME", "EQIX", "MO", "TGT", "PYPL", "CDNS", "SNPS",
    "APH", "CL", "PH", "CSX", "ITW", "MCK", "MSI", "CRWD", "ORLY", "PNC", "USB",
    "NOC", "MAR", "GD", "TT", "EMR", "FDX", "ROP", "AJG", "ECL", "NSC", "AZO",
    "MCO", "APD", "SLB", "TDG", "PCAR", "CARR", "HLT", "MELI", "ABNB", "DASH",
    "SNOW", "DDOG", "ZS", "NET", "MDB", "TEAM", "WDAY", "HUBS", "TTD", "APP",
    "SMCI", "ARM", "PLTR", "COIN", "SQ", "SHOP", "SPOT", "CMG", "LULU", "DECK",
    "NKE", "ROST", "DG", "DLTR", "KR", "SYY", "MNST", "KDP", "STZ", "HSY", "GIS",
    "K", "CAG", "CPB", "MKC", "CHD", "CLX", "KMB", "EL", "ULTA", "YUM", "DPZ",
    "WING", "TXRH", "DRI", "EXPE", "RCL", "CCL", "NCLH", "LVS", "WYNN", "MGM",
    "F", "GM", "RIVN", "LCID", "APTV", "BWA", "LEA", "CMI", "PWR", "URI", "FAST",
    "GWW", "WSO", "POOL", "SITE", "BLDR", "MAS", "MHK", "NVR", "DHI", "LEN", "PHM",
    "TOL", "VMC", "MLM", "NUE", "STLD", "CLF", "X", "FCX", "NEM", "ALB", "DOW",
    "DD", "PPG", "SHER", "IFF", "CE", "EMN", "LYB", "MOS", "CF", "FMC", "AXP",
    "COF", "DFS", "SYF", "ALLY", "TROW", "BEN", "IVZ", "STT", "BK", "NTRS", "RJF",
    "MKTX", "NDAQ", "CBOE", "MSCI", "FDS", "VRSK", "EFX", "TRU", "PAYX", "ADSK",
    "ANSS", "PTC", "TYL", "AKAM", "FFIV", "JNPR", "CIEN", "ON", "MCHP", "SWKS",
    "QRVO", "TER", "ENTG", "MKSI", "AMKR", "ASML", "TSM", "SONY", "NVO", "AZN",
    "SAP", "SHEL", "BP", "TTE", "RIO", "BHP", "HSBC", "UL", "DEO", "BTI", "GSK",
]

CA = [  # Toronto Stock Exchange
    "RY.TO", "TD.TO", "BNS.TO", "BMO.TO", "CM.TO", "NA.TO", "ENB.TO", "TRP.TO",
    "CNQ.TO", "SU.TO", "IMO.TO", "CVE.TO", "TOU.TO", "ARX.TO", "PPL.TO", "CP.TO",
    "CNR.TO", "WCN.TO", "ATD.TO", "L.TO", "MRU.TO", "WN.TO", "DOL.TO", "QSR.TO",
    "SHOP.TO", "CSU.TO", "OTEX.TO", "GIB-A.TO", "BCE.TO", "T.TO", "RCI-B.TO",
    "ABX.TO", "AEM.TO", "WPM.TO", "FNV.TO", "K.TO", "NTR.TO", "MG.TO", "LNR.TO",
    "WSP.TO", "STN.TO", "TIH.TO", "FTT.TO", "POW.TO", "MFC.TO", "SLF.TO", "GWO.TO",
    "IFC.TO", "FFH.TO", "BN.TO", "BAM.TO", "FTS.TO", "EMA.TO", "CU.TO", "AQN.TO",
    "CCO.TO", "TFII.TO", "EMP-A.TO", "SAP.TO", "X.TO", "GFL.TO", "CIGI.TO",
    "DOO.TO", "ATZ.TO", "LUN.TO", "FM.TO", "TECK-B.TO", "IVN.TO", "CJT.TO",
]

GB = [  # London Stock Exchange
    "SHEL.L", "BP.L", "AZN.L", "GSK.L", "HSBA.L", "BARC.L", "LLOY.L", "NWG.L",
    "STAN.L", "ULVR.L", "DGE.L", "RIO.L", "GLEN.L", "AAL.L", "ANTO.L", "BATS.L",
    "IMB.L", "REL.L", "LSEG.L", "PRU.L", "LGEN.L", "AV.L", "III.L", "SGE.L",
    "EXPN.L", "CPG.L", "WTB.L", "NXT.L", "TSCO.L", "SBRY.L", "MKS.L", "JD.L",
    "BDEV.L", "PSN.L", "TW.L", "BKG.L", "SMIN.L", "HLMA.L", "SPX.L", "RR.L",
    "BA.L", "ITRK.L", "CRDA.L", "MNDI.L", "BNZL.L", "DCC.L", "SVT.L", "UU.L",
    "SSE.L", "NG.L", "CNA.L", "VOD.L", "BT-A.L", "IHG.L", "ADM.L", "BEZ.L",
    "HSX.L", "PHNX.L", "SDR.L", "SN.L", "AHT.L", "RTO.L", "WEIR.L", "IMI.L",
    "RMV.L", "AUTO.L", "CCH.L", "ABF.L", "BRBY.L", "FRES.L", "ENT.L", "BAB.L",
]

DE = [  # Frankfurt / XETRA
    "SAP.DE", "SIE.DE", "ALV.DE", "DTE.DE", "MUV2.DE", "BAS.DE", "BAYN.DE",
    "BMW.DE", "MBG.DE", "VOW3.DE", "PAH3.DE", "P911.DE", "RWE.DE", "EOAN.DE",
    "IFX.DE", "ADS.DE", "PUM.DE", "HEN3.DE", "BEI.DE", "MRK.DE", "FRE.DE",
    "FME.DE", "DBK.DE", "CBK.DE", "DB1.DE", "HNR1.DE", "HEI.DE", "AIR.DE",
    "MTX.DE", "RHM.DE", "ZAL.DE", "SY1.DE", "SRT3.DE", "QIA.DE", "1COV.DE",
    "CON.DE", "DHL.DE", "LHA.DE", "TKA.DE", "SHL.DE", "ENR.DE", "VNA.DE",
    "BNR.DE", "NEM.DE", "EVK.DE", "LXS.DE", "DUE.DE", "GXI.DE", "KGX.DE",
    "WCH.DE", "JUN3.DE", "AFX.DE", "COK.DE", "G1A.DE", "NDA.DE", "HFG.DE",
    "DTG.DE", "TEG.DE", "LEG.DE", "HOT.DE", "SZG.DE", "SDF.DE", "FIE.DE",
]

CH = [  # SIX Swiss Exchange (Zurique)
    "NESN.SW", "ROG.SW", "NOVN.SW", "UBSG.SW", "ZURN.SW", "ABBN.SW", "CFR.SW",
    "SIKA.SW", "LONN.SW", "GIVN.SW", "HOLN.SW", "SLHN.SW", "SREN.SW", "GEBN.SW",
    "SCMN.SW", "PGHN.SW", "SOON.SW", "STMN.SW", "BAER.SW", "ALC.SW", "LOGN.SW",
    "TEMN.SW", "SGSN.SW", "KNIN.SW", "BUCN.SW", "VACN.SW", "SCHP.SW", "EMSN.SW",
    "GALD.SW", "DKSH.SW", "BALN.SW", "HELN.SW", "ADEN.SW", "UHR.SW", "BARN.SW",
    "CLN.SW", "KOMN.SW", "BCVN.SW", "ARYN.SW", "SUN.SW", "FHZN.SW", "BELL.SW",
]

FR = [  # Euronext Paris
    "MC.PA", "OR.PA", "RMS.PA", "TTE.PA", "SAN.PA", "AIR.PA", "SU.PA", "AI.PA",
    "BNP.PA", "GLE.PA", "ACA.PA", "CS.PA", "KER.PA", "DG.PA", "SGO.PA", "VIE.PA",
    "ENGI.PA", "CAP.PA", "STLAP.PA", "RNO.PA", "ML.PA", "ORA.PA", "PUB.PA",
    "EL.PA", "LR.PA", "SW.PA", "RI.PA", "BN.PA", "CA.PA", "ERF.PA", "EN.PA",
    "VIV.PA", "TEP.PA", "ALO.PA", "EDEN.PA", "AMUN.PA", "FR.PA", "AKE.PA",
    "SK.PA", "GTT.PA", "IPN.PA", "NEX.PA", "UBI.PA", "ATO.PA", "SESG.PA",
    "BVI.PA", "RXL.PA", "SAF.PA", "HO.PA", "DSY.PA", "LI.PA", "RUI.PA", "CO.PA",
    "OVH.PA", "VRLA.PA", "TRI.PA", "ALD.PA", "ELIS.PA", "SOI.PA", "VLA.PA",
]

IT = [  # Borsa Italiana (Milao)
    "ISP.MI", "UCG.MI", "ENEL.MI", "ENI.MI", "STLAM.MI", "RACE.MI", "G.MI",
    "STM.MI", "TIT.MI", "PST.MI", "SRG.MI", "TRN.MI", "MB.MI", "BMPS.MI",
    "BPE.MI", "BAMI.MI", "FBK.MI", "MONC.MI", "CPR.MI", "PRY.MI", "IG.MI",
    "LDO.MI", "A2A.MI", "HER.MI", "IREN.MI", "DIA.MI", "REC.MI", "AMP.MI",
    "ERG.MI", "TEN.MI", "SPM.MI", "MARR.MI", "BZU.MI", "WBD.MI", "ENAV.MI",
    "IP.MI", "INW.MI", "NEXI.MI", "BC.MI", "SFL.MI", "TOD.MI", "IF.MI",
]

JP = [  # Tokyo Stock Exchange
    "7203.T", "6758.T", "6861.T", "8306.T", "9432.T", "9433.T", "9434.T",
    "6098.T", "4063.T", "6501.T", "6503.T", "6702.T", "6752.T", "6954.T",
    "6971.T", "6981.T", "7267.T", "7269.T", "7201.T", "7011.T", "7012.T",
    "7013.T", "8035.T", "8001.T", "8031.T", "8053.T", "8058.T", "2768.T",
    "8766.T", "8725.T", "8750.T", "8316.T", "8411.T", "8591.T", "8604.T",
    "4502.T", "4503.T", "4507.T", "4523.T", "4519.T", "4568.T", "4578.T",
    "4661.T", "9983.T", "3382.T", "9843.T", "7974.T", "7832.T", "6367.T",
    "6273.T", "6506.T", "6645.T", "4901.T", "4911.T", "3407.T", "4005.T",
    "4188.T", "4452.T", "5401.T", "5406.T", "5713.T", "5802.T", "5108.T",
    "7741.T", "7751.T", "7733.T", "4543.T", "6301.T", "6326.T", "6305.T",
    "6857.T", "6920.T", "8015.T", "9020.T", "9021.T", "9022.T", "9101.T",
    "9104.T", "9107.T", "9202.T", "9531.T", "9532.T", "9503.T", "2914.T",
    "2502.T", "2503.T", "2801.T", "2802.T", "2871.T", "3092.T", "4307.T",
    "4324.T", "4385.T", "4751.T", "6178.T", "9613.T", "9684.T", "9766.T",
]

KR = [  # KOSPI (.KS) e KOSDAQ (.KQ)
    "005930.KS", "000660.KS", "373220.KS", "207940.KS", "005380.KS", "000270.KS",
    "005490.KS", "051910.KS", "006400.KS", "035420.KS", "035720.KS", "105560.KS",
    "055550.KS", "086790.KS", "316140.KS", "012330.KS", "028260.KS", "068270.KS",
    "003550.KS", "034730.KS", "015760.KS", "033780.KS", "017670.KS", "030200.KS",
    "010130.KS", "011200.KS", "009150.KS", "018260.KS", "010950.KS", "096770.KS",
    "011170.KS", "000810.KS", "066570.KS", "051900.KS", "090430.KS", "271560.KS",
    "161390.KS", "004020.KS", "009540.KS", "010140.KS", "042660.KS", "047810.KS",
    "064350.KS", "128940.KS", "302440.KS", "326030.KS", "247540.KQ", "086520.KQ",
    "091990.KQ", "196170.KQ", "058470.KQ", "067310.KQ", "240810.KQ",
]

HK = [  # Hong Kong Stock Exchange
    "0700.HK", "9988.HK", "3690.HK", "1810.HK", "0941.HK", "0762.HK", "0728.HK",
    "1398.HK", "3988.HK", "0939.HK", "1288.HK", "3328.HK", "2318.HK", "2628.HK",
    "1299.HK", "0388.HK", "0005.HK", "0011.HK", "2388.HK", "0016.HK", "0001.HK",
    "0002.HK", "0003.HK", "0006.HK", "0012.HK", "0017.HK", "0027.HK", "0066.HK",
    "0175.HK", "2333.HK", "1211.HK", "0291.HK", "0322.HK", "0288.HK", "2020.HK",
    "2331.HK", "1044.HK", "0669.HK", "1093.HK", "1177.HK", "2269.HK", "6160.HK",
    "1928.HK", "0883.HK", "0386.HK", "0857.HK", "1088.HK", "2899.HK", "0968.HK",
    "0981.HK", "0788.HK", "9618.HK", "9868.HK", "2015.HK", "9999.HK", "9626.HK",
    "1024.HK", "6690.HK", "1919.HK", "2382.HK", "0992.HK", "0836.HK", "0688.HK",
    "1109.HK", "0960.HK", "1997.HK", "0823.HK", "0806.HK",
]

CURATED: dict[str, list[str]] = {
    "US": US, "CA": CA, "GB": GB, "DE": DE, "CH": CH,
    "FR": FR, "IT": IT, "JP": JP, "KR": KR, "HK": HK,
}

# --- Expansao automatica ---------------------------------------------------
# Fontes publicas de constituintes de indice. A ideia e sair de ~700 papeis
# curados para alguns milhares sem hardcodar (e sem inventar) tickers.
# Cada entrada: (url, coluna_do_ticker, sufixo_yahoo)
INDEX_SOURCES: list[tuple[str, str, str, str]] = [
    ("US", "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", "Symbol", ""),
    ("US", "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies", "Symbol", ""),
    ("US", "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies", "Symbol", ""),
    ("US", "https://en.wikipedia.org/wiki/Nasdaq-100", "Ticker", ""),
    ("CA", "https://en.wikipedia.org/wiki/S%26P/TSX_Composite_Index", "Symbol", ".TO"),
    ("GB", "https://en.wikipedia.org/wiki/FTSE_100_Index", "Ticker", ".L"),
    ("GB", "https://en.wikipedia.org/wiki/FTSE_250_Index", "Ticker", ".L"),
    ("DE", "https://en.wikipedia.org/wiki/DAX", "Ticker", ".DE"),
    ("DE", "https://en.wikipedia.org/wiki/MDAX", "Symbol", ".DE"),
    ("FR", "https://en.wikipedia.org/wiki/CAC_40", "Ticker", ".PA"),
    ("IT", "https://en.wikipedia.org/wiki/FTSE_MIB", "Ticker", ".MI"),
    ("CH", "https://en.wikipedia.org/wiki/Swiss_Market_Index", "Ticker", ".SW"),
    ("JP", "https://en.wikipedia.org/wiki/Nikkei_225", "Ticker", ".T"),
    ("HK", "https://en.wikipedia.org/wiki/Hang_Seng_Index", "Ticker", ".HK"),
]

# A Wikipedia recusa requisicoes que nao se identificam como navegador
# (devolve 403). Sem este cabecalho, a expansao falha em silencio e o
# universo encolhe para as listas curadas sem ninguem perceber.
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# Quantos tickers cada fonte rendeu nesta execucao (vai para o JSON final).
UNIVERSE_SOURCES: dict[str, int] = {}

TICKER_COLUMN_CANDIDATES = ["Symbol", "Ticker", "Ticker symbol", "Code", "Epic"]


def _normalize(sym: Any, suffix: str) -> str | None:
    """Converte simbolo bruto de tabela web para o formato do Yahoo."""
    if not isinstance(sym, str):
        return None
    s = sym.strip().upper().split(" ")[0]
    s = s.replace("\xa0", "").replace("$", "")
    if not s or len(s) > 12:
        return None
    if suffix == "":                      # EUA: BRK.B -> BRK-B
        s = s.replace(".", "-")
        return s if s.isalnum() or "-" in s else None
    if suffix == ".T":                    # Japao: so codigo numerico
        s = "".join(ch for ch in s if ch.isdigit())
        if len(s) != 4:
            return None
    if suffix == ".HK":                   # HK: zero-padding para 4 digitos
        s = "".join(ch for ch in s if ch.isdigit())
        if not s:
            return None
        s = s.zfill(4)
    if s.endswith(suffix):
        return s
    return f"{s}{suffix}"


def _fetch_html(url: str) -> str | None:
    """Baixa a pagina fingindo ser um navegador. None se nao conseguir."""
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": BROWSER_UA,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        log.warning("  !! %s devolveu HTTP %s", url.split("/")[-1][:45], exc.code)
    except Exception as exc:
        log.warning("  !! %s falhou: %s", url.split("/")[-1][:45], type(exc).__name__)
    return None


def fetch_index_members(url: str, column: str, suffix: str) -> list[str]:
    """Le a tabela de constituintes de um indice. Falha e sempre reportada."""
    html = _fetch_html(url)
    if html is None:
        return []

    try:
        tables = pd.read_html(StringIO(html), flavor="lxml")
    except Exception as exc:
        log.warning("  !! %s: tabela ilegivel (%s)",
                    url.split("/")[-1][:45], type(exc).__name__)
        return []

    candidates = [column] + [c for c in TICKER_COLUMN_CANDIDATES if c != column]
    out: list[str] = []
    for table in tables:
        cols = {str(c).strip(): c for c in table.columns}
        hit = next((cols[c] for c in candidates if c in cols), None)
        if hit is None:
            continue
        for raw in table[hit].tolist():
            norm = _normalize(raw, suffix)
            if norm:
                out.append(norm)
        if out:
            break

    if not out:
        log.warning("  !! %s: nenhuma coluna de ticker reconhecida",
                    url.split("/")[-1][:45])
    return out


def build_universe(markets: list[str], expand: bool) -> list[str]:
    tickers: list[str] = []
    for m in markets:
        tickers.extend(CURATED.get(m, []))
    log.info("Listas curadas: %d tickers", len(tickers))

    if expand:
        log.info("Expandindo universo pelos constituintes de indice...")
        total_expandido = 0
        for market, url, col, suffix in INDEX_SOURCES:
            if market not in markets:
                continue
            nome = url.split("/")[-1][:45]
            members = fetch_index_members(url, col, suffix)
            UNIVERSE_SOURCES[f"{market}:{nome}"] = len(members)
            total_expandido += len(members)
            log.info("  + %-4s %-46s %5d", market, nome, len(members))
            tickers.extend(members)
            time.sleep(0.5)

        if total_expandido == 0:
            log.error("ATENCAO: a expansao nao trouxe NENHUM ticker.")
            log.error("O universo ficou limitado as listas curadas.")
        else:
            log.info("Expansao trouxe %d tickers (antes de remover repetidos).",
                     total_expandido)
    else:
        log.info("Modo rapido: expansao de indices DESLIGADA.")

    # Arquivo opcional do usuario: um ticker por linha, ja com sufixo.
    if os.path.exists("custom_tickers.txt"):
        with open("custom_tickers.txt", encoding="utf-8") as fh:
            extra = [ln.strip().upper() for ln in fh if ln.strip() and not ln.startswith("#")]
        log.info("  + custom_tickers.txt: %d", len(extra))
        tickers.extend(extra)

    unique = sorted(set(t for t in tickers if t))
    log.info("Universo final: %d tickers unicos", len(unique))
    return unique


# ---------------------------------------------------------------------------
# 2. DOWNLOAD DE PRECOS EM LOTE
# ---------------------------------------------------------------------------

def chunk(seq: list[str], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _extract_ticker(raw: pd.DataFrame, tkr: str) -> pd.DataFrame | None:
    """
    Isola as colunas de um ticker. O yfinance ja mudou a ordem dos niveis
    entre versoes, entao tentamos os dois em vez de assumir um formato.
    """
    if raw is None or raw.empty:
        return None
    if not isinstance(raw.columns, pd.MultiIndex):
        return raw.copy()
    try:
        if tkr in set(raw.columns.get_level_values(0)):
            return raw.xs(tkr, axis=1, level=0).copy()
        if tkr in set(raw.columns.get_level_values(1)):
            return raw.xs(tkr, axis=1, level=1).copy()
    except Exception:
        return None
    return None


def _download_call(batch: list[str]) -> pd.DataFrame | None:
    return yf.download(
        tickers=batch,
        period=PRICE_PERIOD,
        interval="1d",
        group_by="ticker",
        auto_adjust=True,
        threads=PRICE_THREADS,
        progress=False,
        repair=REPAIR_PRICES,
    )


def _collect(raw: pd.DataFrame | None, batch: list[str],
             frames: dict[str, pd.DataFrame]) -> int:
    """Extrai cada ticker do bloco. Um ticker quebrado nao derruba os demais."""
    got = 0
    for tkr in batch:
        try:
            df = _extract_ticker(raw, tkr)
            if df is None:
                continue
            if "Close" not in df.columns:
                continue
            df = df.dropna(subset=["Close"])
            if df.empty or len(df) < MIN_HISTORY_BARS:
                continue
            frames[tkr] = df
            got += 1
        except Exception:
            continue
    return got


def _download_recursive(batch: list[str], frames: dict[str, pd.DataFrame],
                        depth: int = 0) -> None:
    """
    Se a chamada estourar, divide o lote ao meio e tenta de novo, em vez de
    descartar todos os tickers por causa de um unico problematico.
    """
    if not batch:
        return
    try:
        raw = _download_call(batch)
    except Exception as exc:
        msg = f"{type(exc).__name__}: {str(exc)[:120]}"
        DOWNLOAD_ERRORS[msg] += 1
        log.warning("Lote de %d falhou (%s)", len(batch), msg)
        if len(batch) <= MIN_BATCH_RETRY or depth >= 4:
            DOWNLOAD_ERRORS[f"desistiu de {len(batch)} tickers"] += 1
            return
        meio = len(batch) // 2
        time.sleep(PAUSE_BETWEEN_BATCHES)
        _download_recursive(batch[:meio], frames, depth + 1)
        time.sleep(PAUSE_BETWEEN_BATCHES)
        _download_recursive(batch[meio:], frames, depth + 1)
        return

    if raw is None or raw.empty:
        DOWNLOAD_ERRORS["resposta vazia do Yahoo"] += 1
        log.warning("Lote de %d voltou vazio", len(batch))
        return

    got = _collect(raw, batch, frames)
    if got == 0:
        DOWNLOAD_ERRORS["lote sem nenhum ticker aproveitavel"] += 1


def download_prices(tickers: list[str]) -> dict[str, pd.DataFrame]:
    """Baixa historico diario em lotes. Retorna {ticker: DataFrame limpo}."""
    frames: dict[str, pd.DataFrame] = {}
    batches = list(chunk(tickers, BATCH_SIZE))

    for i, batch in enumerate(batches, 1):
        antes = len(frames)
        _download_recursive(batch, frames)
        log.info("Precos: lote %d/%d — %d de %d tickers OK (total %d)",
                 i, len(batches), len(frames) - antes, len(batch), len(frames))
        time.sleep(PAUSE_BETWEEN_BATCHES)

    log.info("Precos validos: %d de %d", len(frames), len(tickers))
    if DOWNLOAD_ERRORS:
        log.warning("---- Problemas no download ----")
        for msg, count in DOWNLOAD_ERRORS.most_common(8):
            log.warning("  %-60s %4d", msg[:60], count)
    return frames


# ---------------------------------------------------------------------------
# 3. FILTRO TECNICO (MINERVINI)
# ---------------------------------------------------------------------------

def technical_snapshot(ticker: str, df: pd.DataFrame) -> dict | None:
    """Calcula medias e aplica o filtro de tendencia. None = reprovado."""
    try:
        close = df["Close"].dropna()
        volume = df["Volume"].fillna(0) if "Volume" in df else pd.Series(0, index=close.index)

        if len(close) < MIN_HISTORY_BARS:
            reject("tecnico: historico insuficiente (<220 pregoes)")
            return None

        price = float(close.iloc[-1])
        prev = float(close.iloc[-2])
        if not np.isfinite(price) or price < MIN_PRICE_LOCAL:
            reject("tecnico: preco invalido ou abaixo do minimo")
            return None

        ma50 = float(close.rolling(50).mean().iloc[-1])
        ma150 = float(close.rolling(150).mean().iloc[-1])
        ma200_series = close.rolling(200).mean()
        ma200 = float(ma200_series.iloc[-1])
        if not all(np.isfinite(v) for v in (ma50, ma150, ma200)):
            reject("tecnico: medias moveis com dado quebrado")
            return None

        # --- criterio obrigatorio: empilhamento das medias ---
        if not (price > ma50 > ma150 > ma200):
            reject("tecnico: nao respeita Preco > MM50 > MM150 > MM200")
            return None

        if REQUIRE_MA200_RISING:
            ref = float(ma200_series.iloc[-22])
            if not np.isfinite(ref) or ma200 <= ref:
                reject("tecnico: MM200 nao esta subindo")
                return None

        window = close.iloc[-252:] if len(close) >= 252 else close
        high52, low52 = float(window.max()), float(window.min())
        if low52 <= 0:
            reject("tecnico: minima de 52 semanas invalida")
            return None
        if (price / low52 - 1.0) < MIN_PCT_ABOVE_52W_LOW:
            reject("tecnico: menos de 30% acima da minima de 52s")
            return None
        if (1.0 - price / high52) > MAX_PCT_BELOW_52W_HIGH:
            reject("tecnico: mais de 25% abaixo da maxima de 52s")
            return None

        turnover = float((volume.iloc[-50:].mean() or 0) * price)
        if turnover < MIN_AVG_TURNOVER_LOCAL:
            reject("tecnico: liquidez abaixo do minimo")
            return None

        return {
            "ticker": ticker,
            "price": round(price, 4),
            "prev_close": round(prev, 4),
            "change_pct": round((price / prev - 1.0) * 100, 2) if prev else 0.0,
            "ma50": round(ma50, 4),
            "ma150": round(ma150, 4),
            "ma200": round(ma200, 4),
            "dist_ma50_pct": round((price / ma50 - 1.0) * 100, 2),
            "dist_ma150_pct": round((price / ma150 - 1.0) * 100, 2),
            "dist_ma200_pct": round((price / ma200 - 1.0) * 100, 2),
            "high_52w": round(high52, 4),
            "low_52w": round(low52, 4),
            "pct_from_high": round((price / high52 - 1.0) * 100, 2),
            "pct_from_low": round((price / low52 - 1.0) * 100, 2),
            "avg_turnover_50d": round(turnover, 0),
            "rs_6m_pct": round((price / float(close.iloc[-126]) - 1.0) * 100, 2)
            if len(close) >= 126 and float(close.iloc[-126]) > 0 else None,
        }
    except Exception as exc:
        reject(f"tecnico: excecao ({type(exc).__name__})")
        return None


# ---------------------------------------------------------------------------
# 4. FUNDAMENTOS: ROIC / ROIIC LTM
# ---------------------------------------------------------------------------

def _row(df: pd.DataFrame | None, names: list[str]) -> pd.Series | None:
    """Busca uma linha do demonstrativo por nome (tolerante a variacoes)."""
    if df is None or df.empty:
        return None
    index_map = {str(i).strip().lower(): i for i in df.index}
    for name in names:
        key = name.strip().lower()
        if key in index_map:
            series = df.loc[index_map[key]]
            if isinstance(series, pd.DataFrame):
                series = series.iloc[0]
            return pd.to_numeric(series, errors="coerce")
    return None


def _sum_window(series: pd.Series | None, start: int, count: int) -> float | None:
    """Soma `count` colunas a partir de `start` (colunas em ordem decrescente)."""
    if series is None:
        return None
    vals = series.iloc[start:start + count].dropna()
    if len(vals) < count:
        return None
    total = float(vals.sum())
    return total if np.isfinite(total) else None


def _at(series: pd.Series | None, pos: int) -> float | None:
    if series is None or len(series) <= pos:
        return None
    val = series.iloc[pos]
    if pd.isna(val):
        return None
    val = float(val)
    return val if np.isfinite(val) else None


def _invested_capital(bs: pd.DataFrame | None, pos: int) -> float | None:
    """IC = linha pronta do Yahoo, senao Divida Total + PL - Caixa."""
    ic = _at(_row(bs, ["Invested Capital"]), pos)
    if ic and ic > 0:
        return ic
    debt = _at(_row(bs, ["Total Debt"]), pos) or 0.0
    equity = _at(_row(bs, ["Stockholders Equity", "Total Equity Gross Minority Interest",
                           "Common Stock Equity"]), pos)
    cash = _at(_row(bs, ["Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments"]), pos) or 0.0
    if equity is None:
        return None
    ic = debt + equity - cash
    return ic if ic > 0 else None


def _cache_path(ticker: str) -> str:
    safe = ticker.replace("/", "_").replace("\\", "_")
    return os.path.join(FUNDAMENTALS_CACHE, f"{safe}.json")


def _read_cache(ticker: str) -> dict | None:
    """
    Sucesso vale FUNDAMENTALS_TTL_DAYS. Falha vale apenas FAILURE_TTL_HOURS,
    porque uma falha costuma ser bloqueio temporario do Yahoo, nao ausencia
    real de balanco. Cachear falha por semanas congela o screener em zero.
    """
    path = _cache_path(ticker)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception:
        return None

    age_hours = (time.time() - os.path.getmtime(path)) / 3600
    if not payload:                                   # {} = falha registrada
        return None if age_hours > FAILURE_TTL_HOURS else {}
    return None if age_hours > FUNDAMENTALS_TTL_DAYS * 24 else payload


def _write_cache(ticker: str, payload: dict) -> None:
    try:
        os.makedirs(FUNDAMENTALS_CACHE, exist_ok=True)
        with open(_cache_path(ticker), "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
    except Exception:
        pass


def _fail(ticker: str, reason: str) -> None:
    """Registra o motivo, memoriza a falha por pouco tempo e descarta."""
    reject(reason)
    _write_cache(ticker, {})
    return None


def fetch_fundamentals(ticker: str, use_cache: bool = True) -> dict | None:
    """
    Retorna metricas LTM. Qualquer dado quebrado/ausente -> None (descarta).

    ROIC  = NOPAT LTM / Capital Investido medio
    ROIIC = variacao do NOPAT / variacao do Capital Investido
    """
    if use_cache:
        cached = _read_cache(ticker)
        if cached is not None:
            if not cached:
                reject("fundamento: falha recente em cache")
                return None
            return cached

    time.sleep(random.uniform(*FUNDAMENTAL_JITTER))

    try:
        t = yf.Ticker(ticker)
        qis = t.quarterly_income_stmt
        qbs = t.quarterly_balance_sheet
        quarterly_ok = (
            qis is not None and not qis.empty and qis.shape[1] >= 8
            and qbs is not None and not qbs.empty and qbs.shape[1] >= 5
        )

        if quarterly_ok:
            inc, bal, step, span = qis, qbs, 4, 4       # LTM = 4 trimestres
        else:                                            # fallback anual
            inc, bal = t.income_stmt, t.balance_sheet
            step, span = 1, 1
            if inc is None or inc.empty or inc.shape[1] < 2:
                return _fail(ticker, "fundamento: DRE indisponivel (bloqueio ou sem dado)")
            if bal is None or bal.empty or bal.shape[1] < 2:
                return _fail(ticker, "fundamento: balanco indisponivel")

        ebit_s = _row(inc, ["EBIT", "Operating Income", "Total Operating Income As Reported"])
        tax_s = _row(inc, ["Tax Provision", "Income Tax Expense"])
        pretax_s = _row(inc, ["Pretax Income", "Income Before Tax"])
        rev_s = _row(inc, ["Total Revenue", "Operating Revenue"])
        ni_s = _row(inc, ["Net Income", "Net Income Common Stockholders"])
        eps_s = _row(inc, ["Diluted EPS", "Basic EPS"])

        ebit_ltm = _sum_window(ebit_s, 0, span)
        ebit_prior = _sum_window(ebit_s, step, span)
        if ebit_ltm is None:
            return _fail(ticker, "fundamento: sem linha de EBIT / lucro operacional")
        if ebit_ltm <= 0:
            return _fail(ticker, "fundamento: EBIT negativo ou zero")
        if ebit_prior is None:
            return _fail(ticker, "fundamento: sem historico p/ comparar (ROIIC)")

        tax_ltm = _sum_window(tax_s, 0, span)
        pretax_ltm = _sum_window(pretax_s, 0, span)
        tax_rate = 0.25
        if tax_ltm is not None and pretax_ltm and pretax_ltm > 0:
            candidate = tax_ltm / pretax_ltm
            if 0.0 <= candidate <= 0.50:
                tax_rate = candidate

        nopat_ltm = ebit_ltm * (1 - tax_rate)
        nopat_prior = ebit_prior * (1 - tax_rate)

        ic_now = _invested_capital(bal, 0)
        ic_prior = _invested_capital(bal, step)
        if ic_now is None or ic_prior is None:
            return _fail(ticker, "fundamento: capital investido nao calculavel")

        ic_avg = (ic_now + ic_prior) / 2
        if ic_avg <= 0:
            return _fail(ticker, "fundamento: capital investido negativo")

        roic = nopat_ltm / ic_avg
        if not np.isfinite(roic):
            return _fail(ticker, "fundamento: ROIC invalido")

        delta_nopat = nopat_ltm - nopat_prior
        delta_ic = ic_now - ic_prior
        if delta_ic > 0:
            roiic = delta_nopat / delta_ic
        elif TREAT_CAPITAL_RELEASE_AS_PASS and delta_nopat > 0:
            roiic = ROIIC_CAP          # cresceu lucro liberando capital
        else:
            roiic = -ROIIC_CAP
        roiic = float(max(-ROIIC_CAP, min(ROIIC_CAP, roiic)))

        # --- metricas de vitrine (modal) ---
        rev_ltm = _sum_window(rev_s, 0, span)
        ni_ltm = _sum_window(ni_s, 0, span)
        eps_ltm = _sum_window(eps_s, 0, span)
        op_margin = (ebit_ltm / rev_ltm) if rev_ltm and rev_ltm > 0 else None

        fast = t.fast_info
        market_cap = getattr(fast, "market_cap", None)
        currency = getattr(fast, "currency", None)
        last_price = getattr(fast, "last_price", None)

        total_debt = _at(_row(bal, ["Total Debt"]), 0) or 0.0
        cash = _at(_row(bal, ["Cash And Cash Equivalents",
                              "Cash Cash Equivalents And Short Term Investments"]), 0) or 0.0
        net_debt = total_debt - cash
        ev = (market_cap + net_debt) if market_cap else None
        pe = (last_price / eps_ltm) if (last_price and eps_ltm and eps_ltm > 0) else None

        sector, industry, name = None, None, ticker
        try:
            info = t.get_info()
            sector = info.get("sector")
            industry = info.get("industry")
            name = info.get("shortName") or info.get("longName") or ticker
            market_cap = market_cap or info.get("marketCap")
        except Exception:
            pass

        payload = {
            "name": name,
            "sector": sector,
            "industry": industry,
            "currency": currency,
            "roic": round(float(roic), 4),
            "roiic": round(float(roiic), 4),
            "tax_rate": round(float(tax_rate), 4),
            "nopat_ltm": float(nopat_ltm),
            "invested_capital": float(ic_now),
            "ebit_ltm": float(ebit_ltm),
            "revenue_ltm": float(rev_ltm) if rev_ltm else None,
            "net_income_ltm": float(ni_ltm) if ni_ltm else None,
            "eps_ltm": round(float(eps_ltm), 4) if eps_ltm else None,
            "operating_margin": round(float(op_margin), 4) if op_margin else None,
            "pe_ratio": round(float(pe), 2) if pe else None,
            "market_cap": float(market_cap) if market_cap else None,
            "enterprise_value": float(ev) if ev else None,
            "net_debt": float(net_debt),
            "total_debt": float(total_debt),
            "cash": float(cash),
            "basis": "quarterly_ltm" if quarterly_ok else "annual",
        }
        _write_cache(ticker, payload)
        return payload

    except Exception as exc:
        log.debug("Fundamentos falharam para %s: %s", ticker, exc)
        return _fail(ticker, f"fundamento: excecao ({type(exc).__name__})")


def passes_quality(f: dict) -> bool:
    if f.get("roic") is None:
        reject("qualidade: ROIC ausente")
        return False
    if f["roic"] <= MIN_ROIC:
        reject(f"qualidade: ROIC abaixo de {MIN_ROIC:.0%}")
        return False
    if REQUIRE_ROIIC_ABOVE_ROIC and (f.get("roiic") is None or f["roiic"] <= f["roic"]):
        reject("qualidade: ROIIC nao supera o ROIC")
        return False
    if EXCLUDE_SECTORS_ENABLED and f.get("sector") in EXCLUDED_SECTORS:
        reject(f"qualidade: setor excluido ({f.get('sector')})")
        return False
    return True


# ---------------------------------------------------------------------------
# 5. ORQUESTRACAO
# ---------------------------------------------------------------------------

def run(markets: list[str], expand: bool, use_cache: bool) -> dict:
    t0 = time.time()
    os.makedirs(CACHE_DIR, exist_ok=True)

    universe = build_universe(markets, expand)
    if not universe:
        raise SystemExit("Universo vazio. Verifique os mercados selecionados.")

    prices = download_prices(universe)

    # --- etapa barata: tecnico em memoria ---
    survivors: list[dict] = []
    for tkr, df in prices.items():
        snap = technical_snapshot(tkr, df)
        if snap:
            survivors.append(snap)
    log.info("Aprovados no filtro tecnico: %d", len(survivors))

    if not survivors:
        log.warning("Nenhum ticker passou no tecnico. Nada a fazer.")
        winners: list[dict] = []
    else:
        # --- etapa cara: fundamentos so nos sobreviventes ---
        winners = []
        done = 0
        with ThreadPoolExecutor(max_workers=FUNDAMENTAL_WORKERS) as pool:
            futures = {pool.submit(fetch_fundamentals, s["ticker"], use_cache): s
                       for s in survivors}
            for fut in as_completed(futures):
                snap = futures[fut]
                done += 1
                if done % 25 == 0:
                    log.info("Fundamentos: %d/%d", done, len(survivors))
                try:
                    fund = fut.result()
                except Exception:
                    continue
                if not fund or not passes_quality(fund):
                    continue
                winners.append({**snap, **fund})

    # Limpeza final: remove qualquer registro com NaN/inf residual.
    clean: list[dict] = []
    for w in winners:
        bad = any(isinstance(v, float) and not math.isfinite(v) for v in w.values())
        if not bad:
            clean.append(w)
    clean.sort(key=lambda r: r.get("roic", 0), reverse=True)

    log.info("---- Funil de descarte ----")
    for reason, count in reject_report()[:12]:
        log.info("  %-52s %5d", reason, count)
    log.info("---------------------------")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "universe_size": len(universe),
        "universe_sources": dict(UNIVERSE_SOURCES),
        "expansion_enabled": expand,
        "priced_ok": len(prices),
        "technical_pass": len(survivors),
        "winners_count": len(clean),
        "criteria": {
            "technical": "Preco > MM50 > MM150 > MM200",
            "ma200_rising": REQUIRE_MA200_RISING,
            "min_pct_above_52w_low": MIN_PCT_ABOVE_52W_LOW,
            "max_pct_below_52w_high": MAX_PCT_BELOW_52W_HIGH,
            "min_roic": MIN_ROIC,
            "roiic_gt_roic": REQUIRE_ROIIC_ABOVE_ROIC,
            "excluded_sectors": sorted(EXCLUDED_SECTORS) if EXCLUDE_SECTORS_ENABLED else [],
        },
        "elapsed_seconds": round(time.time() - t0, 1),
        "rejections": dict(reject_report()),
        "download_errors": dict(DOWNLOAD_ERRORS.most_common(10)),
        "winners": clean,
    }

    tmp = OUTPUT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, OUTPUT_FILE)        # escrita atomica: app.py nunca le lixo

    log.info("OK -> %s | %d vencedores em %.1fs",
             OUTPUT_FILE, len(clean), payload["elapsed_seconds"])
    return payload


def diagnose(ticker: str) -> None:
    """Inspeciona um unico ticker e imprime onde ele passa ou trava."""
    print(f"\n=== DIAGNOSTICO: {ticker} ===\n")

    prices = download_prices([ticker])
    if ticker not in prices:
        print("PRECOS: nada retornado. Ticker errado, sufixo errado ou bloqueio do Yahoo.")
        return
    df = prices[ticker]
    close = df["Close"].dropna()
    print(f"PRECOS: {len(close)} pregoes, de {close.index[0].date()} a {close.index[-1].date()}")
    price = float(close.iloc[-1])
    ma50 = float(close.rolling(50).mean().iloc[-1])
    ma150 = float(close.rolling(150).mean().iloc[-1])
    ma200 = float(close.rolling(200).mean().iloc[-1])
    print(f"  Preco {price:,.2f} | MM50 {ma50:,.2f} | MM150 {ma150:,.2f} | MM200 {ma200:,.2f}")
    print(f"  Empilhamento correto? {'SIM' if price > ma50 > ma150 > ma200 else 'NAO'}")

    REJECTIONS.clear()
    snap = technical_snapshot(ticker, df)
    if snap is None:
        print(f"  REPROVADO NO TECNICO -> {reject_report()[0][0] if reject_report() else '?'}")
    else:
        print("  APROVADO NO TECNICO")

    print()
    REJECTIONS.clear()
    fund = fetch_fundamentals(ticker, use_cache=False)
    if fund is None:
        print(f"FUNDAMENTOS: falhou -> {reject_report()[0][0] if reject_report() else '?'}")
        return
    print(f"FUNDAMENTOS ({fund['basis']}):")
    print(f"  Empresa       {fund.get('name')}  |  setor: {fund.get('sector')}")
    print(f"  EBIT LTM      {fund.get('ebit_ltm'):,.0f}")
    print(f"  Cap. Investido {fund.get('invested_capital'):,.0f}")
    print(f"  ROIC          {fund['roic']:.2%}   (minimo exigido: {MIN_ROIC:.0%})")
    print(f"  ROIIC         {fund['roiic']:.2%}   (precisa superar o ROIC)")

    REJECTIONS.clear()
    ok = passes_quality(fund)
    print(f"\n  RESULTADO: {'APROVADO' if ok else 'REPROVADO -> ' + reject_report()[0][0]}")


def apply_relaxed_preset() -> None:
    """Afrouxa os cortes para diagnosticar um resultado vazio."""
    global MIN_ROIC, REQUIRE_ROIIC_ABOVE_ROIC, REQUIRE_MA200_RISING
    global MIN_PCT_ABOVE_52W_LOW, MAX_PCT_BELOW_52W_HIGH
    MIN_ROIC = 0.10
    REQUIRE_ROIIC_ABOVE_ROIC = False
    REQUIRE_MA200_RISING = False
    MIN_PCT_ABOVE_52W_LOW = 0.10
    MAX_PCT_BELOW_52W_HIGH = 0.35
    log.info("MODO RELAXADO: ROIC>10%, ROIIC livre, MM200 livre")


def main() -> None:
    ap = argparse.ArgumentParser(description="Screener Minervini + ROIC/ROIIC")
    ap.add_argument("--markets", default=",".join(CURATED.keys()),
                    help="Mercados: US,CA,GB,DE,CH,FR,IT,JP,KR,HK")
    ap.add_argument("--quick", action="store_true", help="So listas curadas")
    ap.add_argument("--no-expand", action="store_true", help="Nao busca indices na web")
    ap.add_argument("--no-cache", action="store_true", help="Ignora cache de fundamentos")
    ap.add_argument("--relax", action="store_true", help="Afrouxa os cortes (diagnostico)")
    ap.add_argument("--diagnose", metavar="TICKER", help="Inspeciona um unico ticker")
    args = ap.parse_args()

    if args.diagnose:
        diagnose(args.diagnose.strip().upper())
        return

    if args.relax:
        apply_relaxed_preset()

    markets = [m.strip().upper() for m in args.markets.split(",") if m.strip()]
    unknown = [m for m in markets if m not in CURATED]
    if unknown:
        sys.exit(f"Mercado desconhecido: {unknown}. Validos: {list(CURATED)}")

    expand = not (args.quick or args.no_expand)
    run(markets, expand=expand, use_cache=not args.no_cache)


if __name__ == "__main__":
    main()
