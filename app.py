"""
app.py
======
Dashboard Streamlit (frontend leve).

Regra de ouro: NENHUMA varredura de universo acontece aqui.
    - Acoes filtradas  -> lidas de winners_data.json (gerado pelo backend).
    - Macro/commodities -> ao vivo, mas sao <20 ativos e ficam em cache.
    - Intraday          -> 1 request, so quando o modal abre.

Rodar:
    streamlit run app.py
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

DATA_FILE = "winners_data.json"
CARDS_PER_ROW = 4
PAGE_SIZE = 24

# @st.dialog existe a partir do Streamlit 1.35; antes era experimental_dialog.
dialog = getattr(st, "dialog", None) or getattr(st, "experimental_dialog")

st.set_page_config(
    page_title="Screener Global | Minervini + ROIC",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# ESTILO
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
      .block-container { padding-top: 2.2rem; padding-bottom: 3rem; }

      .stock-card {
        border-radius: 10px;
        padding: 14px 16px 12px 16px;
        margin-bottom: 6px;
        border: 1px solid rgba(255,255,255,0.10);
        min-height: 188px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        font-variant-numeric: tabular-nums;
      }
      .card-up   { background: #0d3b2a; border-left: 4px solid #2ecc71; }
      .card-down { background: #4a1418; border-left: 4px solid #e74c3c; }

      .card-head { display:flex; justify-content:space-between; align-items:baseline; gap:8px; }
      .card-ticker { font-size: 1.15rem; font-weight: 700; color:#fff; letter-spacing:.3px; }
      .card-chg { font-size: .95rem; font-weight: 700; }
      .chg-up { color:#7ef2b0; } .chg-down { color:#ff9a9a; }

      .card-name { font-size:.72rem; color:rgba(255,255,255,.55);
                   white-space:nowrap; overflow:hidden; text-overflow:ellipsis; margin-top:2px; }
      .card-price { font-size:1.7rem; font-weight:700; color:#fff; margin:10px 0 8px 0; line-height:1; }

      .ma-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:6px; }
      .ma-item { background:rgba(0,0,0,.25); border-radius:6px; padding:5px 4px; text-align:center; }
      .ma-label { display:block; font-size:.62rem; color:rgba(255,255,255,.55); }
      .ma-value { display:block; font-size:.82rem; font-weight:700; color:#eaf6f0; }

      .quality-row { margin-top:8px; font-size:.7rem; color:rgba(255,255,255,.7); }

      .macro-tile {
        background:#14171c; border:1px solid rgba(255,255,255,.08);
        border-radius:8px; padding:10px 12px; text-align:left;
      }
      .macro-label { font-size:.68rem; color:rgba(255,255,255,.55); }
      .macro-value { font-size:1.05rem; font-weight:700; color:#fff; }
      .macro-chg { font-size:.75rem; font-weight:600; }

      div.stButton > button { width:100%; border-radius:8px; font-weight:600; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# DADOS
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_winners(mtime: float) -> dict:
    """mtime entra na assinatura para invalidar o cache quando o arquivo muda."""
    with open(DATA_FILE, encoding="utf-8") as fh:
        return json.load(fh)


def read_dataset() -> dict | None:
    if not os.path.exists(DATA_FILE):
        return None
    return load_winners(os.path.getmtime(DATA_FILE))


MACRO = {
    "S&P 500": "^GSPC",
    "Nasdaq 100": "^NDX",
    "Ibovespa": "^BVSP",
    "VIX": "^VIX",
    "Treasury 10Y": "^TNX",
    "Dólar Index": "DX-Y.NYB",
    "USD/BRL": "USDBRL=X",
    "EUR/USD": "EURUSD=X",
    "Ouro": "GC=F",
    "Prata": "SI=F",
    "Petróleo WTI": "CL=F",
    "Petróleo Brent": "BZ=F",
    "Gás Natural": "NG=F",
    "Cobre": "HG=F",
    "Bitcoin": "BTC-USD",
    "Ethereum": "ETH-USD",
}


@st.cache_data(ttl=300, show_spinner=False)
def load_macro() -> list[dict]:
    """16 ativos, uma unica chamada em lote, cache de 5 minutos."""
    out: list[dict] = []
    try:
        raw = yf.download(
            list(MACRO.values()), period="5d", interval="1d",
            group_by="ticker", auto_adjust=True, threads=True, progress=False,
        )
    except Exception:
        return out

    for label, tkr in MACRO.items():
        try:
            df = raw[tkr] if isinstance(raw.columns, pd.MultiIndex) else raw
            close = df["Close"].dropna()
            if len(close) < 2:
                continue
            last, prev = float(close.iloc[-1]), float(close.iloc[-2])
            out.append({
                "label": label,
                "value": last,
                "change_pct": (last / prev - 1) * 100 if prev else 0.0,
            })
        except Exception:
            continue
    return out


@st.cache_data(ttl=120, show_spinner=False)
def load_intraday(ticker: str) -> pd.DataFrame:
    """Intraday de 5 dias; cai para 6 meses diario se o mercado nao fornecer."""
    try:
        df = yf.Ticker(ticker).history(period="5d", interval="5m", auto_adjust=True)
        if df is not None and not df.empty:
            return df.dropna(subset=["Close"])
    except Exception:
        pass
    try:
        df = yf.Ticker(ticker).history(period="6mo", interval="1d", auto_adjust=True)
        return df.dropna(subset=["Close"]) if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# FORMATADORES
# ---------------------------------------------------------------------------
def fmt_num(v, dec: int = 2) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{v:,.{dec}f}".replace(",", "@").replace(".", ",").replace("@", ".")


def fmt_big(v) -> str:
    if v is None:
        return "—"
    v = float(v)
    sign = "-" if v < 0 else ""
    v = abs(v)
    for unit, div in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if v >= div:
            return f"{sign}{v/div:,.2f}{unit}".replace(",", ".")
    return f"{sign}{v:,.0f}"


def fmt_pct(v, dec: int = 2, already_pct: bool = True) -> str:
    if v is None:
        return "—"
    val = float(v) if already_pct else float(v) * 100
    return f"{val:+.{dec}f}%".replace(".", ",")


def card_html(row: dict) -> str:
    up = (row.get("change_pct") or 0) >= 0
    css = "card-up" if up else "card-down"
    chg_css = "chg-up" if up else "chg-down"
    name = (row.get("name") or row["ticker"])[:34]
    roic = fmt_pct(row.get("roic"), 1, already_pct=False)
    roiic = fmt_pct(row.get("roiic"), 1, already_pct=False)

    def ma_cell(label, value):
        return (f'<div class="ma-item"><span class="ma-label">{label}</span>'
                f'<span class="ma-value">{fmt_pct(value, 1)}</span></div>')

    return f"""
    <div class="stock-card {css}">
      <div>
        <div class="card-head">
          <span class="card-ticker">{row['ticker']}</span>
          <span class="card-chg {chg_css}">{fmt_pct(row.get('change_pct'))}</span>
        </div>
        <div class="card-name">{name}</div>
        <div class="card-price">{fmt_num(row.get('price'))}</div>
      </div>
      <div>
        <div class="ma-grid">
          {ma_cell("vs MM50", row.get("dist_ma50_pct"))}
          {ma_cell("vs MM150", row.get("dist_ma150_pct"))}
          {ma_cell("vs MM200", row.get("dist_ma200_pct"))}
        </div>
        <div class="quality-row">ROIC {roic} &nbsp;·&nbsp; ROIIC {roiic}</div>
      </div>
    </div>
    """


# ---------------------------------------------------------------------------
# MODAL DE ANALISE
# ---------------------------------------------------------------------------
@dialog("Análise do ativo", width="large")
def analysis_dialog(row: dict) -> None:
    st.subheader(f"{row['ticker']} — {row.get('name') or ''}")
    meta = " · ".join(filter(None, [row.get("sector"), row.get("industry"), row.get("currency")]))
    if meta:
        st.caption(meta)

    df = load_intraday(row["ticker"])
    if df.empty:
        st.warning("Sem dados intraday disponíveis para este ativo agora.")
    else:
        fig = go.Figure()
        if {"Open", "High", "Low", "Close"}.issubset(df.columns):
            fig.add_trace(go.Candlestick(
                x=df.index, open=df["Open"], high=df["High"],
                low=df["Low"], close=df["Close"],
                increasing_line_color="#2ecc71", decreasing_line_color="#e74c3c",
                name="Preço",
            ))
        else:
            fig.add_trace(go.Scatter(x=df.index, y=df["Close"], mode="lines", name="Preço"))

        for label, key, color in (("MM50", "ma50", "#f1c40f"),
                                  ("MM150", "ma150", "#3498db"),
                                  ("MM200", "ma200", "#e67e22")):
            if row.get(key):
                fig.add_hline(y=row[key], line_dash="dot", line_color=color,
                              annotation_text=label, annotation_position="right")

        fig.update_layout(
            height=420, margin=dict(l=10, r=10, t=10, b=10),
            xaxis_rangeslider_visible=False, template="plotly_dark",
            showlegend=False, paper_bgcolor="rgba(0,0,0,0)",
        )
        # Pula buracos de pregao (noites e fins de semana) no eixo X.
        fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("##### Fundamentos (LTM)")
    metrics = [
        ("Enterprise Value", fmt_big(row.get("enterprise_value"))),
        ("Dívida Líquida", fmt_big(row.get("net_debt"))),
        ("Market Cap", fmt_big(row.get("market_cap"))),
        ("EPS", fmt_num(row.get("eps_ltm"))),
        ("P/E", fmt_num(row.get("pe_ratio"))),
        ("Margem Operacional", fmt_pct(row.get("operating_margin"), 1, already_pct=False)),
        ("ROIC", fmt_pct(row.get("roic"), 1, already_pct=False)),
        ("ROIIC", fmt_pct(row.get("roiic"), 1, already_pct=False)),
        ("Receita LTM", fmt_big(row.get("revenue_ltm"))),
        ("EBIT LTM", fmt_big(row.get("ebit_ltm"))),
        ("Capital Investido", fmt_big(row.get("invested_capital"))),
        ("Alíquota efetiva", fmt_pct(row.get("tax_rate"), 1, already_pct=False)),
    ]
    for i in range(0, len(metrics), 4):
        cols = st.columns(4)
        for col, (label, value) in zip(cols, metrics[i:i + 4]):
            col.metric(label, value)

    st.markdown("##### Posição técnica")
    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Preço", fmt_num(row.get("price")))
    t2.metric("Máx. 52s", fmt_num(row.get("high_52w")), fmt_pct(row.get("pct_from_high")))
    t3.metric("Mín. 52s", fmt_num(row.get("low_52w")), fmt_pct(row.get("pct_from_low")))
    t4.metric("Retorno 6m", fmt_pct(row.get("rs_6m_pct")))
    st.caption(f"Base contábil: {row.get('basis', 'n/d')}")


# ---------------------------------------------------------------------------
# PAGINA
# ---------------------------------------------------------------------------
data = read_dataset()

st.title("Screener Global")

if data is None:
    st.error(
        f"`{DATA_FILE}` não encontrado. Rode o motor antes de abrir o dashboard:\n\n"
        "```bash\npython screener_backend.py --quick\n```"
    )
    st.stop()

winners: list[dict] = data.get("winners", [])
generated = data.get("generated_at", "")
try:
    age = datetime.now(timezone.utc) - datetime.fromisoformat(generated)
    age_txt = f"{int(age.total_seconds() // 3600)}h atrás"
except Exception:
    age_txt = "—"

st.caption(
    f"Universo {data.get('universe_size', 0):,} · aprovados no técnico "
    f"{data.get('technical_pass', 0)} · vencedores {len(winners)} · "
    f"dados de {generated[:16].replace('T', ' ')} ({age_txt})".replace(",", ".")
)

# --- Faixa macro (ao vivo) -------------------------------------------------
with st.expander("Macro e commodities (ao vivo)", expanded=True):
    macro = load_macro()
    if not macro:
        st.info("Não foi possível carregar os dados macro agora.")
    else:
        for i in range(0, len(macro), 8):
            cols = st.columns(8)
            for col, item in zip(cols, macro[i:i + 8]):
                color = "#2ecc71" if item["change_pct"] >= 0 else "#e74c3c"
                col.markdown(
                    f"""<div class="macro-tile">
                          <div class="macro-label">{item['label']}</div>
                          <div class="macro-value">{fmt_num(item['value'])}</div>
                          <div class="macro-chg" style="color:{color}">
                            {fmt_pct(item['change_pct'])}</div>
                        </div>""",
                    unsafe_allow_html=True,
                )

# --- Sidebar: filtros ------------------------------------------------------
with st.sidebar:
    st.header("Filtros")
    if st.button("Recarregar arquivo"):
        st.cache_data.clear()
        st.rerun()

    query = st.text_input("Buscar ticker ou nome", "").strip().upper()

    sectors = sorted({w.get("sector") for w in winners if w.get("sector")})
    chosen_sectors = st.multiselect("Setor", sectors, default=[])

    suffixes = sorted({(w["ticker"].split(".")[-1] if "." in w["ticker"] else "US")
                       for w in winners})
    chosen_markets = st.multiselect("Praça (sufixo Yahoo)", suffixes, default=[])

    min_roic = st.slider("ROIC mínimo (%)", 15, 60, 15, step=1)
    order = st.selectbox(
        "Ordenar por",
        ["ROIC", "ROIIC", "Variação %", "Distância da MM200", "Retorno 6m", "Ticker"],
    )

rows = winners
if query:
    rows = [r for r in rows
            if query in r["ticker"].upper() or query in (r.get("name") or "").upper()]
if chosen_sectors:
    rows = [r for r in rows if r.get("sector") in chosen_sectors]
if chosen_markets:
    rows = [r for r in rows
            if (r["ticker"].split(".")[-1] if "." in r["ticker"] else "US") in chosen_markets]
rows = [r for r in rows if (r.get("roic") or 0) * 100 >= min_roic]

sort_key = {
    "ROIC": lambda r: r.get("roic") or 0,
    "ROIIC": lambda r: r.get("roiic") or 0,
    "Variação %": lambda r: r.get("change_pct") or 0,
    "Distância da MM200": lambda r: r.get("dist_ma200_pct") or 0,
    "Retorno 6m": lambda r: r.get("rs_6m_pct") or 0,
    "Ticker": lambda r: r["ticker"],
}[order]
rows = sorted(rows, key=sort_key, reverse=(order != "Ticker"))

if not rows:
    st.warning("Nenhum ativo atende aos filtros selecionados.")
    st.stop()

# --- Paginacao -------------------------------------------------------------
total_pages = (len(rows) - 1) // PAGE_SIZE + 1
head_l, head_r = st.columns([3, 1])
head_l.subheader(f"{len(rows)} ativos aprovados")
page = head_r.number_input(
    "Página", min_value=1, max_value=total_pages, value=1, step=1,
    label_visibility="collapsed",
) if total_pages > 1 else 1
page_rows = rows[(page - 1) * PAGE_SIZE: page * PAGE_SIZE]

# --- Grade de cards --------------------------------------------------------
for i in range(0, len(page_rows), CARDS_PER_ROW):
    slice_ = page_rows[i:i + CARDS_PER_ROW]
    cols = st.columns(CARDS_PER_ROW)

    for col, row in zip(cols, slice_):
        with col:
            st.markdown(card_html(row), unsafe_allow_html=True)
            if st.button("Analisar", key=f"btn_{row['ticker']}_{i}", use_container_width=True):
                analysis_dialog(row)

    # Colunas sobrando na ultima linha viram placeholders vazios, para os
    # cards nao esticarem horizontalmente.
    for j in range(len(slice_), CARDS_PER_ROW):
        cols[j].empty()

# --- Tabela bruta ----------------------------------------------------------
with st.expander("Ver dados em tabela"):
    show_cols = ["ticker", "name", "sector", "price", "change_pct",
                 "dist_ma50_pct", "dist_ma150_pct", "dist_ma200_pct",
                 "roic", "roiic", "pe_ratio", "operating_margin", "eps_ltm"]
    table = pd.DataFrame(rows)
    table = table[[c for c in show_cols if c in table.columns]]
    st.dataframe(table, use_container_width=True, hide_index=True)
    st.download_button(
        "Baixar CSV",
        table.to_csv(index=False).encode("utf-8"),
        file_name=f"screener_{datetime.now():%Y%m%d}.csv",
        mime="text/csv",
    )
# custom_tickers.txt
# ---------------------------------------------------------------------------
# Tickers extras lidos pelo screener_backend.py alem das listas internas.
# Um por linha, ja com o sufixo do Yahoo Finance. Linhas com # sao ignoradas.
#
# Este arquivo preenche as duas lacunas da expansao automatica:
#   - Japao  (.T ) : a tabela do Nikkei 225 na Wikipedia nao e legivel
#   - Coreia (.KS/.KQ): nao existe fonte de expansao
#
# Ticker invalido nao quebra nada: volta sem dado e e descartado em silencio.
# Para acrescentar os seus, basta escrever no fim do arquivo.
# ---------------------------------------------------------------------------


# --- JAPAO — Bolsa de Toquio (224 papeis) ---
1332.T
1605.T
1721.T
1801.T
1802.T
1803.T
1808.T
1812.T
1925.T
1928.T
1963.T
2002.T
2269.T
2282.T
2413.T
2432.T
2501.T
2531.T
2587.T
3086.T
3099.T
3101.T
3105.T
3401.T
3402.T
3405.T
3861.T
3863.T
4004.T
4021.T
4042.T
4043.T
4061.T
4062.T
4088.T
4151.T
4183.T
4186.T
4204.T
4208.T
4272.T
4403.T
4506.T
4516.T
4521.T
4528.T
4530.T
4536.T
4540.T
4552.T
4587.T
4612.T
4613.T
4631.T
4634.T
4684.T
4689.T
4704.T
4716.T
4755.T
4768.T
4902.T
4967.T
5019.T
5020.T
5101.T
5201.T
5214.T
5232.T
5233.T
5301.T
5332.T
5333.T
5334.T
5411.T
5541.T
5631.T
5711.T
5714.T
5801.T
5803.T
5901.T
6013.T
6103.T
6113.T
6135.T
6141.T
6146.T
6201.T
6268.T
6302.T
6323.T
6324.T
6361.T
6383.T
6395.T
6417.T
6432.T
6448.T
6457.T
6465.T
6471.T
6472.T
6473.T
6479.T
6481.T
6504.T
6508.T
6526.T
6532.T
6586.T
6588.T
6594.T
6674.T
6701.T
6723.T
6724.T
6728.T
6762.T
6770.T
6806.T
6841.T
6845.T
6849.T
6869.T
6902.T
6923.T
6952.T
6963.T
6965.T
6967.T
6976.T
6988.T
7003.T
7004.T
7202.T
7205.T
7211.T
7259.T
7261.T
7270.T
7272.T
7309.T
7453.T
7532.T
7550.T
7581.T
7616.T
7649.T
7701.T
7729.T
7731.T
7735.T
7747.T
7752.T
7762.T
7911.T
7912.T
7936.T
7951.T
8002.T
8113.T
8227.T
8233.T
8267.T
8282.T
8304.T
8308.T
8309.T
8331.T
8355.T
8377.T
8418.T
8425.T
8473.T
8570.T
8595.T
8601.T
8630.T
8697.T
8795.T
8801.T
8802.T
8804.T
8830.T
9001.T
9005.T
9007.T
9008.T
9009.T
9024.T
9041.T
9042.T
9044.T
9045.T
9048.T
9062.T
9064.T
9065.T
9143.T
9147.T
9201.T
9301.T
9364.T
9401.T
9404.T
9435.T
9449.T
9468.T
9501.T
9502.T
9504.T
9505.T
9506.T
9507.T
9508.T
9509.T
9513.T
9533.T
9602.T
9697.T
9706.T
9735.T
9744.T

# --- COREIA — KOSPI (100 papeis) ---
000100.KS
000120.KS
000150.KS
000240.KS
000670.KS
000720.KS
000880.KS
000990.KS
001040.KS
001120.KS
001230.KS
001450.KS
001680.KS
001800.KS
002380.KS
002790.KS
003230.KS
003490.KS
003620.KS
004000.KS
004170.KS
004370.KS
004990.KS
005250.KS
005300.KS
005850.KS
005940.KS
006260.KS
006280.KS
006360.KS
006650.KS
006800.KS
007070.KS
008560.KS
008770.KS
009830.KS
010060.KS
010120.KS
010620.KS
011070.KS
011780.KS
011790.KS
012450.KS
016360.KS
017800.KS
018880.KS
020150.KS
021240.KS
023530.KS
024110.KS
028050.KS
029780.KS
030000.KS
032640.KS
034020.KS
034220.KS
035250.KS
036460.KS
036570.KS
042670.KS
047050.KS
051600.KS
052690.KS
057050.KS
069260.KS
069960.KS
071050.KS
078930.KS
088350.KS
097950.KS
103140.KS
111770.KS
120110.KS
138040.KS
139480.KS
145020.KS
161890.KS
175330.KS
180640.KS
181710.KS
213500.KS
241560.KS
251270.KS
267250.KS
267260.KS
272210.KS
282330.KS
293490.KS
294870.KS
298020.KS
298040.KS
307950.KS
329180.KS
336260.KS
352820.KS
361610.KS
375500.KS
377300.KS
383220.KS
402340.KS

# --- COREIA — KOSDAQ (22 papeis) ---
000250.KQ
005290.KQ
022100.KQ
028300.KQ
035760.KQ
039030.KQ
041510.KQ
053800.KQ
056190.KQ
065350.KQ
084370.KQ
095340.KQ
098460.KQ
121600.KQ
137400.KQ
140860.KQ
178320.KQ
214150.KQ
214450.KQ
253450.KQ
263750.KQ
403870.KQ
