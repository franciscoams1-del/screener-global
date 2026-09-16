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
ALTURA_ROLAGEM = 760          # altura da area rolavel, em pixels

# Sufixo do Yahoo -> nome da praca. "US" cobre NYSE e Nasdaq, que nao usam sufixo.
BOLSAS = {
    "US": "Estados Unidos — NYSE / Nasdaq",
    "SA": "Brasil — B3",
    "TO": "Canadá — Toronto",
    "L": "Reino Unido — Londres",
    "DE": "Alemanha — Frankfurt",
    "SW": "Suíça — Zurique",
    "PA": "França — Paris",
    "MI": "Itália — Milão",
    "T": "Japão — Tóquio",
    "KS": "Coreia — KOSPI",
    "KQ": "Coreia — KOSDAQ",
    "HK": "Hong Kong",
}


def bolsa_de(ticker: str) -> str:
    return ticker.split(".")[-1] if "." in ticker else "US"


def nome_bolsa(sufixo: str) -> str:
    return BOLSAS.get(sufixo, sufixo)

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

      .quality-row { margin-top:8px; font-size:.7rem; color:rgba(255,255,255,.7);
                     display:flex; justify-content:space-between; align-items:center; }
      .rs-badge { background:rgba(255,255,255,.14); border-radius:10px;
                  padding:2px 8px; font-weight:700; font-size:.68rem; color:#fff; }
      .rs-badge.forte { background:#1f7a4d; }
      .bolsa-header { margin:18px 0 6px 0; padding-bottom:5px;
                      border-bottom:1px solid rgba(255,255,255,.12);
                      font-size:.95rem; font-weight:700; color:#eaf6f0;
                      letter-spacing:.3px; }
      .bolsa-header span { font-weight:400; color:rgba(255,255,255,.5);
                           font-size:.8rem; margin-left:8px; }

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

    rs = row.get("rs_rating")
    rs_css = "rs-badge forte" if (rs or 0) >= 80 else "rs-badge"
    rs_html = f'<span class="{rs_css}">FR {rs}</span>' if rs else ""

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
        <div class="quality-row"><span>ROIC {roic} &nbsp;·&nbsp; ROIIC {roiic}</span>{rs_html}</div>
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

    st.markdown("##### Força relativa e timing")
    f1, f2, f3, f4 = st.columns(4)
    rs = row.get("rs_rating")
    f1.metric("Força relativa", f"{rs}/99" if rs else "—",
              f"#{row['rank_rs']} da lista" if row.get("rank_rs") else None)
    f2.metric("Retorno 6m", fmt_pct(row.get("rs_6m_pct")))
    ifr = row.get("rsi_14")
    if ifr is None:
        leitura = "—"
    elif ifr >= 70:
        leitura = "sobrecomprado"
    elif ifr <= 30:
        leitura = "sobrevendido"
    else:
        leitura = "neutro"
    f3.metric("IFR (14)", fmt_num(ifr, 1) if ifr is not None else "—", leitura)
    f4.metric("Praça", nome_bolsa(bolsa_de(row["ticker"])).split(" — ")[0])
    st.caption(
        "Força relativa compara esta ação com todo o universo varrido: 90 significa "
        "que rendeu mais que 90% das ações. O IFR de Wilder mede a própria ação — "
        "acima de 70 costuma indicar entrada esticada."
    )

    st.markdown("##### Posição técnica")
    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Preço", fmt_num(row.get("price")))
    t2.metric("Máx. 52s", fmt_num(row.get("high_52w")), fmt_pct(row.get("pct_from_high")))
    t3.metric("Mín. 52s", fmt_num(row.get("low_52w")), fmt_pct(row.get("pct_from_low")))
    t4.metric("Liquidez média 50d", fmt_big(row.get("avg_turnover_50d")))
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
        ["Força relativa", "ROIC", "ROIIC", "Variação %",
         "Distância da MM200", "IFR (14)", "Ticker"],
    )
    agrupar = st.toggle("Agrupar por bolsa", value=True)

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
    "Força relativa": lambda r: r.get("rs_rating") or 0,
    "IFR (14)": lambda r: r.get("rsi_14") or 0,
    "ROIC": lambda r: r.get("roic") or 0,
    "ROIIC": lambda r: r.get("roiic") or 0,
    "Variação %": lambda r: r.get("change_pct") or 0,
    "Distância da MM200": lambda r: r.get("dist_ma200_pct") or 0,
    "Ticker": lambda r: r["ticker"],
}[order]
rows = sorted(rows, key=sort_key, reverse=(order != "Ticker"))

if not rows:
    st.warning("Nenhum ativo atende aos filtros selecionados.")
    st.stop()

st.subheader(f"{len(rows)} ativos aprovados")
st.caption("Ordenados por força relativa, do mais forte para o mais fraco. "
           "Role a lista abaixo — todos os ativos estão nesta área.")


def desenhar_grade(itens: list[dict], prefixo: str) -> None:
    """Grade de 4 colunas. Colunas sobrando viram vazias para nao esticar."""
    for i in range(0, len(itens), CARDS_PER_ROW):
        fatia = itens[i:i + CARDS_PER_ROW]
        cols = st.columns(CARDS_PER_ROW)

        for col, row in zip(cols, fatia):
            with col:
                st.markdown(card_html(row), unsafe_allow_html=True)
                if st.button("Analisar", key=f"btn_{prefixo}_{row['ticker']}_{i}",
                             use_container_width=True):
                    analysis_dialog(row)

        for j in range(len(fatia), CARDS_PER_ROW):
            cols[j].empty()


# Toda a lista vive dentro de uma unica area rolavel: sem paginas, sem
# recarregar a tela a cada bloco de ativos.
with st.container(height=ALTURA_ROLAGEM, border=False):
    if not agrupar:
        desenhar_grade(rows, "todos")
    else:
        grupos: dict[str, list[dict]] = {}
        for row in rows:
            grupos.setdefault(bolsa_de(row["ticker"]), []).append(row)

        # Praca com o ativo mais forte aparece primeiro.
        ordem = sorted(
            grupos.items(),
            key=lambda kv: max((r.get("rs_rating") or 0) for r in kv[1]),
            reverse=True,
        )

        for sufixo, itens in ordem:
            melhor = max((r.get("rs_rating") or 0) for r in itens)
            st.markdown(
                f'<div class="bolsa-header">{nome_bolsa(sufixo)}'
                f'<span>{len(itens)} ativos · melhor força relativa {melhor}</span></div>',
                unsafe_allow_html=True,
            )
            desenhar_grade(itens, sufixo)

# --- Tabela bruta ----------------------------------------------------------
with st.expander("Ver dados em tabela"):
    show_cols = ["rank_rs", "ticker", "name", "exchange", "sector", "price",
                 "change_pct", "rs_rating", "rsi_14",
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
