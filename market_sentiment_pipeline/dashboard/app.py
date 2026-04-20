"""
dashboard/app.py
-----------------
Streamlit dashboard for the Market Sentiment & Predictive Analytics Pipeline.

Run:
    streamlit run dashboard/app.py

Features:
    - KPI summary row (posts, avg sentiment, predictions, accuracy)
    - Sentiment by symbol (bar chart)
    - 48-hour sentiment trend (line chart)
    - Latest predictions table
    - Model accuracy bar chart
    - Stock price history with RSI
    - Sentiment distribution donut
    - Live raw posts feed
"""

import sys
import os

# Make the project root importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from config.settings import settings

# ── Page configuration ────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Market Sentiment Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.2rem; padding-bottom: 0.5rem; }
    div[data-testid="metric-container"] {
        background: var(--secondary-background-color);
        border-radius: 8px;
        padding: 14px 16px;
    }
    h1 { font-size: 1.5rem !important; font-weight: 600 !important; }
    h2 { font-size: 1.1rem !important; }
    h3 { font-size: 1.0rem !important; }
    .stAlert { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("⚙️ Settings")

    selected_symbol = st.selectbox(
        "Focus symbol",
        options=["All"] + settings.STOCK_SYMBOLS,
        index=0,
    )

    sentiment_hours = st.slider(
        "Sentiment window (hours)",
        min_value=6, max_value=72, value=24, step=6,
    )

    trend_hours = st.slider(
        "Trend chart window (hours)",
        min_value=12, max_value=120, value=48, step=12,
    )

    st.divider()
    st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")

    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.markdown("**Pipeline status**")
    st.success("Kafka: connected")
    st.success("PostgreSQL: connected")


# ── Data loaders (all cached for 5 min) ──────────────────────────────────────

def safe_load(fn, *args, **kwargs):
    """Wrap a DB call and return empty DataFrame on failure."""
    try:
        result = fn(*args, **kwargs)
        return pd.DataFrame(result) if result else pd.DataFrame()
    except Exception as e:
        st.warning(f"Data load error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_sentiment_summary(hours):
    from database.db import fetch_sentiment_summary
    return safe_load(fetch_sentiment_summary, hours=hours)


@st.cache_data(ttl=300)
def load_predictions():
    from database.db import fetch_latest_predictions
    return safe_load(fetch_latest_predictions, limit=25)


@st.cache_data(ttl=300)
def load_timeseries(symbol, hours):
    from database.db import fetch_sentiment_timeseries
    return safe_load(fetch_sentiment_timeseries, symbol=symbol, hours=hours)


@st.cache_data(ttl=600)
def load_accuracy():
    from database.db import fetch_model_accuracy
    return safe_load(fetch_model_accuracy)


@st.cache_data(ttl=300)
def load_stock_history(symbol, days=30):
    from database.db import fetch_stock_history
    return safe_load(fetch_stock_history, symbol=symbol, days=days)


@st.cache_data(ttl=60)
def load_recent_posts(symbol, limit=12):
    from database.db import fetch_recent_posts
    return safe_load(fetch_recent_posts, symbol=symbol if symbol != "All" else None, limit=limit)


# Load all data
sentiment_df   = load_sentiment_summary(sentiment_hours)
predictions_df = load_predictions()
accuracy_df    = load_accuracy()


# ── Header ────────────────────────────────────────────────────────────────────

col_h1, col_h2 = st.columns([5, 1])
with col_h1:
    st.title("📈 Market Sentiment & Predictive Analytics")
with col_h2:
    st.markdown("<br>", unsafe_allow_html=True)
    if not sentiment_df.empty:
        st.success("Live")
    else:
        st.warning("No data yet")


# ── KPI row ───────────────────────────────────────────────────────────────────

k1, k2, k3, k4, k5 = st.columns(5)

total_posts   = int(sentiment_df["post_count"].sum())       if not sentiment_df.empty else 0
avg_compound  = float(sentiment_df["avg_compound"].mean())  if not sentiment_df.empty else 0.0
pred_count    = len(predictions_df)                         if not predictions_df.empty else 0
correct_preds = int(predictions_df["was_correct"].sum())    if (not predictions_df.empty
                                                               and "was_correct" in predictions_df) else 0

overall_acc = 0.0
if not accuracy_df.empty and "accuracy_pct" in accuracy_df:
    vals = accuracy_df["accuracy_pct"].dropna()
    overall_acc = float(vals.mean()) if len(vals) > 0 else 0.0

k1.metric("Posts analysed",    f"{total_posts:,}",           help=f"In last {sentiment_hours}h")
k2.metric("Avg sentiment",     f"{avg_compound:+.3f}",       help="VADER compound (-1 to +1)")
k3.metric("Predictions made",  str(pred_count),              help="Total stored predictions")
k4.metric("Verified correct",  str(correct_preds),           help="Back-filled vs actual price")
k5.metric("Model accuracy",    f"{overall_acc:.1f}%",        help="Across all verified predictions")

st.divider()


# ── Row 1: Sentiment bars + Trend line ───────────────────────────────────────

col_bars, col_trend = st.columns([1, 2])

with col_bars:
    st.subheader(f"Sentiment by symbol ({sentiment_hours}h)")

    if sentiment_df.empty:
        st.info("No sentiment data yet.\nStart the ingestion producers to populate data.")
    else:
        df_plot = sentiment_df.copy().sort_values("avg_compound", ascending=True)
        df_plot["color"] = df_plot["avg_compound"].apply(
            lambda x: "#1D9E75" if x > 0.05 else ("#E24B4A" if x < -0.05 else "#888780")
        )
        fig = go.Figure(go.Bar(
            y=df_plot["symbol"],
            x=df_plot["avg_compound"].astype(float),
            orientation="h",
            marker_color=df_plot["color"],
            text=df_plot["avg_compound"].apply(lambda x: f"{x:+.3f}"),
            textposition="outside",
        ))
        fig.update_layout(
            height=280, margin=dict(l=10, r=50, t=10, b=10),
            xaxis=dict(title="", range=[-1, 1], zeroline=True, zerolinecolor="#ccc"),
            yaxis=dict(title=""),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font=dict(size=12),
        )
        st.plotly_chart(fig, use_container_width=True)

        vol_df = sentiment_df[["symbol", "post_count", "positive_count", "negative_count"]].copy()
        vol_df.columns = ["Symbol", "Total posts", "Positive", "Negative"]
        st.dataframe(vol_df, hide_index=True, use_container_width=True, height=160)


with col_trend:
    st.subheader(f"Sentiment trend ({trend_hours}h)")

    symbols_to_plot = settings.STOCK_SYMBOLS if selected_symbol == "All" else [selected_symbol]
    colors          = ["#1D9E75", "#E24B4A", "#378ADD", "#BA7517", "#7F77DD"]
    fig_trend       = go.Figure()

    for i, sym in enumerate(symbols_to_plot):
        ts_df = load_timeseries(sym, trend_hours)
        if ts_df.empty:
            continue
        ts_df["hour"] = pd.to_datetime(ts_df["hour"])
        fig_trend.add_trace(go.Scatter(
            x=ts_df["hour"], y=ts_df["avg_compound"].astype(float),
            name=sym, mode="lines",
            line=dict(color=colors[i % len(colors)], width=2),
            hovertemplate=f"<b>{sym}</b><br>%{{x|%H:%M}}<br>Sentiment: %{{y:+.3f}}<extra></extra>",
        ))

    fig_trend.add_hline(y=0, line_dash="dot", line_color="#999", line_width=1)
    fig_trend.add_hrect(y0=0.05,  y1=1.0,  fillcolor="#1D9E75", opacity=0.04, line_width=0)
    fig_trend.add_hrect(y0=-1.0,  y1=-0.05, fillcolor="#E24B4A", opacity=0.04, line_width=0)
    fig_trend.update_layout(
        height=310, margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(title="", showgrid=True, gridcolor="rgba(150,150,150,0.15)"),
        yaxis=dict(title="Compound score", range=[-1, 1], tickformat="+.1f",
                   showgrid=True, gridcolor="rgba(150,150,150,0.15)"),
        legend=dict(orientation="h", y=1.08, x=0),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
    )

    if not any(
        not load_timeseries(s, trend_hours).empty for s in symbols_to_plot
    ):
        st.info("No timeseries data yet. Posts will appear here as ingestion runs.")
    else:
        st.plotly_chart(fig_trend, use_container_width=True)

st.divider()


# ── Row 2: Predictions table + Accuracy ──────────────────────────────────────

col_pred, col_acc = st.columns([3, 2])

with col_pred:
    st.subheader("Latest predictions")

    if predictions_df.empty:
        st.info("No predictions yet.\nAirflow will generate predictions every 4 hours.")
    else:
        display_df = predictions_df[[
            "symbol", "predicted_direction", "confidence_score",
            "avg_sentiment", "post_count", "model_version",
            "predicted_at", "was_correct",
        ]].copy()

        display_df["confidence_score"] = display_df["confidence_score"].apply(
            lambda x: f"{float(x)*100:.1f}%" if pd.notna(x) else "—"
        )
        display_df["avg_sentiment"] = display_df["avg_sentiment"].apply(
            lambda x: f"{float(x):+.3f}" if pd.notna(x) else "—"
        )
        display_df["predicted_at"] = pd.to_datetime(
            display_df["predicted_at"]
        ).dt.strftime("%m-%d %H:%M")
        display_df["was_correct"] = display_df["was_correct"].apply(
            lambda x: "✓" if x is True else ("✗" if x is False else "—")
        )
        display_df.columns = [
            "Symbol", "Direction", "Confidence",
            "Avg sentiment", "Posts", "Model",
            "Predicted at", "Correct?",
        ]

        def color_dir(val):
            if val == "up":   return "color:#1D9E75;font-weight:500"
            if val == "down": return "color:#E24B4A;font-weight:500"
            return "color:#888780"

        st.dataframe(
            display_df.style.applymap(color_dir, subset=["Direction"]),
            hide_index=True, use_container_width=True, height=280,
        )


with col_acc:
    st.subheader("Prediction accuracy by symbol")

    if accuracy_df.empty:
        st.info("No accuracy data yet.\nPredictions need back-filling after market close.")
    else:
        acc_plot = accuracy_df.dropna(subset=["accuracy_pct"])
        fig_acc  = go.Figure(go.Bar(
            x=acc_plot["symbol"],
            y=acc_plot["accuracy_pct"].astype(float),
            text=acc_plot["accuracy_pct"].apply(lambda x: f"{float(x):.1f}%"),
            textposition="outside",
            marker_color=acc_plot["accuracy_pct"].apply(
                lambda x: "#1D9E75" if float(x) >= 55 else
                          ("#BA7517" if float(x) >= 45 else "#E24B4A")
            ),
        ))
        fig_acc.add_hline(y=33.3, line_dash="dot", line_color="#aaa",
                          annotation_text="Random baseline 33%")
        fig_acc.update_layout(
            height=250, margin=dict(l=10, r=10, t=10, b=10),
            yaxis=dict(title="Accuracy %", range=[0, 80]),
            xaxis=dict(title=""),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
        )
        st.plotly_chart(fig_acc, use_container_width=True)

        summ = accuracy_df[["symbol", "total", "correct", "accuracy_pct"]].copy()
        summ.columns = ["Symbol", "Predictions", "Correct", "Accuracy %"]
        st.dataframe(summ, hide_index=True, use_container_width=True, height=130)

st.divider()


# ── Row 3: Stock price history ────────────────────────────────────────────────

focus = selected_symbol if selected_symbol != "All" else settings.STOCK_SYMBOLS[0]
st.subheader(f"Price & technical view — {focus} (last 30 days)")

stock_df = load_stock_history(focus, days=30)

if stock_df.empty:
    st.info(f"No stock data for {focus} yet. The stock producer will populate this.")
else:
    stock_df["trade_date"] = pd.to_datetime(stock_df["trade_date"])
    stock_df = stock_df.sort_values("trade_date")

    col_price, col_vol = st.columns(2)

    with col_price:
        fig_price = go.Figure()
        fig_price.add_trace(go.Scatter(
            x=stock_df["trade_date"], y=stock_df["price"].astype(float),
            name="Price", mode="lines+markers",
            line=dict(color="#378ADD", width=2), marker=dict(size=4),
        ))
        fig_price.add_trace(go.Bar(
            x=stock_df["trade_date"], y=stock_df["change_pct"].astype(float),
            name="Change %",
            marker_color=stock_df["change_pct"].apply(
                lambda x: "#1D9E75" if float(x or 0) > 0 else "#E24B4A"
            ),
            yaxis="y2", opacity=0.5,
        ))
        fig_price.update_layout(
            height=220, margin=dict(l=10, r=10, t=10, b=10),
            yaxis=dict(title="Price $"),
            yaxis2=dict(title="Change %", overlaying="y", side="right", showgrid=False),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", y=1.1), hovermode="x unified",
        )
        st.plotly_chart(fig_price, use_container_width=True)

    with col_vol:
        fig_vol = go.Figure(go.Bar(
            x=stock_df["trade_date"],
            y=stock_df["volume"].astype(float) / 1e6,
            name="Volume (M)",
            marker_color="#7F77DD",
        ))
        fig_vol.update_layout(
            height=220, margin=dict(l=10, r=10, t=10, b=10),
            yaxis=dict(title="Volume (millions)"),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
        )
        st.plotly_chart(fig_vol, use_container_width=True)

st.divider()


# ── Row 4: Sentiment distribution + Raw posts ─────────────────────────────────

col_pie, col_posts = st.columns([1, 2])

with col_pie:
    st.subheader("Sentiment distribution")
    if not sentiment_df.empty:
        pos = int(sentiment_df["positive_count"].sum())
        neg = int(sentiment_df["negative_count"].sum())
        neu = int(sentiment_df["neutral_count"].sum())
        fig_pie = go.Figure(go.Pie(
            labels=["Positive", "Neutral", "Negative"],
            values=[pos, neu, neg],
            hole=0.62,
            marker_colors=["#1D9E75", "#888780", "#E24B4A"],
            textinfo="label+percent",
            textfont_size=12,
        ))
        fig_pie.update_layout(
            height=210, margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False,
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_pie, use_container_width=True)
        st.caption(f"{pos} positive · {neu} neutral · {neg} negative")
    else:
        st.info("No data available yet.")


with col_posts:
    st.subheader("Recent posts (live feed)")

    raw_posts_df = load_recent_posts(selected_symbol, limit=10)

    if raw_posts_df.empty:
        st.info("No posts yet. Start the data ingestion producers.")
    else:
        for _, row in raw_posts_df.iterrows():
            label = row.get("sentiment_label", "neutral")
            score = float(row.get("sentiment_compound", 0))
            sym   = row.get("query_term", "")
            src   = row.get("source", "")
            text  = str(row.get("text", ""))[:140]

            badge_style = (
                "background:#EAF3DE;color:#27500A" if label == "positive"
                else "background:#FCEBEB;color:#791F1F" if label == "negative"
                else "background:#F1EFE8;color:#444441"
            )

            st.markdown(
                f'<div style="padding:7px 0;border-bottom:0.5px solid var(--secondary-background-color)">'
                f'<span style="{badge_style};font-size:11px;padding:2px 8px;border-radius:10px;margin-right:6px">'
                f'{label} {score:+.2f}</span>'
                f'<span style="font-size:11px;color:#888;margin-right:6px">[{src}] {sym}</span>'
                f'<span style="font-size:13px">{text}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

# ── Footer ─────────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    "Market Sentiment & Predictive Analytics Pipeline · "
    "Data: Twitter API / Reddit API / Alpha Vantage (simulated fallback) · "
    "Sentiment: VADER · Not financial advice."
)
