"""
dashboard.py — Olist E-Commerce Dashboard
Fase 4: Visualización y análisis de negocio

Ejecutar:
    streamlit run dashboard/dashboard.py
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# =============================================================
#  CONFIG PÁGINA
# =============================================================
st.set_page_config(
    page_title="Olist · Data Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================
#  ESTILOS CUSTOM
# =============================================================
st.markdown("""
<style>

@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Sans:wght@300;400;500&display=swap');

/* --------- FONDO GENERAL --------- */

.stApp {
    background: #4c4f80;
}

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    color: #334155;
}


/* --------- TITULOS --------- */

h1, h2, h3 {
    font-family: 'Syne', sans-serif !important;
    color: #0F172A !important;
    letter-spacing: -0.02em;
}


/* --------- SIDEBAR --------- */

[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #EFF6FF 0%, #DBEAFE 100%);
    border-right: 1px solid #BFDBFE;
}

[data-testid="stSidebar"] * {
    color: #1E293B !important;
}


/* --------- HEADER DASHBOARD --------- */

.dash-header {
    background: linear-gradient(135deg, #EFF6FF 0%, #DBEAFE 100%);
    padding: 32px;
    border-radius: 20px;
    border: 1px solid #BFDBFE;
    margin-bottom: 35px;
}

.dash-header h1 {
    font-size: 2.4rem;
    font-weight: 800;
    margin: 0;
    color: #1E3A8A !important;
}

.dash-header p {
    margin-top: 10px;
    color: #475569;
}


/* --------- METRICAS --------- */

[data-testid="metric-container"] {

    background: white;

    border-radius: 16px;

    padding: 22px;

    border: 1px solid #DBEAFE;

    box-shadow: 
        0 10px 25px rgba(37, 99, 235, 0.08),
        0 2px 6px rgba(37, 99, 235, 0.05);

    transition: all 0.25s ease;
}

[data-testid="metric-container"]:hover {

    transform: translateY(-3px);

    box-shadow: 
        0 16px 35px rgba(37, 99, 235, 0.12),
        0 4px 10px rgba(37, 99, 235, 0.08);
}


[data-testid="stMetricValue"] {

    font-family: 'Syne', sans-serif !important;

    color: #2563EB !important;

    font-size: 2rem;
}

[data-testid="stMetricLabel"] {

    color: #64748B !important;

    text-transform: uppercase;

    letter-spacing: 0.05em;

    font-size: 0.8rem;
}


/* --------- DIVIDER --------- */

hr {
    border: none;
    border-top: 1px solid #E2E8F0;
    margin: 35px 0;
}


/* --------- BOTONES --------- */

.stButton>button {

    background: linear-gradient(135deg, #2563EB, #3B82F6);

    color: white;

    border: none;

    border-radius: 10px;

    padding: 10px 18px;

    font-weight: 500;

    transition: all 0.25s ease;

}

.stButton>button:hover {

    transform: translateY(-1px);

    box-shadow: 0 6px 15px rgba(37, 99, 235, 0.25);

}


/* --------- TABS --------- */

button[data-baseweb="tab"] {

    font-weight: 500;

}

button[data-baseweb="tab"][aria-selected="true"] {

    color: #2563EB;

    border-bottom: 3px solid #2563EB;

}


/* --------- DATAFRAME --------- */

[data-testid="stDataFrame"] {

    border-radius: 12px;

    overflow: hidden;

    border: 1px solid #E2E8F0;

}


/* --------- GRAFICOS --------- */

canvas {

    border-radius: 10px;

}

</style>
""", unsafe_allow_html=True)

# =============================================================
#  CONEXIÓN A BASE DE DATOS
# =============================================================
@st.cache_resource
def get_engine():
    cfg = {
        "host":     os.getenv("PGHOST",     "192.168.1.26"),
        "port":     os.getenv("PGPORT",     "5432"),
        "database": os.getenv("PGDATABASE", "olist_db"),
        "user":     os.getenv("PGUSER",     "postgres"),
        "password": os.getenv("PGPASSWORD", "admin"),
    }
    pw = quote_plus(cfg["password"])
    url = f"postgresql+psycopg2://{cfg['user']}:{pw}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return create_engine(url, pool_pre_ping=True)

@st.cache_data(ttl=300)
def query(_engine, sql: str) -> pd.DataFrame:
    with _engine.connect() as conn:
        return pd.read_sql(text(sql), conn)

try:
    engine = get_engine()
    with engine.connect() as c:
        c.execute(text("SELECT 1"))
    db_ok = True
except Exception as e:
    db_ok = False
    st.error(f"No se pudo conectar a PostgreSQL: {e}")
    st.stop()

# =============================================================
#  SIDEBAR — FILTROS GLOBALES
# =============================================================
with st.sidebar:
    st.markdown("## Olist Dashboard")
    st.markdown("---")

    # Rango de fechas
    years_df = query(engine, "SELECT DISTINCT order_year FROM fact_orders WHERE order_year IS NOT NULL ORDER BY 1")
    years = sorted(years_df["order_year"].dropna().astype(int).tolist())
    year_range = st.select_slider("Año", options=years, value=(min(years), max(years))) if years else (2016, 2018)

    st.markdown("---")

    # Estado del cliente
    states_df = query(engine, "SELECT DISTINCT state FROM dim_customers WHERE state IS NOT NULL ORDER BY 1")
    all_states = states_df["state"].tolist()
    sel_states = st.multiselect("Estado del cliente", all_states, default=[], placeholder="Todos los estados")

    st.markdown("---")

    # Estado del pedido
    status_df = query(engine, "SELECT DISTINCT order_status FROM fact_orders ORDER BY 1")
    all_status = status_df["order_status"].tolist()
    sel_status = st.multiselect("Estado del pedido", all_status, default=[], placeholder="Todos los estados")

    st.markdown("---")
    st.caption("Datos: Olist Brazilian E-Commerce · Kaggle")

# Construcción de cláusulas WHERE dinámicas
year_filter  = f"o.order_year BETWEEN {year_range[0]} AND {year_range[1]}"
state_clause = f"AND c.state IN ({','.join([repr(s) for s in sel_states])})" if sel_states else ""
status_clause= f"AND o.order_status IN ({','.join([repr(s) for s in sel_status])})" if sel_status else ""
base_filter  = f"WHERE {year_filter} {state_clause} {status_clause}"

# =============================================================
#  HEADER
# =============================================================
st.markdown("""
<div class="dash-header">
  <h1>DashBoard</h1>
  <p>Brazilian marketplace · 2016–2018 · Data Engineer Intern — Fase 4</p>
</div>
""", unsafe_allow_html=True)

# =============================================================
#  KPIs GLOBALES
# =============================================================
kpi_sql = f"""
SELECT
    COUNT(DISTINCT o.order_id)                                   AS total_orders,
    ROUND(SUM(p.payment_value)::NUMERIC, 0)                      AS total_revenue,
    COUNT(DISTINCT c.customer_unique_id)                         AS unique_buyers,
    ROUND(AVG(o.delivery_time_days)::NUMERIC, 1)                 AS avg_delivery_days,
    ROUND(
        COUNT(*) FILTER (WHERE o.order_status = 'canceled')::NUMERIC
        / NULLIF(COUNT(*),0) * 100, 2
    )                                                            AS cancel_rate,
    ROUND(AVG(r.review_score)::NUMERIC, 2)                       AS avg_review
FROM fact_orders o
JOIN dim_customers c  ON o.customer_id = c.customer_id
LEFT JOIN fact_payments p ON o.order_id = p.order_id
LEFT JOIN fact_reviews r  ON o.order_id = r.order_id
{base_filter}
"""
kpi = query(engine, kpi_sql).iloc[0]

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total pedidos",       f"{int(kpi.total_orders or 0):,}")
c2.metric("Revenue total",       f"R$ {float(kpi.total_revenue or 0):,.0f}")
c3.metric("Compradores únicos",  f"{int(kpi.unique_buyers or 0):,}")
c4.metric("Entrega promedio",    f"{float(kpi.avg_delivery_days or 0):.1f} días")
c5.metric("Tasa cancelación",    f"{float(kpi.cancel_rate or 0):.2f}%")
c6.metric("Score promedio",      f"⭐ {float(kpi.avg_review or 0):.2f}")

st.markdown("<hr>", unsafe_allow_html=True)

# =============================================================
#  Q1 — VOLUMEN POR MES + ESTACIONALIDAD
# =============================================================
st.markdown('<p class="section-label">Q1 — ¿Cuál es el volumen total de transacciones/registros por mes? ¿Hay estacionalidad?</p>', unsafe_allow_html=True)
st.subheader("Pedidos por mes")

q1_sql = f"""
SELECT
    o.order_yearmonth,
    COUNT(*)                                           AS total_orders,
    COUNT(*) FILTER (WHERE o.is_delivered = 1)         AS delivered,
    ROUND(SUM(p.payment_value)::NUMERIC, 0)            AS revenue
FROM fact_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
LEFT JOIN fact_payments p ON o.order_id = p.order_id
{base_filter}
  AND o.order_yearmonth IS NOT NULL
GROUP BY o.order_yearmonth
ORDER BY o.order_yearmonth
"""
q1 = query(engine, q1_sql)

if not q1.empty:
    col_left, col_right = st.columns([2, 1])

    with col_left:
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(
            x=q1["order_yearmonth"], y=q1["total_orders"],
            mode="lines+markers",
            name="Total pedidos",
            line=dict(color="#0f0f0f", width=2.5),
            marker=dict(size=5),
            fill="tozeroy", fillcolor="rgba(15,15,15,0.06)"
        ))
        fig1.add_trace(go.Scatter(
            x=q1["order_yearmonth"], y=q1["delivered"],
            mode="lines",
            name="Entregados",
            line=dict(color="#e85d04", width=1.5, dash="dot"),
        ))
        fig1.update_layout(
            height=320, margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", y=1.1),
            xaxis=dict(showgrid=False, tickangle=-45),
            yaxis=dict(gridcolor="#f0f0f0"),
            font=dict(family="DM Sans"),
        )
        st.plotly_chart(fig1, use_container_width=True)

    with col_right:
        fig1b = px.bar(
            q1, x="order_yearmonth", y="revenue",
            title="Revenue mensual (BRL)",
            color_discrete_sequence=["#0f0f0f"],
        )
        fig1b.update_layout(
            height=320, margin=dict(l=0, r=0, t=30, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=False, tickangle=-45, title=""),
            yaxis=dict(gridcolor="#f0f0f0", title=""),
            font=dict(family="DM Sans"),
            showlegend=False,
        )
        st.plotly_chart(fig1b, use_container_width=True)

    # Insight estacionalidad
    peak = q1.loc[q1["total_orders"].idxmax()]
    st.caption(f"Pico de ventas: **{peak['order_yearmonth']}** con **{int(peak['total_orders']):,}** pedidos")

st.markdown("<hr>", unsafe_allow_html=True)

# =============================================================
#  Q2 — TOP 10 CATEGORÍAS POR REVENUE
# =============================================================
st.markdown('<p class="section-label">Q2 — ¿Cuáles son los 10 clientes, productos o categorías con mayor valor generado?</p>', unsafe_allow_html=True)
st.subheader("Top 10 categorías por revenue")

q2_sql = f"""
SELECT
    p.category_name_en                          AS category,
    COUNT(DISTINCT oi.order_id)                 AS orders,
    ROUND(SUM(oi.price)::NUMERIC, 0)            AS revenue,
    ROUND(AVG(oi.price)::NUMERIC, 2)            AS avg_price,
    COUNT(*)                                    AS items_sold
FROM fact_order_items oi
JOIN dim_products p   ON oi.product_id = p.product_id
JOIN fact_orders o    ON oi.order_id   = o.order_id
JOIN dim_customers c  ON o.customer_id = c.customer_id
{base_filter.replace('WHERE', 'WHERE p.category_name_en IS NOT NULL AND')}
GROUP BY p.category_name_en
ORDER BY revenue DESC
LIMIT 10
"""
q2 = query(engine, q2_sql)

if not q2.empty:
    col_left, col_right = st.columns([3, 2])

    with col_left:
        fig2 = px.bar(
            q2.sort_values("revenue"),
            x="revenue", y="category",
            orientation="h",
            color="revenue",
            color_continuous_scale=["#f0ece4", "#e85d04", "#0f0f0f"],
            text="revenue",
        )
        fig2.update_traces(texttemplate="R$ %{text:,.0f}", textposition="outside")
        fig2.update_layout(
            height=380, margin=dict(l=0, r=80, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=False, title=""),
            yaxis=dict(title=""),
            coloraxis_showscale=False,
            font=dict(family="DM Sans"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    with col_right:
        fig2b = px.scatter(
            q2, x="avg_price", y="items_sold",
            size="revenue", color="category",
            hover_name="category",
            title="Precio promedio vs. ítems vendidos",
            size_max=50,
        )
        fig2b.update_layout(
            height=380, margin=dict(l=0, r=0, t=30, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#f0f0f0", title="Precio promedio (BRL)"),
            yaxis=dict(gridcolor="#f0f0f0", title="Ítems vendidos"),
            showlegend=False,
            font=dict(family="DM Sans"),
        )
        st.plotly_chart(fig2b, use_container_width=True)

st.markdown("<hr>", unsafe_allow_html=True)

# =============================================================
#  Q3 — TIEMPO DE ENTREGA
# =============================================================
st.markdown('<p class="section-label">Q3 — ¿Cuál es el tiempo promedio entre eventos clave del flujo?</p>', unsafe_allow_html=True)
st.subheader("Tiempo promedio compra → entrega por estado")

q3_sql = f"""
SELECT
    c.state,
    COUNT(*)                                              AS orders,
    ROUND(AVG(o.delivery_time_days)::NUMERIC, 1)          AS avg_days,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
          (ORDER BY o.delivery_time_days)::NUMERIC, 1)    AS median_days,
    ROUND(AVG(o.delay_days)::NUMERIC, 1)                  AS avg_delay,
    ROUND(
        COUNT(*) FILTER (WHERE o.delay_days > 0)::NUMERIC
        / NULLIF(COUNT(*),0) * 100, 1
    )                                                     AS late_pct
FROM fact_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
{base_filter}
  AND o.is_delivered = 1
  AND o.delivery_time_days IS NOT NULL
GROUP BY c.state
HAVING COUNT(*) >= 10
ORDER BY avg_days DESC
"""
q3 = query(engine, q3_sql)

if not q3.empty:
    col_left, col_right = st.columns(2)

    with col_left:
        fig3a = px.bar(
            q3.head(15), x="state", y=["avg_days", "median_days"],
            barmode="group",
            color_discrete_map={"avg_days": "#0f0f0f", "median_days": "#e85d04"},
            labels={"value": "Días", "variable": "Métrica", "state": "Estado"},
            title="Top 15 estados con mayor tiempo de entrega",
        )
        fig3a.update_layout(
            height=350, margin=dict(l=0, r=0, t=30, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=False),
            yaxis=dict(gridcolor="#f0f0f0"),
            legend=dict(orientation="h", y=1.1,
                        itemsizing="constant",
                        title_text=""),
            font=dict(family="DM Sans"),
        )
        fig3a.for_each_trace(lambda t: t.update(
            name="Promedio" if t.name == "avg_days" else "Mediana"
        ))
        st.plotly_chart(fig3a, use_container_width=True)

    with col_right:
        fig3b = px.scatter(
            q3, x="avg_days", y="late_pct",
            size="orders", color="avg_delay",
            hover_name="state",
            color_continuous_scale=["#0f9e6a", "#f5f5f0", "#e85d04"],
            labels={"avg_days": "Días promedio de entrega",
                    "late_pct": "% pedidos tardíos",
                    "avg_delay": "Retraso promedio (días)"},
            title="Entrega promedio vs. % pedidos tardíos",
        )
        fig3b.update_layout(
            height=350, margin=dict(l=0, r=0, t=30, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#f0f0f0"),
            yaxis=dict(gridcolor="#f0f0f0"),
            font=dict(family="DM Sans"),
        )
        st.plotly_chart(fig3b, use_container_width=True)

st.markdown("<hr>", unsafe_allow_html=True)

# =============================================================
#  Q4 — INCIDENCIAS Y RESULTADOS NEGATIVOS
# =============================================================
st.markdown('<p class="section-label">Q4 — ¿Qué porcentaje de registros tiene algún tipo de incidencia o resultado negativo?</p>', unsafe_allow_html=True)
st.subheader("Porcentaje de pedidos con incidencias")

q4_sql = f"""
WITH flags AS (
    SELECT
        o.order_id,
        CASE WHEN o.order_status = 'canceled'  THEN 1 ELSE 0 END AS is_canceled,
        CASE WHEN o.delay_days > 0             THEN 1 ELSE 0 END AS is_late,
        COALESCE(r.is_negative, 0)                               AS is_negative
    FROM fact_orders o
    JOIN dim_customers c ON o.customer_id = c.customer_id
    LEFT JOIN fact_reviews r ON o.order_id = r.order_id
    {base_filter}
)
SELECT
    SUM(is_canceled)                              AS cancelados,
    SUM(is_late)                                  AS tardios,
    SUM(is_negative)                              AS resenas_negativas,
    COUNT(*) - SUM(GREATEST(is_canceled, is_late, is_negative)) AS sin_incidencia,
    COUNT(*)                                      AS total
FROM flags
"""
q4 = query(engine, q4_sql).iloc[0]
total = int(q4["total"] or 1)

col_left, col_right = st.columns([1, 2])

with col_left:
    labels  = ["Sin incidencia", "Cancelados", "Entrega tardía", "Reseña negativa"]
    values  = [
        int(q4["sin_incidencia"] or 0),
        int(q4["cancelados"] or 0),
        int(q4["tardios"] or 0),
        int(q4["resenas_negativas"] or 0),
    ]
    colors  = ["#0f0f0f", "#e85d04", "#f4a261", "#e9c46a"]
    fig4a = go.Figure(go.Pie(
        labels=labels, values=values,
        hole=0.6,
        marker_colors=colors,
        textinfo="percent",
        hovertemplate="%{label}: %{value:,}<extra></extra>",
    ))
    fig4a.add_annotation(
        text=f"<b>{total:,}</b><br>pedidos",
        x=0.5, y=0.5, showarrow=False,
        font=dict(size=14, family="Syne"),
    )
    fig4a.update_layout(
        height=320, margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        legend=dict(orientation="v"),
        showlegend=True,
    )
    st.plotly_chart(fig4a, use_container_width=True)

with col_right:
    q4b_sql = f"""
    SELECT
        r.review_score,
        COUNT(*) AS total,
        ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 1) AS pct
    FROM fact_reviews r
    JOIN fact_orders o  ON r.order_id  = o.order_id
    JOIN dim_customers c ON o.customer_id = c.customer_id
    {base_filter}
    GROUP BY r.review_score
    ORDER BY r.review_score
    """
    q4b = query(engine, q4b_sql)
    if not q4b.empty:
        score_colors = {1:"#e63946", 2:"#f4a261", 3:"#e9c46a", 4:"#90be6d", 5:"#2d6a4f"}
        fig4b = px.bar(
            q4b, x="review_score", y="total",
            color="review_score",
            color_discrete_map=score_colors,
            text="pct",
            labels={"review_score": "Score", "total": "Reseñas"},
            title="Distribución de review scores",
        )
        fig4b.update_traces(texttemplate="%{text}%", textposition="outside")
        fig4b.update_layout(
            height=320, margin=dict(l=0, r=0, t=30, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=False, tickmode="linear"),
            yaxis=dict(gridcolor="#f0f0f0"),
            showlegend=False,
            font=dict(family="DM Sans"),
        )
        st.plotly_chart(fig4b, use_container_width=True)

st.markdown("<hr>", unsafe_allow_html=True)

# =============================================================
#  Q5 — ANÁLISIS LIBRE: Perfil de vendedores
# =============================================================
st.markdown('<p class="section-label">Q5 — Análisis propio</p>', unsafe_allow_html=True)
st.subheader("¿Qué vendedores tienen mejor ratio calidad / volumen?")
st.caption("Vendedores con ≥ 30 pedidos entregados · Tamaño del punto = revenue total")

q5_sql = f"""
SELECT
    oi.seller_id,
    s.state                                          AS seller_state,
    COUNT(DISTINCT oi.order_id)                      AS orders,
    ROUND(SUM(oi.price)::NUMERIC, 0)                 AS revenue,
    ROUND(AVG(r.review_score)::NUMERIC, 2)           AS avg_score,
    ROUND(
        COUNT(*) FILTER (WHERE r.is_negative = 1)::NUMERIC
        / NULLIF(COUNT(DISTINCT oi.order_id), 0) * 100, 1
    )                                                AS negative_pct,
    ROUND(AVG(o.delivery_time_days)::NUMERIC, 1)     AS avg_delivery_days,
    ROUND(
        COUNT(*) FILTER (WHERE o.delay_days > 0)::NUMERIC
        / NULLIF(COUNT(DISTINCT oi.order_id), 0) * 100, 1
    )                                                AS late_pct
FROM fact_order_items oi
JOIN fact_orders  o  ON oi.order_id  = o.order_id
JOIN dim_sellers  s  ON oi.seller_id = s.seller_id
JOIN dim_customers c ON o.customer_id = c.customer_id
LEFT JOIN fact_reviews r ON o.order_id = r.order_id
{base_filter}
  AND o.is_delivered = 1
GROUP BY oi.seller_id, s.state
HAVING COUNT(DISTINCT oi.order_id) >= 30
ORDER BY avg_score DESC, revenue DESC
"""
q5 = query(engine, q5_sql)

if not q5.empty:
    col_l, col_r = st.columns([3, 2])

    with col_l:
        fig5 = px.scatter(
            q5,
            x="avg_delivery_days",
            y="avg_score",
            size="revenue",
            color="late_pct",
            hover_name="seller_id",
            hover_data={"orders": True, "revenue": ":,.0f",
                        "negative_pct": ":.1f", "seller_state": True},
            color_continuous_scale=["#2d6a4f", "#e9c46a", "#e63946"],
            size_max=40,
            labels={
                "avg_delivery_days": "Días promedio de entrega",
                "avg_score":         "Score promedio",
                "late_pct":          "% tardíos",
            },
        )
        fig5.update_layout(
            height=420, margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#f0f0f0"),
            yaxis=dict(gridcolor="#f0f0f0", range=[1, 5.5]),
            font=dict(family="DM Sans"),
            coloraxis_colorbar=dict(title="% tardíos"),
        )
        st.plotly_chart(fig5, use_container_width=True)

    with col_r:
        # Top 10 vendedores por score + volumen
        top10 = q5.head(10)[["seller_id", "orders", "revenue", "avg_score", "late_pct"]].copy()
        top10["seller_id"] = top10["seller_id"].str[:8] + "…"
        top10.columns = ["Vendedor", "Pedidos", "Revenue (BRL)", "Score ⭐", "% Tardíos"]
        top10["Revenue (BRL)"] = top10["Revenue (BRL)"].apply(lambda x: f"R$ {x:,.0f}")
        st.markdown("**Top 10 vendedores por calidad**")
        st.dataframe(
            top10,
            use_container_width=True,
            hide_index=True,
            height=380,
        )

# =============================================================
#  FOOTER
# =============================================================
st.markdown("<hr>", unsafe_allow_html=True)
st.caption("Olist Brazilian E-Commerce Dataset · Prueba Técnica Data Engineer Intern")
