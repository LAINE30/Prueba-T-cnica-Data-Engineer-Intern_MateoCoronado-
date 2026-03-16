"""
pipeline_runner.py
------------------
Script orquestado por n8n. Cada paso se puede invocar
individualmente via argumento, o ejecutar todo el flujo completo.

Uso:
    python pipeline/pipeline_runner.py --step all
    python pipeline/pipeline_runner.py --step clean
    python pipeline/pipeline_runner.py --step load
    python pipeline/pipeline_runner.py --step report

Salida: JSON a stdout para que n8n pueda leer el resultado.
"""

import os
import sys
import json
import time
import argparse
import traceback
from datetime import datetime

# Raíz del proyecto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

# =========================================================
#  HELPERS
# =========================================================

def result(step: str, status: str, message: str, data: dict = None):
    """Imprime resultado como JSON — n8n lo lee del stdout."""
    out = {
        "step":      step,
        "status":    status,   # "ok" | "error"
        "message":   message,
        "timestamp": datetime.now().isoformat(),
        "data":      data or {},
    }
    print(json.dumps(out, ensure_ascii=False))
    return out

def elapsed(t0):
    return round(time.time() - t0, 2)


# =========================================================
#  PASO 1: LIMPIEZA
# =========================================================

def step_clean():
    t0 = time.time()
    try:
        import pandas as pd
        import numpy as np
        import unicodedata
        import re

        RAW   = os.path.join(BASE_DIR, "data", "raw")
        CLEAN = os.path.join(BASE_DIR, "data", "clean")
        os.makedirs(CLEAN, exist_ok=True)

        def normalize_text(s):
            if pd.isna(s): return s
            s = str(s).lower().strip()
            s = unicodedata.normalize("NFKD", s)
            s = "".join(c for c in s if not unicodedata.combining(c))
            return re.sub(r"\s+", " ", s)

        stats = {}

        # --- geolocation ---
        geo = pd.read_csv(f"{RAW}/olist_geolocation_dataset.csv")
        geo = geo[geo["geolocation_lat"].between(-33.7, 5.3) &
                  geo["geolocation_lng"].between(-73.9, -28.8)]
        geo["geolocation_city"] = geo["geolocation_city"].apply(normalize_text)
        geo = (geo.groupby("geolocation_zip_code_prefix", as_index=False)
                  .agg(geolocation_lat=("geolocation_lat","median"),
                       geolocation_lng=("geolocation_lng","median"),
                       geolocation_city=("geolocation_city", lambda x: x.mode().iloc[0]),
                       geolocation_state=("geolocation_state", lambda x: x.mode().iloc[0])))
        geo.to_csv(f"{CLEAN}/geolocation_clean.csv", index=False)
        stats["geolocation"] = len(geo)

        # --- customers ---
        cust = pd.read_csv(f"{RAW}/olist_customers_dataset.csv")
        cust["customer_city"]  = cust["customer_city"].apply(normalize_text)
        cust["customer_state"] = cust["customer_state"].str.upper().str.strip()
        cust.to_csv(f"{CLEAN}/customers_clean.csv", index=False)
        stats["customers"] = len(cust)

        # --- sellers ---
        sell = pd.read_csv(f"{RAW}/olist_sellers_dataset.csv")
        sell["seller_city"]  = sell["seller_city"].apply(normalize_text)
        sell["seller_state"] = sell["seller_state"].str.upper().str.strip()
        sell.to_csv(f"{CLEAN}/sellers_clean.csv", index=False)
        stats["sellers"] = len(sell)

        # --- products ---
        prod  = pd.read_csv(f"{RAW}/olist_products_dataset.csv")
        trans = pd.read_csv(f"{RAW}/product_category_name_translation.csv")
        tmap  = dict(zip(trans["product_category_name"], trans["product_category_name_english"]))
        prod["product_category_name"] = prod["product_category_name"].fillna("unknown")
        prod["product_category_name_english"] = (prod["product_category_name"]
                                                  .map(tmap)
                                                  .fillna(prod["product_category_name"]))
        dim_cols = ["product_weight_g","product_length_cm","product_height_cm","product_width_cm"]
        for col in dim_cols:
            prod[col] = prod[col].replace(0, np.nan)
            prod[col] = prod[col].fillna(
                prod.groupby("product_category_name")[col].transform("median")
            ).fillna(prod[col].median())
        prod.to_csv(f"{CLEAN}/products_clean.csv", index=False)
        stats["products"] = len(prod)

        # --- orders ---
        ord_ = pd.read_csv(f"{RAW}/olist_orders_dataset.csv")
        date_cols = ["order_purchase_timestamp","order_approved_at",
                     "order_delivered_carrier_date","order_delivered_customer_date",
                     "order_estimated_delivery_date"]
        for c in date_cols:
            ord_[c] = pd.to_datetime(ord_[c], errors="coerce")
        ord_["is_delivered"]    = (ord_["order_status"] == "delivered").astype(int)
        ord_["delivery_time_days"] = (ord_["order_delivered_customer_date"]
                                      - ord_["order_purchase_timestamp"]).dt.days
        ord_["delay_days"]      = (ord_["order_delivered_customer_date"]
                                   - ord_["order_estimated_delivery_date"]).dt.days
        ord_["order_year"]      = ord_["order_purchase_timestamp"].dt.year
        ord_["order_month"]     = ord_["order_purchase_timestamp"].dt.month
        ord_["order_yearmonth"] = ord_["order_purchase_timestamp"].dt.to_period("M").astype(str)
        items = pd.read_csv(f"{RAW}/olist_order_items_dataset.csv")
        ord_["has_items"] = ord_["order_id"].isin(set(items["order_id"])).astype(int)
        ord_.to_csv(f"{CLEAN}/orders_clean.csv", index=False)
        stats["orders"] = len(ord_)

        # --- order_items ---
        items["shipping_limit_date"] = pd.to_datetime(items["shipping_limit_date"], errors="coerce")
        items["total_item_value"]    = items["price"] + items["freight_value"]
        items.to_csv(f"{CLEAN}/order_items_clean.csv", index=False)
        stats["order_items"] = len(items)

        # --- payments ---
        pay = pd.read_csv(f"{RAW}/olist_order_payments_dataset.csv")
        pay = pay[pay["payment_value"] > 0].copy()
        pay["payment_type"] = pay["payment_type"].str.lower().str.strip()
        pay.to_csv(f"{CLEAN}/payments_clean.csv", index=False)
        stats["payments"] = len(pay)

        # --- reviews ---
        rev = pd.read_csv(f"{RAW}/olist_order_reviews_dataset.csv")
        rev["review_creation_date"]    = pd.to_datetime(rev["review_creation_date"],    errors="coerce")
        rev["review_answer_timestamp"] = pd.to_datetime(rev["review_answer_timestamp"], errors="coerce")
        rev = (rev.sort_values("review_answer_timestamp", ascending=False)
                  .drop_duplicates(subset="order_id", keep="first"))
        rev["review_comment_title"]   = rev["review_comment_title"].fillna("")
        rev["review_comment_message"] = rev["review_comment_message"].fillna("")
        rev["is_negative"] = (rev["review_score"] <= 2).astype(int)
        rev["has_comment"] = (rev["review_comment_message"].str.len() > 0).astype(int)
        rev.to_csv(f"{CLEAN}/reviews_clean.csv", index=False)
        stats["reviews"] = len(rev)

        return result("clean", "ok",
                      f"Limpieza completada en {elapsed(t0)}s",
                      {"rows_clean": stats, "elapsed_s": elapsed(t0)})

    except Exception as e:
        return result("clean", "error", str(e),
                      {"traceback": traceback.format_exc()})


# =========================================================
#  PASO 2: CARGA A POSTGRESQL
# =========================================================

def step_load():
    t0 = time.time()
    try:
        # Importar el loader existente como módulo
        loader_path = os.path.join(BASE_DIR, "scripts", "load_to_postgres.py")
        import importlib.util
        spec = importlib.util.spec_from_file_location("loader", loader_path)
        loader = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(loader)

        engine = loader.get_engine()

        # Test conexión
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        # Ejecutar schema + carga
        loader.run_schema(engine, loader.SCHEMA_FILE)
        loader.set_fk_checks(engine, enabled=False)

        counts = {}
        for csv_file, table_name, chunksize in loader.LOAD_ORDER:
            csv_path = os.path.join(loader.CLEAN_DIR, csv_file)
            if not os.path.exists(csv_path):
                counts[table_name] = 0
                continue
            rows = loader.load_table(engine, csv_path, table_name, chunksize)
            counts[table_name] = rows

        loader.set_fk_checks(engine, enabled=True)
        total = sum(counts.values())

        return result("load", "ok",
                      f"Carga completada: {total:,} filas en {elapsed(t0)}s",
                      {"tables": counts, "total_rows": total, "elapsed_s": elapsed(t0)})

    except Exception as e:
        return result("load", "error", str(e),
                      {"traceback": traceback.format_exc()})


# =========================================================
#  PASO 3: GENERAR REPORTE
# =========================================================

def step_report():
    t0 = time.time()
    try:
        import pandas as pd
        from sqlalchemy import create_engine, text
        from urllib.parse import quote_plus

        cfg = {
            "host":     os.getenv("PGHOST",     "192.168.1.26"),
            "port":     os.getenv("PGPORT",     "5432"),
            "database": os.getenv("PGDATABASE", "olist_db"),
            "user":     os.getenv("PGUSER",     "postgres"),
            "password": os.getenv("PGPASSWORD", "postgres"),
        }
        pw  = quote_plus(cfg["password"])
        url = f"postgresql+psycopg2://{cfg['user']}:{pw}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        eng = create_engine(url)

        def q(sql):
            with eng.connect() as conn:
                return pd.read_sql(text(sql), conn)

        # KPIs
        kpi = q("""
            SELECT
                COUNT(DISTINCT o.order_id)                               AS total_orders,
                ROUND(SUM(p.payment_value)::NUMERIC, 0)                  AS total_revenue,
                ROUND(AVG(o.delivery_time_days)::NUMERIC, 1)             AS avg_delivery_days,
                ROUND(COUNT(*) FILTER (WHERE o.order_status='canceled')
                      ::NUMERIC / NULLIF(COUNT(*),0)*100, 2)             AS cancel_rate,
                ROUND(AVG(r.review_score)::NUMERIC, 2)                   AS avg_score
            FROM fact_orders o
            LEFT JOIN fact_payments p ON o.order_id = p.order_id
            LEFT JOIN fact_reviews r  ON o.order_id = r.order_id
        """).iloc[0]

        # Top 5 categorías
        top_cats = q("""
            SELECT p.category_name_en, ROUND(SUM(oi.price)::NUMERIC,0) AS revenue
            FROM fact_order_items oi
            JOIN dim_products p ON oi.product_id = p.product_id
            GROUP BY p.category_name_en
            ORDER BY revenue DESC LIMIT 5
        """)

        # Mes con más pedidos
        peak = q("""
            SELECT order_yearmonth, COUNT(*) AS orders
            FROM fact_orders WHERE order_yearmonth IS NOT NULL
            GROUP BY order_yearmonth ORDER BY orders DESC LIMIT 1
        """).iloc[0]

        # Construir HTML del reporte
        top_cats_rows = "".join(
            f"<tr><td>{row['category_name_en']}</td><td><b>R$ {int(row['revenue']):,}</b></td></tr>"
            for _, row in top_cats.iterrows()
        )

        html = f"""
        <html><body style="font-family:sans-serif;max-width:600px;margin:auto;color:#1a1a1a">
        <div style="background:#0f0f0f;padding:24px 32px;border-radius:12px;margin-bottom:24px">
          <h1 style="color:#fff;margin:0;font-size:1.6rem">📦 Olist Pipeline Report</h1>
          <p style="color:#888;margin:6px 0 0">{datetime.now().strftime('%Y-%m-%d %H:%M')} · Ejecución automática</p>
        </div>

        <h2>KPIs del dataset</h2>
        <table style="width:100%;border-collapse:collapse">
          <tr style="background:#f8f7f4"><td style="padding:10px">Total pedidos</td>
              <td style="padding:10px"><b>{int(kpi['total_orders'] or 0):,}</b></td></tr>
          <tr><td style="padding:10px">Revenue total</td>
              <td style="padding:10px"><b>R$ {float(kpi['total_revenue'] or 0):,.0f}</b></td></tr>
          <tr style="background:#f8f7f4"><td style="padding:10px">Entrega promedio</td>
              <td style="padding:10px"><b>{float(kpi['avg_delivery_days'] or 0):.1f} días</b></td></tr>
          <tr><td style="padding:10px">Tasa de cancelación</td>
              <td style="padding:10px"><b>{float(kpi['cancel_rate'] or 0):.2f}%</b></td></tr>
          <tr style="background:#f8f7f4"><td style="padding:10px">Score promedio</td>
              <td style="padding:10px"><b>⭐ {float(kpi['avg_score'] or 0):.2f}</b></td></tr>
        </table>

        <h2>Top 5 categorías por revenue</h2>
        <table style="width:100%;border-collapse:collapse">
          <tr style="background:#0f0f0f;color:#fff">
            <th style="padding:10px;text-align:left">Categoría</th>
            <th style="padding:10px;text-align:left">Revenue</th>
          </tr>
          {top_cats_rows}
        </table>

        <h2>Estacionalidad</h2>
        <p>📈 Mes pico: <b>{peak['order_yearmonth']}</b> con <b>{int(peak['orders']):,}</b> pedidos</p>

        <hr style="border:none;border-top:1px solid #eee;margin:32px 0">
        <p style="color:#aaa;font-size:0.8rem">
          Generado automáticamente por el pipeline n8n · Olist Data Engineer Intern
        </p>
        </body></html>
        """

        # Guardar HTML localmente
        report_path = os.path.join(BASE_DIR, "pipeline", "last_report.html")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html)

        report_data = {
            "total_orders":     int(kpi["total_orders"] or 0),
            "total_revenue":    float(kpi["total_revenue"] or 0),
            "avg_delivery_days":float(kpi["avg_delivery_days"] or 0),
            "cancel_rate":      float(kpi["cancel_rate"] or 0),
            "avg_score":        float(kpi["avg_score"] or 0),
            "peak_month":       str(peak["order_yearmonth"]),
            "peak_orders":      int(peak["orders"]),
            "top_categories":   top_cats.to_dict(orient="records"),
            "html_report":      html,
            "report_path":      report_path,
            "elapsed_s":        elapsed(t0),
        }

        return result("report", "ok",
                      f"Reporte generado en {elapsed(t0)}s",
                      report_data)

    except Exception as e:
        return result("report", "error", str(e),
                      {"traceback": traceback.format_exc()})


# =========================================================
#  MAIN
# =========================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=["clean", "load", "report", "all"],
                        default="all")
    args = parser.parse_args()

    if args.step == "clean":
        step_clean()
    elif args.step == "load":
        step_load()
    elif args.step == "report":
        step_report()
    elif args.step == "all":
        r1 = step_clean()
        if r1["status"] == "error":
            sys.exit(1)
        r2 = step_load()
        if r2["status"] == "error":
            sys.exit(1)
        step_report()

if __name__ == "__main__":
    main()
