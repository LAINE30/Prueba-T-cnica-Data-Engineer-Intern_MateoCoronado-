"""
load_to_postgres.py
--------------------
Carga los CSVs limpios de Olist a PostgreSQL.

Uso:
    python scripts/load_to_postgres.py

Edita DB_CONFIG abajo o usa variables de entorno:
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""

import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# =============================================================
#  RUTAS - absolutas desde la raiz del proyecto
# =============================================================
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEAN_DIR   = os.path.join(BASE_DIR, "data", "clean")
SCHEMA_FILE = os.path.join(BASE_DIR, "sql", "schema.sql")

# =============================================================
#  CONFIGURACION DE BASE DE DATOS
# =============================================================
DB_CONFIG = {
    "host":     os.getenv("PGHOST",     "192.168.1.26"),
    "port":     os.getenv("PGPORT",     "5432"),
    "database": os.getenv("PGDATABASE", "olist_db"),
    "user":     os.getenv("PGUSER",     "postgres"),
    "password": os.getenv("PGPASSWORD", "admin"),
}

# Orden de carga: dimensiones PRIMERO (fact tables tienen FK hacia ellas)
LOAD_ORDER = [
    ("geolocation_clean.csv",     "dim_geolocation",  5_000),
    ("customers_clean.csv",       "dim_customers",    10_000),
    ("sellers_clean.csv",         "dim_sellers",      5_000),
    ("products_clean.csv",        "dim_products",     5_000),
    ("orders_clean.csv",          "fact_orders",      10_000),
    ("order_items_clean.csv",     "fact_order_items", 20_000),
    ("payments_clean.csv",        "fact_payments",    10_000),
    ("reviews_clean.csv",         "fact_reviews",     10_000),
]

COLUMN_RENAME = {
    "dim_geolocation": {
        "geolocation_zip_code_prefix": "zip_code_prefix",
        "geolocation_lat":             "lat",
        "geolocation_lng":             "lng",
        "geolocation_city":            "city",
        "geolocation_state":           "state",
    },
    "dim_customers": {
        "customer_zip_code_prefix": "zip_code_prefix",
    },
    "dim_sellers": {
        "seller_zip_code_prefix": "zip_code_prefix",
        "seller_city":            "city",
        "seller_state":           "state",
    },
    "dim_products": {
        "product_category_name":         "category_name_pt",
        "product_category_name_english": "category_name_en",
        "product_name_lenght":           "name_length",
        "product_description_lenght":    "description_length",
        "product_photos_qty":            "photos_qty",
        "product_weight_g":              "weight_g",
        "product_length_cm":             "length_cm",
        "product_height_cm":             "height_cm",
        "product_width_cm":              "width_cm",
    },
    "fact_orders": {
        "order_purchase_timestamp":      "purchase_timestamp",
        "order_approved_at":             "approved_at",
        "order_delivered_carrier_date":  "delivered_carrier_date",
        "order_delivered_customer_date": "delivered_customer_date",
        "order_estimated_delivery_date": "estimated_delivery_date",
    },
}

COLUMNS_KEEP = {
    "dim_geolocation":  ["zip_code_prefix", "city", "state", "lat", "lng"],
    "dim_customers":    ["customer_id", "customer_unique_id", "zip_code_prefix", "city", "state"],
    "dim_sellers":      ["seller_id", "zip_code_prefix", "city", "state"],
    "dim_products":     ["product_id", "category_name_pt", "category_name_en",
                         "name_length", "description_length", "photos_qty",
                         "weight_g", "length_cm", "height_cm", "width_cm"],
    "fact_orders":      ["order_id", "customer_id", "order_status", "is_delivered", "has_items",
                         "purchase_timestamp", "approved_at", "delivered_carrier_date",
                         "delivered_customer_date", "estimated_delivery_date",
                         "delivery_time_days", "delay_days",
                         "order_year", "order_month", "order_yearmonth"],
    "fact_order_items": ["order_id", "order_item_id", "product_id", "seller_id",
                         "shipping_limit_date", "price", "freight_value", "total_item_value"],
    "fact_payments":    ["order_id", "payment_sequential", "payment_type",
                         "payment_installments", "payment_value"],
    "fact_reviews":     ["review_id", "order_id", "review_score", "is_negative", "has_comment",
                         "review_comment_title", "review_comment_message",
                         "review_creation_date", "review_answer_timestamp"],
}


# =============================================================
#  HELPERS
# =============================================================

def get_engine():
    from urllib.parse import quote_plus
    password = quote_plus(DB_CONFIG["password"])
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{password}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(url, pool_pre_ping=True)


def run_schema(engine, schema_file):
    """Ejecuta el DDL completo (DROP + CREATE de todas las tablas)."""
    print(f"\n[schema] {schema_file}")
    with open(schema_file, "r", encoding="utf-8") as f:
        ddl = f.read()
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("   OK  Schema recreado")


def set_fk_checks(engine, enabled: bool):
    """Habilita o deshabilita triggers (FK) en todas las tablas del proyecto."""
    tables = [
        "dim_geolocation", "dim_customers", "dim_sellers", "dim_products",
        "fact_orders", "fact_order_items", "fact_payments", "fact_reviews",
    ]
    action = "ENABLE" if enabled else "DISABLE"
    with engine.begin() as conn:
        for table in tables:
            conn.execute(text(f'ALTER TABLE "{table}" {action} TRIGGER ALL'))
    state = "habilitados" if enabled else "deshabilitados"
    print(f"   FK triggers {state}")


def generate_products_csv(clean_dir):
    """
    Si products_clean.csv no existe, lo genera desde el CSV raw.
    Esto cubre el caso en que el notebook no lo exporto correctamente.
    """
    out_path = os.path.join(clean_dir, "products_clean.csv")
    if os.path.exists(out_path):
        return  # ya existe, nada que hacer

    print("\n[aviso] products_clean.csv no encontrado — generando desde raw...")
    raw_path = os.path.join(
        os.path.dirname(clean_dir), "raw", "olist_products_dataset.csv"
    )
    trans_path = os.path.join(
        os.path.dirname(clean_dir), "raw", "product_category_name_translation.csv"
    )
    if not os.path.exists(raw_path):
        print("   ERROR: tampoco existe el CSV raw. Exporta products_clean desde el notebook.")
        return

    import numpy as np
    products = pd.read_csv(raw_path)

    # Traduccion de categorias
    if os.path.exists(trans_path):
        trans = pd.read_csv(trans_path)
        tmap = dict(zip(trans["product_category_name"], trans["product_category_name_english"]))
        products["product_category_name_english"] = (
            products["product_category_name"].map(tmap).fillna(products["product_category_name"])
        )
    else:
        products["product_category_name_english"] = products["product_category_name"]

    products["product_category_name"] = products["product_category_name"].fillna("unknown")
    products["product_category_name_english"] = products["product_category_name_english"].fillna("unknown")

    # Dimensiones: ceros -> NaN -> imputar mediana por categoria
    dim_cols = ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    for col in dim_cols:
        products[col] = products[col].replace(0, np.nan)
        products[col] = products[col].fillna(
            products.groupby("product_category_name")[col].transform("median")
        ).fillna(products[col].median())

    products.to_csv(out_path, index=False)
    print(f"   OK  products_clean.csv generado ({len(products):,} filas)")


def _psycopg2_execute_values(table, conn, keys, data_iter):
    """
    Insercion via psycopg2 execute_values.
    - Sin limite de parametros
    - Mas rapido que method='multi' para datasets grandes
    """
    from psycopg2.extras import execute_values
    raw_conn = conn.connection
    cols = ", ".join(f'"{k}"' for k in keys)
    sql = f'INSERT INTO "{table.name}" ({cols}) VALUES %s ON CONFLICT DO NOTHING'
    execute_values(raw_conn.cursor(), sql, list(data_iter), page_size=500)


def load_table(engine, csv_path, table_name, chunksize):
    """Lee el CSV, transforma columnas y carga en PostgreSQL por chunks."""
    print(f"\n[load] {table_name}")
    t0 = time.time()

    df = pd.read_csv(csv_path, low_memory=False)

    rename_map = COLUMN_RENAME.get(table_name, {})
    if rename_map:
        df = df.rename(columns=rename_map)

    keep = COLUMNS_KEEP.get(table_name)
    if keep:
        keep = [c for c in keep if c in df.columns]
        df = df[keep]

    # NaN -> None para que PostgreSQL reciba NULL
    df = df.where(pd.notnull(df), None)

    total_rows = len(df)
    loaded = 0

    for chunk in _chunks(df, chunksize):
        chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
            method=_psycopg2_execute_values,
        )
        loaded += len(chunk)
        pct = loaded / total_rows * 100
        print(f"   {loaded:>8,} / {total_rows:,}  ({pct:.1f}%)", end="\r")

    elapsed = time.time() - t0
    print(f"   OK  {total_rows:,} filas en {elapsed:.1f}s                    ")
    return total_rows


def _chunks(df, size):
    for i in range(0, len(df), size):
        yield df.iloc[i : i + size]


def verify_counts(engine):
    tables = [t for _, t, _ in LOAD_ORDER]
    print("\n[verificacion] Filas por tabla:")
    print(f"   {'Tabla':<25} {'Filas':>10}")
    print("   " + "-" * 37)
    with engine.connect() as conn:
        for table in tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"   {table:<25} {count:>10,}")


# =============================================================
#  MAIN
# =============================================================

def main():
    print("=" * 60)
    print("  Olist -> PostgreSQL Loader")
    print("=" * 60)

    engine = get_engine()

    # Test de conexion
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"\nConexion OK: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    except SQLAlchemyError as e:
        print(f"\nERROR de conexion: {e}")
        return

    # 1. Recrear schema (DROP + CREATE — limpia cualquier carga previa fallida)
    try:
        run_schema(engine, SCHEMA_FILE)
    except SQLAlchemyError as e:
        print(f"\nERROR al crear schema: {e}")
        return

    # 2. Generar products_clean.csv si no existe
    generate_products_csv(CLEAN_DIR)

    # 3. Cargar tablas (FK deshabilitadas para evitar errores de orden)
    set_fk_checks(engine, enabled=False)
    total_loaded = 0
    errors = []

    for csv_file, table_name, chunksize in LOAD_ORDER:
        csv_path = os.path.join(CLEAN_DIR, csv_file)

        if not os.path.exists(csv_path):
            msg = f"Archivo no encontrado: {csv_path}"
            print(f"\nADVERTENCIA  {msg}")
            errors.append(msg)
            continue

        try:
            rows = load_table(engine, csv_path, table_name, chunksize)
            total_loaded += rows
        except Exception as e:
            msg = f"{table_name}: {e}"
            print(f"\nERROR: {msg}")
            errors.append(msg)

    # 4. Re-habilitar FK
    set_fk_checks(engine, enabled=True)

    # 5. Verificacion final
    verify_counts(engine)

    # 6. Resumen
    print("\n" + "=" * 60)
    print(f"  Total filas insertadas: {total_loaded:,}")
    if errors:
        print(f"  ERRORES ({len(errors)}):")
        for err in errors:
            print(f"     - {err}")
    else:
        print("  Carga completada sin errores")
    print("=" * 60)


if __name__ == "__main__":
    main()