# Olist E-Commerce — Data Pipeline

**Prueba Técnica · Mateo Coronado**  
Dataset: [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## Tabla de contenidos

- [Descripción del proyecto](#descripción-del-proyecto)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Stack tecnológico](#stack-tecnológico)
- [Instalación y setup](#instalación-y-setup)
- [Fases del proyecto](#fases-del-proyecto)
  - [Fase 1 — Exploración](#fase-1--exploración-y-diagnóstico)
  - [Fase 2 — Limpieza](#fase-2--limpieza-y-transformación)
  - [Fase 3 — Modelado y carga](#fase-3--modelado-y-carga-en-postgresql)
  - [Fase 4 — Dashboard](#fase-4--dashboard-streamlit)
  - [Fase 5 — Pipeline n8n](#fase-5--pipeline-orquestado-con-n8n)
- [Preguntas de negocio respondidas](#preguntas-de-negocio-respondidas)
- [Decisiones técnicas](#decisiones-técnicas)

---

## Descripción del proyecto

**Objetivo:** demostrar la capacidad de trabajar con datos reales y sucios, diseñar estructuras de bases de datos, construir pipelines de transformación y comunicar hallazgos de negocio.

---

## Estructura del repositorio

```
olist-data-pipeline/
│
├── data/
│   ├── raw/                          # CSVs originales 
│   └── clean/                        # CSVs procesados 
│
├── notebooks/
│   ├── 01_exploration.ipynb          # Fase 1: diagnóstico de calidad de datos
│   └── 02_cleaning.ipynb             # Fase 2: transformaciones
│
├── sql/
│   ├── schema.sql                    # DDL completo del star schema
│   └── queries.sql                   # Queries analíticas Q1–Q5
│
├── scripts/
│   └── load_to_postgres.py           # Carga de CSVs limpios a PostgreSQL
│
├── dashboard/
│   └── dashboard.py                  # Dashboard Streamlit
│
├── pipeline/
│   ├── pipeline_runner.py            # Orquestador Python invocado por n8n
│   └── last_report.html              # Último reporte generado (automático)
│
├── docker-compose.yml                # Levanta n8n en Docker
├── olist_n8n_workflow.json           # Workflow n8n importable
├── requirements.txt
└── README.md
```


## Instalación y setup

### 1. Clonar el repositorio

```bash
git clone https://github.com/LAINE30/Prueba-T-cnica-Data-Engineer-Intern_MateoCoronado-.git
```

### 2. Crear entorno virtual e instalar dependencias

```bash
python -m venv venv

.\venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

### 3. Descargar el dataset

Descargar desde [Kaggle · Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) y colocar los 9 CSVs en `data/raw/`.

### 4. Configurar PostgreSQL

El servidor PostgreSQL puede estar en otra máquina de la misma red. Editar las credenciales en `scripts/load_to_postgres.py`:

```python
DB_CONFIG = {
    "host":     "localhost",   #Si el servidor esta en otra PC, IP del servidor: 192.168.1.26
    "port":     "5432",
    "database": "olist_db",
    "user":     "postgres",
    "password": "tu_password",
}
```

O usar variables de entorno:

```bash
export PGHOST=192.168.x.x
export PGDATABASE=olist_db
export PGUSER=postgres
export PGPASSWORD=tu_password
```

---

## Fases del proyecto

### Fase 1 — Exploración y Diagnóstico

**Notebook:** `notebooks/01_exploration.ipynb`

Exploración inicial de las 8 tablas antes de cualquier transformación. Principales hallazgos:

| Tabla | Problema encontrado |
|---|---|---|
| `geolocation` | 57 coordenadas fuera del territorio de Brasil |
| `geolocation` | 981k filas duplicadas por ZIP code (98.1%) |
| `orders` | Nulos esperados en fechas de entrega (pedidos no entregados) |
| `products` | 610 productos sin categoría (1.85%) |
| `payments` | 9 registros con `payment_value == 0` |
| `reviews` | 551 duplicados por `order_id` |
| `customers` / `sellers` | Inconsistencias de encoding en nombres de ciudades |


---

### Fase 2 — Limpieza y Transformación

**Notebook:** `notebooks/02_cleaning.ipynb`

Cada decisión está justificada en el notebook. Resumen:

| Problema | Decisión | Justificación |
|---|---|---|
| Coords fuera de Brasil | Filtrar por bounding box | Imposibles geográficamente |
| Duplicados por ZIP | Agregar con mediana lat/lng | 1 coord representativa por ZIP |
| Encoding en ciudades | `normalize_text()` con `unicodedata` | Consistencia en joins y filtros |
| Productos sin categoría | Imputar `'unknown'` | Sin base para inferir categoría |
| Dimensiones = 0 | Tratar como nulo + mediana por categoría | Imposibles físicamente |
| Categorías en portugués | Traducir al inglés vía `category_translation.csv` | Legibilidad en dashboard |
| Nulos en fechas de entrega | **Mantener** | Son válidos: pedidos cancelados/pendientes |
| 9 pagos con valor 0 | Eliminar | No representan transacción real |
| 551 reseñas duplicadas por orden | Conservar la más reciente | La reseña actualizada es la vigente |

**Columnas derivadas creadas:**

- `delivery_time_days` — días entre compra y entrega
- `delay_days` — días de retraso respecto al estimado (positivo = tarde)
- `is_delivered` — flag binario
- `order_yearmonth` — periodo para análisis de estacionalidad
- `total_item_value` — precio + flete por ítem
- `is_negative` — flag de reseña negativa (score ≤ 2)

---

### Fase 3 — Modelado y Carga en PostgreSQL

**Archivos:** `sql/schema.sql`, `scripts/load_to_postgres.py`

#### Modelo de datos — Star Schema

```
                    dim_geolocation
                         ↑
         ┌───────────────┴───────────────┐
    dim_customers                   dim_sellers
         ↑                               ↑
         └──────────── fact_orders ───────┘
                            ↑
              ┌─────────────┼─────────────┐
       fact_order_items  fact_payments  fact_reviews
              ↑
         dim_products
```

#### Ejecutar la carga

```bash
# 1. Correr notebooks 01 y 02 para generar data/clean/
01_exploration.ipynb 
02_cleaning.ipynb

# 2. Cargar a PostgreSQL
python scripts/load_to_postgres.py
```

**Resultado esperado:**

```
dim_geolocation          19,011
dim_customers            99,441
dim_sellers               3,095
dim_products             32,951
fact_orders              99,441
fact_order_items        112,650
fact_payments           103,877
fact_reviews             98,095
─────────────────────────────────
Total                   569,561
```

---

### Fase 4 — Dashboard Streamlit

**Archivo:** `dashboard/dashboard.py`

```bash
streamlit run dashboard/dashboard.py
```

El dashboard se abre en `http://localhost:8501` o `http://192.168.1.26:8501`,dependiendo de la configuración  y se conecta directamente a PostgreSQL.

**Visualizaciones incluidas:**

| # | Pregunta | Visualización |
|---|---|---|
| KPIs | Resumen global | 6 métricas: pedidos, revenue, compradores, entrega, cancelación, score |
| Q1 | ¿Hay estacionalidad? | Línea de pedidos por mes + barras de revenue mensual |
| Q2 | Top categorías por valor | Barras horizontales + scatter precio vs. volumen |
| Q3 | Tiempo de entrega | Barras por estado + scatter días vs. % tardíos |
| Q4 | % de incidencias | Donut de incidencias + distribución de review scores |
| Q5 | Calidad de vendedores | Scatter calidad/velocidad + tabla top 10 |

---


---

## Preguntas de negocio respondidas

**Q1 — ¿Cuál es el volumen total por mes? ¿Hay estacionalidad?**  
Sí. El pico de ventas ocurre en **noviembre 2017** (Black Friday), con un crecimiento sostenido desde mediados de 2017. Diciembre muestra una caída post-pico típica del retail.

**Q2 — Top 10 categorías con mayor valor generado**  
Las categorías de mayor revenue son `health_beauty`, `watches_gifts` y `bed_bath_table`. Los productos de `computers` tienen el ticket promedio más alto pero menor volumen.

**Q3 — Tiempo promedio entre compra y entrega**  
La mediana nacional es ~12 días. Los estados del norte y noreste (AM, RR, AP) tienen tiempos 2–3× mayores que São Paulo (SP), evidenciando un problema estructural de logística regional.

**Q4 — % de registros con incidencias**  
~11% de los pedidos tienen alguna incidencia: 0.6% cancelados, ~8% con entrega tardía, ~11% con reseña negativa (score ≤ 2). La mayor fuente de insatisfacción es el tiempo de entrega, no el producto.

**Q5 — ¿Qué vendedores tienen mejor ratio calidad/volumen?**  
Los vendedores con mejor score promedio (≥4.5) y bajo % de tardíos (<10%) se concentran en SP y PR. Existe una correlación negativa clara entre días de entrega y score de reseña.

---

## Decisiones técnicas

**¿Por qué star schema y no 3NF?**  
El caso de uso es analítico (OLAP), no transaccional (OLTP). El star schema permite queries de agregación más simples y rápidas, a costa de algo de redundancia controlada.

**¿Por qué mediana y no media en geolocation?**  
La mediana es más robusta ante outliers. Un ZIP code con pocas coordenadas erróneas no distorsiona la ubicación representativa.

**¿Por qué mantener los nulos en fechas de entrega?**  
Son informativos: un nulo en `delivered_customer_date` indica que el pedido no fue entregado (cancelado, perdido, en tránsito). Eliminarlos sesgaría las métricas de tiempo de entrega hacia los casos exitosos.

**¿Por qué n8n y no Airflow?**  
Para el volumen y frecuencia de este pipeline (semanal, ~570k filas), n8n es más ligero, no requiere infraestructura adicional, y permite iterar el flujo visualmente. Airflow sería la elección para pipelines con decenas de DAGs interdependientes o procesamiento en tiempo real.

---

*Mateo Coronado · Prueba Técnica Data Engineer Intern · Invers AI*