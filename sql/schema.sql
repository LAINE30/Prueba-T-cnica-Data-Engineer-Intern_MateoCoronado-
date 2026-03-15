-- =============================================================
--  Olist E-Commerce — Schema PostgreSQL
--  Modelo: Star Schema
--  Fase 3 — Modelado y Carga en Base de Datos
-- =============================================================

-- -------------------------------------------------------------
--  Limpieza previa (útil para re-runs en desarrollo)
-- -------------------------------------------------------------
DROP TABLE IF EXISTS fact_order_items  CASCADE;
DROP TABLE IF EXISTS fact_payments     CASCADE;
DROP TABLE IF EXISTS fact_reviews      CASCADE;
DROP TABLE IF EXISTS fact_orders       CASCADE;
DROP TABLE IF EXISTS dim_customers     CASCADE;
DROP TABLE IF EXISTS dim_sellers       CASCADE;
DROP TABLE IF EXISTS dim_products      CASCADE;
DROP TABLE IF EXISTS dim_geolocation   CASCADE;


-- =============================================================
--  DIMENSIONES
-- =============================================================

CREATE TABLE dim_geolocation (
    zip_code_prefix   INTEGER       PRIMARY KEY,
    city              VARCHAR(100),
    state             CHAR(2)       NOT NULL,
    lat               NUMERIC(9,6),
    lng               NUMERIC(9,6)
);

COMMENT ON TABLE  dim_geolocation             IS 'Una fila por código postal brasileño, coordenadas agregadas por mediana.';
COMMENT ON COLUMN dim_geolocation.lat         IS 'Latitud mediana del ZIP code.';
COMMENT ON COLUMN dim_geolocation.lng         IS 'Longitud mediana del ZIP code.';


-- -------------------------------------------------------------
CREATE TABLE dim_customers (
    customer_id         VARCHAR(50)  PRIMARY KEY,
    customer_unique_id  VARCHAR(50)  NOT NULL,
    zip_code_prefix     INTEGER,
    city                VARCHAR(100),
    state               CHAR(2),

    CONSTRAINT fk_cust_geo
        FOREIGN KEY (zip_code_prefix)
        REFERENCES dim_geolocation (zip_code_prefix)
        ON DELETE SET NULL
);

COMMENT ON TABLE  dim_customers                    IS 'Un registro por customer_id (un comprador puede tener varios).';
COMMENT ON COLUMN dim_customers.customer_unique_id IS 'ID real del comprador — múltiples customer_id pueden mapear al mismo unique_id.';


-- -------------------------------------------------------------
CREATE TABLE dim_sellers (
    seller_id           VARCHAR(50)  PRIMARY KEY,
    zip_code_prefix     INTEGER,
    city                VARCHAR(100),
    state               CHAR(2),

    CONSTRAINT fk_sell_geo
        FOREIGN KEY (zip_code_prefix)
        REFERENCES dim_geolocation (zip_code_prefix)
        ON DELETE SET NULL
);


-- -------------------------------------------------------------
CREATE TABLE dim_products (
    product_id                      VARCHAR(50)   PRIMARY KEY,
    category_name_pt                VARCHAR(100),
    category_name_en                VARCHAR(100),
    name_length                     INTEGER,
    description_length              INTEGER,
    photos_qty                      INTEGER,
    weight_g                        NUMERIC(10,2),
    length_cm                       NUMERIC(8,2),
    height_cm                       NUMERIC(8,2),
    width_cm                        NUMERIC(8,2)
);

COMMENT ON COLUMN dim_products.category_name_pt IS 'Categoría original en portugués.';
COMMENT ON COLUMN dim_products.category_name_en IS 'Categoría traducida al inglés.';


-- =============================================================
--  HECHOS
-- =============================================================

CREATE TABLE fact_orders (
    order_id                        VARCHAR(50)   PRIMARY KEY,
    customer_id                     VARCHAR(50)   NOT NULL,

    -- Estado
    order_status                    VARCHAR(20)   NOT NULL,
    is_delivered                    SMALLINT      NOT NULL DEFAULT 0,
    has_items                       SMALLINT      NOT NULL DEFAULT 1,

    -- Timestamps
    purchase_timestamp              TIMESTAMP,
    approved_at                     TIMESTAMP,
    delivered_carrier_date          TIMESTAMP,
    delivered_customer_date         TIMESTAMP,
    estimated_delivery_date         TIMESTAMP,

    -- Métricas derivadas
    delivery_time_days              INTEGER,
    delay_days                      INTEGER,

    -- Dimensiones temporales (desnormalizadas para performance analítica)
    order_year                      SMALLINT,
    order_month                     SMALLINT,
    order_yearmonth                 CHAR(7),      -- 'YYYY-MM'

    CONSTRAINT fk_order_customer
        FOREIGN KEY (customer_id)
        REFERENCES dim_customers (customer_id)
);

CREATE INDEX idx_orders_yearmonth  ON fact_orders (order_yearmonth);
CREATE INDEX idx_orders_status     ON fact_orders (order_status);
CREATE INDEX idx_orders_customer   ON fact_orders (customer_id);

COMMENT ON TABLE  fact_orders                    IS 'Granularidad: 1 fila por pedido.';
COMMENT ON COLUMN fact_orders.is_delivered       IS '1 si order_status = delivered, 0 en otro caso.';
COMMENT ON COLUMN fact_orders.delivery_time_days IS 'Días entre purchase_timestamp y delivered_customer_date. NULL si no entregado.';
COMMENT ON COLUMN fact_orders.delay_days         IS 'Días respecto al estimado. Positivo = tarde, Negativo = adelantado.';


-- -------------------------------------------------------------
CREATE TABLE fact_order_items (
    order_id            VARCHAR(50)   NOT NULL,
    order_item_id       SMALLINT      NOT NULL,   -- secuencia dentro del pedido
    product_id          VARCHAR(50),
    seller_id           VARCHAR(50),

    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10,2) NOT NULL,
    freight_value       NUMERIC(10,2) NOT NULL DEFAULT 0,
    total_item_value    NUMERIC(10,2),

    PRIMARY KEY (order_id, order_item_id),

    CONSTRAINT fk_item_order
        FOREIGN KEY (order_id)
        REFERENCES fact_orders (order_id),
    CONSTRAINT fk_item_product
        FOREIGN KEY (product_id)
        REFERENCES dim_products (product_id)
        ON DELETE SET NULL,
    CONSTRAINT fk_item_seller
        FOREIGN KEY (seller_id)
        REFERENCES dim_sellers (seller_id)
        ON DELETE SET NULL
);

CREATE INDEX idx_items_product ON fact_order_items (product_id);
CREATE INDEX idx_items_seller  ON fact_order_items (seller_id);

COMMENT ON TABLE  fact_order_items               IS 'Granularidad: 1 fila por ítem dentro de un pedido.';
COMMENT ON COLUMN fact_order_items.order_item_id IS 'Número secuencial del ítem dentro del pedido (no es PK global).';
COMMENT ON COLUMN fact_order_items.freight_value IS 'Puede ser 0 (envío gratuito). No es un error.';


-- -------------------------------------------------------------
CREATE TABLE fact_payments (
    payment_id           SERIAL        PRIMARY KEY,  -- surrogate key
    order_id             VARCHAR(50)   NOT NULL,
    payment_sequential   SMALLINT,
    payment_type         VARCHAR(30)   NOT NULL,
    payment_installments SMALLINT,
    payment_value        NUMERIC(10,2) NOT NULL,

    CONSTRAINT fk_pay_order
        FOREIGN KEY (order_id)
        REFERENCES fact_orders (order_id)
);

CREATE INDEX idx_payments_order ON fact_payments (order_id);

COMMENT ON TABLE  fact_payments                      IS 'Granularidad: 1 fila por transacción de pago. Un pedido puede tener N pagos.';
COMMENT ON COLUMN fact_payments.payment_sequential   IS 'Orden del pago dentro del pedido cuando hay múltiples métodos.';


-- -------------------------------------------------------------
CREATE TABLE fact_reviews (
    review_id               VARCHAR(50)   PRIMARY KEY,
    order_id                VARCHAR(50)   NOT NULL,

    review_score            SMALLINT      NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    is_negative             SMALLINT      NOT NULL DEFAULT 0,
    has_comment             SMALLINT      NOT NULL DEFAULT 0,

    review_comment_title    TEXT,
    review_comment_message  TEXT,

    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,

    CONSTRAINT fk_review_order
        FOREIGN KEY (order_id)
        REFERENCES fact_orders (order_id)
);

CREATE INDEX idx_reviews_order ON fact_reviews (order_id);
CREATE INDEX idx_reviews_score ON fact_reviews (review_score);

COMMENT ON TABLE  fact_reviews              IS 'Granularidad: 1 reseña por pedido (deduplicada en limpieza).';
COMMENT ON COLUMN fact_reviews.is_negative  IS '1 si review_score <= 2.';
COMMENT ON COLUMN fact_reviews.has_comment  IS '1 si review_comment_message no está vacío.';
