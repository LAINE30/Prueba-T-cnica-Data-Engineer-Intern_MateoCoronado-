-- =============================================================
--  Olist — Queries Analíticas
--  Responden Q1 a Q5 del dashboard
-- =============================================================


-- -------------------------------------------------------------
--  Q1: Volumen de órdenes por mes — ¿hay estacionalidad?
-- -------------------------------------------------------------
SELECT
    order_yearmonth,
    COUNT(*)                                      AS total_orders,
    COUNT(*) FILTER (WHERE is_delivered = 1)      AS delivered_orders,
    ROUND(
        COUNT(*) FILTER (WHERE is_delivered = 1)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                             AS delivery_rate_pct
FROM fact_orders
WHERE order_yearmonth IS NOT NULL
GROUP BY order_yearmonth
ORDER BY order_yearmonth;


-- -------------------------------------------------------------
--  Q2: Top 10 categorías por revenue total
-- -------------------------------------------------------------
SELECT
    p.category_name_en,
    COUNT(DISTINCT oi.order_id)          AS total_orders,
    SUM(oi.price)                        AS total_revenue,
    ROUND(AVG(oi.price), 2)              AS avg_price,
    COUNT(*)                             AS items_sold
FROM fact_order_items oi
JOIN dim_products p ON oi.product_id = p.product_id
WHERE p.category_name_en IS NOT NULL
GROUP BY p.category_name_en
ORDER BY total_revenue DESC
LIMIT 10;


-- -------------------------------------------------------------
--  Q3: Tiempo promedio compra → entrega por estado del cliente
-- -------------------------------------------------------------
SELECT
    c.state                                   AS customer_state,
    COUNT(*)                                  AS delivered_orders,
    ROUND(AVG(o.delivery_time_days), 1)       AS avg_delivery_days,
    ROUND(PERCENTILE_CONT(0.5)
          WITHIN GROUP (ORDER BY o.delivery_time_days)::NUMERIC, 1)
                                              AS median_delivery_days,
    ROUND(AVG(o.delay_days), 1)               AS avg_delay_days,
    COUNT(*) FILTER (WHERE o.delay_days > 0)  AS late_deliveries,
    ROUND(
        COUNT(*) FILTER (WHERE o.delay_days > 0)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                         AS late_pct
FROM fact_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
WHERE o.is_delivered = 1
  AND o.delivery_time_days IS NOT NULL
GROUP BY c.state
ORDER BY avg_delivery_days DESC;


-- -------------------------------------------------------------
--  Q4: % de órdenes con incidencias
--  (canceladas, reseñas negativas, entrega tardía)
-- -------------------------------------------------------------
WITH order_flags AS (
    SELECT
        o.order_id,
        CASE WHEN o.order_status = 'canceled'     THEN 1 ELSE 0 END  AS is_canceled,
        CASE WHEN o.delay_days > 0                THEN 1 ELSE 0 END  AS is_late,
        COALESCE(r.is_negative, 0)                                    AS is_negative_review,
        CASE
            WHEN o.order_status = 'canceled'
              OR o.delay_days > 0
              OR r.is_negative = 1
            THEN 1 ELSE 0
        END                                                           AS has_any_incident
    FROM fact_orders o
    LEFT JOIN fact_reviews r ON o.order_id = r.order_id
)
SELECT
    COUNT(*)                                               AS total_orders,
    SUM(is_canceled)                                       AS canceled,
    SUM(is_late)                                           AS late_deliveries,
    SUM(is_negative_review)                                AS negative_reviews,
    SUM(has_any_incident)                                  AS with_any_incident,
    ROUND(SUM(is_canceled)::NUMERIC        / COUNT(*) * 100, 2) AS canceled_pct,
    ROUND(SUM(is_late)::NUMERIC            / COUNT(*) * 100, 2) AS late_pct,
    ROUND(SUM(is_negative_review)::NUMERIC / COUNT(*) * 100, 2) AS negative_review_pct,
    ROUND(SUM(has_any_incident)::NUMERIC   / COUNT(*) * 100, 2) AS incident_pct
FROM order_flags;


-- -------------------------------------------------------------
--  Q5 (libre): ¿Qué vendedores tienen mejor ratio calidad/volumen?
--  Vendedores con al menos 50 órdenes — score promedio y % tardíos
-- -------------------------------------------------------------
SELECT
    oi.seller_id,
    s.city                                         AS seller_city,
    s.state                                        AS seller_state,
    COUNT(DISTINCT oi.order_id)                    AS total_orders,
    ROUND(SUM(oi.price), 2)                        AS total_revenue,
    ROUND(AVG(r.review_score), 2)                  AS avg_review_score,
    COUNT(*) FILTER (WHERE r.is_negative = 1)      AS negative_reviews,
    ROUND(
        COUNT(*) FILTER (WHERE r.is_negative = 1)::NUMERIC
        / NULLIF(COUNT(DISTINCT oi.order_id), 0) * 100, 2
    )                                              AS negative_pct,
    ROUND(AVG(o.delivery_time_days), 1)            AS avg_delivery_days,
    COUNT(*) FILTER (WHERE o.delay_days > 0)       AS late_orders,
    ROUND(
        COUNT(*) FILTER (WHERE o.delay_days > 0)::NUMERIC
        / NULLIF(COUNT(DISTINCT oi.order_id), 0) * 100, 2
    )                                              AS late_pct
FROM fact_order_items oi
JOIN fact_orders  o  ON oi.order_id  = o.order_id
JOIN dim_sellers  s  ON oi.seller_id = s.seller_id
LEFT JOIN fact_reviews r ON o.order_id = r.order_id
WHERE o.is_delivered = 1
GROUP BY oi.seller_id, s.city, s.state
HAVING COUNT(DISTINCT oi.order_id) >= 50
ORDER BY avg_review_score DESC, total_revenue DESC
LIMIT 20;
