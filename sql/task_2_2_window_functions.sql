-- Dialect: PostgreSQL
-- Task 2.2: Window functions
--
-- Daily revenue per app, running total over observed days, 7-row moving
-- average, and day-over-day percent change. Days with no events are
-- absent; that makes the moving average a 7-observation window, not
-- necessarily 7 calendar days (see ANSWERS.md).

WITH ranked_by_ingest AS (
    SELECT
        app_id,
        event_time,
        revenue_usd,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY
                ingested_at DESC,
                CAST(
                    NULLIF(REPLACE(revenue_usd::text, ',', '.'), '')
                    AS DECIMAL(12, 2)
                ) DESC NULLS LAST,
                CONCAT_WS(
                    CHR(31),
                    COALESCE(app_id::text, ''), COALESCE(event_time::text, ''),
                    COALESCE(ingested_at::text, ''), COALESCE(revenue_usd::text, '')
                ) ASC
        ) AS rn
    FROM events_raw
    WHERE LOWER(TRIM(is_test::text)) NOT IN ('true', 't', '1', 'yes')
),

deduplicated_events AS (
    SELECT
        app_id,
        event_time,
        revenue_usd
    FROM ranked_by_ingest
    WHERE rn = 1
),

daily_revenue AS (
    SELECT
        app_id,
        CAST(event_time AS DATE) AS event_date,
        SUM(
            COALESCE(
                CAST(
                    NULLIF(REPLACE(revenue_usd::text, ',', '.'), '')
                    AS DECIMAL(12, 2)
                ),
                0
            )
        ) AS daily_revenue
    FROM deduplicated_events
    GROUP BY
        app_id,
        CAST(event_time AS DATE)
),

daily_with_windows AS (
    SELECT
        dr.app_id,
        dr.event_date,
        dr.daily_revenue,
        SUM(dr.daily_revenue) OVER (
            PARTITION BY dr.app_id
            ORDER BY dr.event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_revenue,
        AVG(dr.daily_revenue) OVER (
            PARTITION BY dr.app_id
            ORDER BY dr.event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg_7d,
        LAG(dr.daily_revenue) OVER (
            PARTITION BY dr.app_id
            ORDER BY dr.event_date
        ) AS previous_day_revenue
    FROM daily_revenue dr
)

SELECT
    app_id,
    event_date,
    daily_revenue,
    running_revenue,
    moving_avg_7d,
    CASE
        WHEN previous_day_revenue IS NULL
             OR previous_day_revenue = 0
            THEN NULL
        ELSE
            ((daily_revenue - previous_day_revenue) / previous_day_revenue) * 100
    END AS revenue_change_pct
FROM daily_with_windows
ORDER BY
    app_id,
    event_date;
