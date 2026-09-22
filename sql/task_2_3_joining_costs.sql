-- Dialect: PostgreSQL
-- Task 2.3: Joining costs
--
-- Daily revenue and cost side by side at app / media_source / campaign.
-- FULL OUTER JOIN keeps cost-only and revenue-only campaigns.
-- ROAS is NULL when cost is zero (division by zero is undefined).

WITH ranked_by_ingest AS (
    SELECT
        event_time,
        app_id,
        media_source,
        campaign,
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
                    COALESCE(ingested_at::text, ''), COALESCE(media_source::text, ''),
                    COALESCE(campaign::text, ''), COALESCE(revenue_usd::text, '')
                ) ASC
        ) AS rn
    FROM events_raw
    WHERE LOWER(TRIM(is_test::text)) NOT IN ('true', 't', '1', 'yes')
),

deduplicated_events AS (
    SELECT
        event_time,
        app_id,
        media_source,
        campaign,
        revenue_usd
    FROM ranked_by_ingest
    WHERE rn = 1
),

daily_revenue AS (
    SELECT
        CAST(event_time AS DATE) AS date,
        app_id,
        media_source,
        campaign,
        SUM(
            COALESCE(
                CAST(
                    NULLIF(REPLACE(revenue_usd::text, ',', '.'), '')
                    AS DECIMAL(12, 2)
                ),
                0
            )
        ) AS revenue
    FROM deduplicated_events
    GROUP BY
        CAST(event_time AS DATE),
        app_id,
        media_source,
        campaign
),

daily_costs AS (
    SELECT
        date,
        app_id,
        media_source,
        campaign,
        SUM(cost_usd) AS cost
    FROM campaign_costs
    GROUP BY
        date,
        app_id,
        media_source,
        campaign
)

SELECT
    COALESCE(r.date, c.date) AS date,
    COALESCE(r.app_id, c.app_id) AS app_id,
    COALESCE(r.media_source, c.media_source) AS media_source,
    COALESCE(r.campaign, c.campaign) AS campaign,
    COALESCE(r.revenue, 0) AS revenue,
    COALESCE(c.cost, 0) AS cost,
    CASE
        WHEN COALESCE(c.cost, 0) = 0 THEN NULL
        ELSE COALESCE(r.revenue, 0) / c.cost
    END AS roas
FROM daily_revenue r
FULL OUTER JOIN daily_costs c
    ON r.date = c.date
    AND r.app_id = c.app_id
    AND r.media_source = c.media_source
    AND r.campaign = c.campaign
ORDER BY
    date,
    app_id,
    media_source,
    campaign;
