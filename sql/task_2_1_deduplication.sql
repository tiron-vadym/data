-- Dialect: PostgreSQL
-- Task 2.1: De-duplication
--
-- One row per event_id. Latest ingested_at wins so a later revenue
-- correction replaces the original delivery. Test traffic is excluded.
-- Tie-break: parsed revenue_usd DESC, then full payload,
-- so equal ingested_at values still produce a deterministic winner.

WITH ranked_by_ingest AS (
    SELECT
        event_id,
        user_id,
        app_id,
        event_name,
        event_time,
        ingested_at,
        country,
        media_source,
        campaign,
        revenue_usd,
        is_test,
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
                    COALESCE(user_id::text, ''), COALESCE(app_id::text, ''),
                    COALESCE(event_name::text, ''), COALESCE(event_time::text, ''),
                    COALESCE(ingested_at::text, ''), COALESCE(country::text, ''),
                    COALESCE(media_source::text, ''), COALESCE(campaign::text, ''),
                    COALESCE(revenue_usd::text, ''), COALESCE(is_test::text, '')
                ) ASC
        ) AS rn
    FROM events_raw
    WHERE LOWER(TRIM(is_test::text)) NOT IN ('true', 't', '1', 'yes')
)

SELECT
    event_id,
    user_id,
    app_id,
    event_name,
    event_time,
    ingested_at,
    country,
    media_source,
    campaign,
    revenue_usd,
    is_test
FROM ranked_by_ingest
WHERE rn = 1;
