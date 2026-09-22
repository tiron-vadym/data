-- Dialect: PostgreSQL
-- Task 2.4: Incremental load
--
-- Loads new and corrected events from events_staging into events_clean.
-- Filter on ingested_at (not event_time): a correction keeps the original
-- event_time and only the delivery timestamp moves forward.
--
-- Watermark: max(ingested_at) already in events_clean, minus a replay
-- window so a row that landed just before the last high-water mark is
-- not skipped. Re-running the overlapping window cannot duplicate rows
-- (UNIQUE event_id) and only overwrites when the incoming ingested_at
-- is strictly later.
--
-- _REPLAY_GRACE_MINUTES = 90
-- events_clean is assumed to have UNIQUE (event_id).

WITH replay_params AS (
    SELECT
        90 AS _REPLAY_GRACE_MINUTES,
        (
            SELECT COALESCE(MAX(ingested_at), TIMESTAMPTZ '-infinity')
            FROM events_clean
        ) AS last_loaded_at
),

ranked_by_ingest AS (
    SELECT
        s.event_id,
        s.user_id,
        s.app_id,
        s.event_name,
        s.event_time,
        s.ingested_at,
        s.country,
        s.media_source,
        s.campaign,
        s.revenue_usd,
        s.is_test,
        ROW_NUMBER() OVER (
            PARTITION BY s.event_id
            ORDER BY
                s.ingested_at DESC,
                CAST(
                    NULLIF(REPLACE(s.revenue_usd::text, ',', '.'), '')
                    AS DECIMAL(12, 2)
                ) DESC NULLS LAST,
                CONCAT_WS(
                    CHR(31),
                    COALESCE(s.user_id::text, ''), COALESCE(s.app_id::text, ''),
                    COALESCE(s.event_name::text, ''), COALESCE(s.event_time::text, ''),
                    COALESCE(s.ingested_at::text, ''), COALESCE(s.country::text, ''),
                    COALESCE(s.media_source::text, ''), COALESCE(s.campaign::text, ''),
                    COALESCE(s.revenue_usd::text, ''), COALESCE(s.is_test::text, '')
                ) ASC
        ) AS rn
    FROM events_staging s
    CROSS JOIN replay_params p
    WHERE LOWER(TRIM(s.is_test::text)) NOT IN ('true', 't', '1', 'yes')
      AND s.ingested_at >= p.last_loaded_at
          - make_interval(mins => p._REPLAY_GRACE_MINUTES)
),

latest_events AS (
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
    WHERE rn = 1
)

INSERT INTO events_clean (
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
FROM latest_events
ON CONFLICT (event_id)
DO UPDATE SET
    user_id = EXCLUDED.user_id,
    app_id = EXCLUDED.app_id,
    event_name = EXCLUDED.event_name,
    event_time = EXCLUDED.event_time,
    ingested_at = EXCLUDED.ingested_at,
    country = EXCLUDED.country,
    media_source = EXCLUDED.media_source,
    campaign = EXCLUDED.campaign,
    revenue_usd = EXCLUDED.revenue_usd,
    is_test = EXCLUDED.is_test
WHERE events_clean.ingested_at < EXCLUDED.ingested_at;
