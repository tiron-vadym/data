-- Dialect: PostgreSQL
-- Task 2.5: Data quality checks (post-load)
--
-- Each statement returns zero rows when the check passes.
-- Fail = stop the pipeline. Warn = record and continue.

-- Check 1 — FAIL
-- Duplicate event_id in the clean table. The load contract is one row
-- per event; duplicates would double-count revenue.
SELECT
    event_id,
    COUNT(*) AS row_count
FROM events_clean
GROUP BY event_id
HAVING COUNT(*) > 1;

-- Check 2 — FAIL
-- Test traffic reached the reporting table. QA devices must never
-- appear in clean events.
SELECT
    event_id,
    user_id,
    app_id,
    is_test
FROM events_clean
WHERE LOWER(TRIM(is_test::text)) IN ('true', 't', '1', 'yes');

-- Check 3 — WARN
-- Clean events whose app_id is not in apps. This may be a late-arriving
-- dimension row rather than a bad event, so warn instead of failing.
SELECT
    e.event_id,
    e.app_id
FROM events_clean e
LEFT JOIN apps a
    ON e.app_id = a.app_id
WHERE e.app_id IS NOT NULL
  AND a.app_id IS NULL;
