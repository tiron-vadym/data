### 2.1 De-duplication

The query keeps one row per `event_id` by ranking deliveries with `ROW_NUMBER()` and retaining `rn = 1`. Ranking is by `ingested_at` descending so a later vendor correction of `revenue_usd` replaces the earlier delivery. Test rows are excluded before ranking. If two deliveries share the exact same `ingested_at`, a rank ordered only by that timestamp would be non-deterministic: PostgreSQL could pick either row on different runs. To make the result stable I also order by parsed `revenue_usd` descending (the corrected numeric value wins when the vendor sent two payloads in the same ingest batch) and then by a canonical representation of the remaining payload fields. When the complete payload is identical, either copy is equivalent.

### 2.2 Window functions

The query calculates daily revenue per app and calendar day, then uses window functions to calculate the running revenue total, 7-day moving average, and day-over-day revenue change. The running total is calculated from the earliest available event day for each app. Days with no events are missing from the current output, so `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` represents seven available rows rather than necessarily seven calendar days. This can make the moving average inaccurate when there are gaps between event dates. To fix this, I would generate a complete calendar for each app from `apps.launched_on`, left join the daily revenue to it, and fill missing days with zero revenue before applying the window function. The day-over-day percentage is returned as NULL when the previous day's revenue is zero to avoid division by zero.

### 2.3 Joining costs

The query joins daily revenue with daily campaign costs by date, app, media source, and campaign. I used a FULL OUTER JOIN because both cases must be preserved. Cost with no revenue means spend that has not converted yet — still a real cost the business needs to see, and ROAS is zero after coalescing revenue to 0 (unless cost itself is 0). Revenue with no cost means attributed income without a matching cost row for that day — organic or a missing cost feed; ROAS is NULL because dividing by zero is undefined. Missing revenue or cost values are replaced with zero using COALESCE except for ROAS, which stays NULL when cost is zero.

### 2.4 Incremental load

The incremental load reads new and corrected events from the staging table using `ingested_at`, not `event_time`. A revenue correction keeps the original `event_time` and only `ingested_at` moves forward, so filtering on event time would miss the correction. The high-water mark is `max(ingested_at)` already stored in `events_clean`, minus a 90-minute replay window so rows that arrived just before the previous run are not skipped. Late-arriving rows are absorbed by the replay window. That window is a working assumption for this task; in production it should match measured vendor latency. The smallest safe reload is “last watermark minus the maximum lateness you still need to accept”: a shorter window is cheaper (less data to re-read and upsert) but will silently drop events that arrive later than the grace period. `ON CONFLICT (event_id)` makes a second run over the overlapping period a no-op for unchanged rows and applies a newer `ingested_at` when a correction is present.

### 2.5 Data quality checks

Three post-load queries, each returning zero rows when the check is clean. (1) Duplicate `event_id` in `events_clean` — fail the pipeline, because the clean table’s grain is broken and revenue would double-count. (2) Test traffic in `events_clean` — fail the pipeline, because QA devices must never reach reporting. (3) `app_id` not present in `apps` — warn only, because this is often a late dimension update rather than a bad event; failing would block a valid revenue day. Counts are not used: an empty result set is the pass signal.

### 3.1 Cleaning

The cleaning function reads the raw CSV as strings and normalises revenue, country, and timestamps. Revenue values are converted to float; empty, `NULL`, comma-decimal, and other unparseable values become 0.0 and the number of those cases is returned in `stats['revenue_parse_errors']`. Country is trimmed and upper-cased; a value is kept only when it is a two-letter A–Z code, otherwise it becomes `XX`. Timestamps are parsed as timezone-aware UTC. Test rows are removed. Duplicates follow the same rule as task 2.1. Rows with invalid timestamps, missing required identifiers, or a malformed CSV line are quarantined with a reason instead of being silently dropped, and a single bad row does not abort the rest of the file.

### 3.2 A small pipeline

The script writes clean Parquet to a temporary directory first, then replaces only the event-date partitions that belong to this run (and drops stale partitions on or after `--since` that are no longer produced). If it dies while writing the temp directory, the previous output is left untouched. During publishing, an old partition is first moved to a sibling backup and is restored if a normal filesystem operation fails; a completed replacement is published only after its temporary Parquet files exist. A fully atomic publish for concurrent readers would use a versioned dataset plus an atomic manifest or pointer swap; that is the next production improvement.

### 3.3 Reading someone else's code

The main problems, ranked by impact:

1. **Incorrect revenue parsing** — `astype(float)` crashes on empty values, comma decimals such as `5,25`, `NULL`, or other malformed values.
2. **Incorrect date filtering** — `str.startswith(day)` depends on the timestamp string format and does not handle timezone-aware timestamps or malformed values.
3. **Duplicate events are not handled** — duplicated `event_id` values can cause revenue to be counted more than once.
4. **Malformed rows are not handled** — one bad row can cause the whole function to fail.
5. **Row-by-row iteration is inefficient** — `iterrows()` is much slower than vectorised pandas operations and is unnecessary for this aggregation.
6. **String concatenation for the key is fragile** — missing `app_id` or `media_source` values can produce incorrect or ambiguous keys.
7. **The function reads the entire file** even when only one day is requested, which can become expensive for large files.
8. **The function does not validate the input schema**, so missing columns result in less informative errors.

The rewritten function reuses Task 3.1 cleaning so revenue parsing, timestamps, test-row removal, and deduplication stay consistent. The day is filtered on parsed UTC timestamps. Aggregation uses `groupby` instead of `iterrows()`. The `(path, day)` signature is kept so existing callers do not change; internally the work is split so parsing is not mixed into the daily aggregate.

### 4.1 Star schema

The central fact table would be `fact_campaign_daily`. Its grain is one row per date, app, media source, and campaign. Measures are revenue, cost, impressions, clicks, and derived ROAS. Dimensions are `dim_date`, `dim_app`, `dim_campaign`, and `dim_media_source`, with foreign keys from the fact table to each. An event-level fact is possible but this daily grain matches how marketing already asks “what did this campaign spend and return yesterday?”(more common thing). Degenerate attributes such as `event_name` would stay on a separate event fact if we needed funnel counts at click grain.

### 4.2 A dimension that changes

I would treat campaign as a slowly changing dimension Type 2. When the name changes I would close the current dimension row (set an end date) and insert a new row with the new name, a new surrogate key, and a new effective start. Fact rows already loaded keep the old surrogate key, so yesterday’s report still shows the old name; new facts use the new key. Current-name reporting then needs either a filter on `is_current` or a join that picks the version valid on the fact’s date. The cost is extra keys, extra rows in the dimension, and slightly heavier joins.

### 4.3 Idempotency

For a marketing manager, idempotency means that running the same data process twice produces the same business result as running it once. If yesterday's campaign data is loaded again because of a retry, the report should not show the revenue twice. In other words, repeating the same operation should not duplicate the numbers.

For an engineer, a plain `INSERT ... SELECT` into a daily table is not idempotent because every execution inserts another copy of the selected rows. Common fixes are a unique key with an upsert, deleting and rebuilding the affected partition or day before inserting, or using a staging table followed by a transactional merge. The appropriate approach depends on the table grain and whether existing records can be corrected.

### 4.4 Late data

When an event that belongs to Monday arrives on Thursday, the pipeline sees a new `ingested_at` and upserts it into the clean table, then rebuilds Monday’s event-date partition or daily aggregate. The original Monday email is not silently rewritten. I would tell the client that late-arriving events changed Monday’s figure, send the corrected number, and keep both the original snapshot and the correction timestamp so the change is auditable. Operationally we re-run the incremental load from Thursday’s watermark minus the replay window, which also covers other late rows from the same lag. That is cheaper than recomputing the whole history and still leaves a paper trail if the client asks why Monday moved.

### 4.5 Partitioning

I would partition the fact table by event date because most analytical queries filter or aggregate by reporting day. Pruning then reads only the days in the request instead of the whole table. I would not partition by `ingested_at`: that column describes arrival, not the business day, and a Monday event that arrives on Thursday would land in Thursday’s partition while dashboards still filter Monday. The trade-off is that operational queries of the form “everything ingested in the last hour” become slower, because those rows are scattered across event-date partitions.

### 5. Debugging scenario

1. **Do Sunday raw events exist for this app?** Yes: the vendor delivered something, so the gap is downstream. No: the problem is upstream (SDK, vendor, or file drop) and the pipeline correctly loaded an empty slice.

2. **Did those rows survive cleaning?** Yes: parsing, test-row filters, and quarantine are not the cause. No: look at quarantine reasons, `is_test`, country/revenue parsing, or a bad timestamp that dropped the whole file’s worth of Sunday rows for that app.

3. **Is Sunday’s daily aggregate for this app zero or missing?** Yes, zero/missing while clean events exist: the aggregation join or date truncation is wrong (timezone, campaign grain, inner join to costs). No, the aggregate is non-zero: the warehouse is fine and the dashboard is lying.

4. **Does the dashboard’s query, filter, or cache match that aggregate?** Yes, the source is already zero: go back upstream. No, the warehouse has revenue and the chart shows 0: stale extract, wrong app filter, timezone off-by-one on “Sunday”, or a cached tile.

5. **Do the other eleven apps show Sunday revenue?** Yes, only this app is zero: app-specific feed, `app_id` mapping, or a campaign cost join unique to that app. No, several apps are zero: shared job, partition, or weekend file that the success flag still treated as an empty-but-valid run.
