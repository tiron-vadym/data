- SQL dialect: PostgreSQL.
- The source CSV files were not provided with the brief, so equivalent mock data was generated from the documented schema and known data issues. Input CSVs are local only and are not part of the submission.
- `is_test` is stored as text in the CSV (`true`/`false`); SQL casts it to text before filtering so the same predicate works if the column is loaded as boolean.

### 2.1 De-duplication
- For duplicate `event_id` values, the row with the latest `ingested_at` is the authoritative delivery.
- When `ingested_at` ties, parsed `revenue_usd` is the tie-breaker, followed by a canonical ordering of the complete payload. Identical payloads are equivalent.

### 2.2 Window functions
- Days with no events are not filled in. The 7-day moving average is therefore seven observed days, not seven calendar days. A calendar spine from `apps.launched_on` would be required for a true calendar window.
- Running total starts at the first observed event day, not necessarily `launched_on`.

### 2.3 Joining costs
- Grain of the join is date, `app_id`, `media_source`, and `campaign`.
- FULL OUTER JOIN keeps campaigns that exist on only one side.
- ROAS is NULL when cost is zero.

### 2.4 Incremental load
- Filter column is `ingested_at` because corrections do not change `event_time`.
- Watermark is `max(ingested_at)` on `events_clean`.
- Replay window is 90 minutes (`_REPLAY_GRACE_MINUTES`). That is a task working assumption; production should follow measured vendor lag. Events later than that window relative to the watermark will not be picked up until a wider backfill.
- `events_clean` is assumed to have `UNIQUE (event_id)`.

### 2.5 Data quality checks
- Checks run against `events_clean` after the load, not against raw.
- Unknown `app_id` is a warning (late dimension) rather than a failed load.

### 3.1 Cleaning
- A country value is kept only if it is exactly two ASCII letters after trim and upper-case; anything else, including empty string and `--`, becomes `XX`.
- Empty, `NULL`, comma-decimal, and other unparseable revenue values become 0.0 and are counted in `revenue_parse_errors`.
- Invalid timestamps, missing `event_id`/`user_id`/`app_id`, and malformed CSV lines are quarantined. Test rows are dropped, not quarantined.
- Malformed CSV lines are captured via pandas `on_bad_lines` and do not abort the rest of the file.

### 3.2 Small pipeline
- From the repository root the assignment command is:
  `python -m pipeline.run --input data/ --output out/ --since 2026-01-01`
- On Windows, if `python` opens the Microsoft Store, use the project venv or the launcher instead:
  `.venv\Scripts\python.exe -m pipeline.run --input data/ --output out/ --since 2026-01-01`
  or `py -3 -m pipeline.run --input data/ --output out/ --since 2026-01-01`
- Parquet output uses pyarrow, which is listed in `requirements.txt` (pandas needs an engine to write Parquet).
- `--since` filters on UTC event date. Late events for earlier dates are not rewritten unless `--since` is moved back.
- Clean events are Parquet partitioned by `event_date`. Quarantine is a single `quarantine.parquet`.
- Each run writes to a temp directory, then replaces partitions on or after `--since`, so a re-run does not append duplicate Parquet files.
- Each old partition is retained as a sibling backup until all replacements are installed; normal filesystem failures roll back to the old state, and a leftover backup from an interrupted run is recovered on the next start. A versioned dataset plus an atomic manifest would still be needed for a single all-or-nothing snapshot for concurrent readers.

### 3.3 Reading someone else's code
- `day` is a UTC calendar date because timestamps are normalised to UTC in Task 3.1.
- The `(path, day)` signature is unchanged; cleaning is delegated to Task 3.1 so parsing rules stay in one place.
