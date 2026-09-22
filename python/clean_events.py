import sys
from pathlib import Path

import pandas as pd


def _parse_revenue(series: pd.Series) -> tuple[pd.Series, int]:
    raw = series.astype(str).str.strip()
    parsed = pd.to_numeric(raw.str.replace(",", ".", regex=False), errors="coerce")
    unparseable = parsed.isna()
    return parsed.fillna(0.0).astype(float), int(unparseable.sum())


def _normalize_country(series: pd.Series) -> pd.Series:
    code = series.astype(str).str.strip().str.upper()
    valid = code.str.fullmatch(r"[A-Z]{2}", na=False)
    return code.where(valid, "XX")


def _is_test_row(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .isin({"true", "1", "t", "yes"})
    )


def _missing_identifier(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    missing = pd.Series(False, index=df.index)
    for column in columns:
        missing = missing | df[column].isna() | df[column].astype(str).str.strip().eq("")
    return missing


def _payload_sort_key(df: pd.DataFrame) -> pd.Series:
    """Create a deterministic ordering key for otherwise tied deliveries."""
    columns = [
        "user_id", "app_id", "event_name", "event_time", "ingested_at",
        "country", "media_source", "campaign", "revenue_usd", "is_test",
    ]
    return df[columns].astype("string").fillna("").agg("\x1f".join, axis=1)


def clean_events(path: str | Path):
    """
    Read and clean events_raw.csv.

    Returns:
        clean_df: cleaned events
        quarantine_df: rows that could not be safely processed
        stats: processing statistics, including revenue_parse_errors
    """
    path = Path(path)
    quarantine_frames: list[pd.DataFrame] = []
    malformed_rows: list[dict] = []

    def on_bad_line(bad_line: list[str]):
        malformed_rows.append(
            {
                "quarantine_reason": "malformed_csv_row",
                "raw_row": ",".join(str(part) for part in bad_line),
            }
        )
        return None

    try:
        raw_df = pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            on_bad_lines=on_bad_line,
            engine="python",
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to read input file: {exc}") from exc

    rows_in = len(raw_df) + len(malformed_rows)

    if malformed_rows:
        quarantine_frames.append(pd.DataFrame(malformed_rows))

    required_columns = {
        "event_id",
        "user_id",
        "app_id",
        "event_name",
        "event_time",
        "ingested_at",
        "country",
        "media_source",
        "campaign",
        "revenue_usd",
        "is_test",
    }
    missing_columns = required_columns - set(raw_df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    df = raw_df.copy()
    df["revenue_usd"], revenue_parse_errors = _parse_revenue(df["revenue_usd"])

    event_time = pd.to_datetime(df["event_time"], errors="coerce", utc=True)
    ingested_at = pd.to_datetime(df["ingested_at"], errors="coerce", utc=True)
    invalid_timestamp = event_time.isna() | ingested_at.isna()
    if invalid_timestamp.any():
        bad_rows = df.loc[invalid_timestamp].copy()
        bad_rows["quarantine_reason"] = "invalid_timestamp"
        quarantine_frames.append(bad_rows)

    df = df.loc[~invalid_timestamp].copy()
    df["event_time"] = event_time.loc[~invalid_timestamp]
    df["ingested_at"] = ingested_at.loc[~invalid_timestamp]
    df["country"] = _normalize_country(df["country"])
    df = df.loc[~_is_test_row(df["is_test"])].copy()

    invalid_required = _missing_identifier(df, ["event_id", "user_id", "app_id"])
    if invalid_required.any():
        bad_rows = df.loc[invalid_required].copy()
        bad_rows["quarantine_reason"] = "missing_required_field"
        quarantine_frames.append(bad_rows)
        df = df.loc[~invalid_required].copy()

    before_dedup = len(df)
    df = df.assign(_payload_sort_key=_payload_sort_key(df)).sort_values(
        by=["event_id", "ingested_at", "revenue_usd", "_payload_sort_key"],
        ascending=[True, False, False, True],
        na_position="last",
        kind="mergesort",
    )
    df = df.drop_duplicates(subset=["event_id"], keep="first").drop(
        columns="_payload_sort_key"
    )
    duplicates_removed = before_dedup - len(df)

    df = df.sort_values(by=["event_time", "event_id"]).reset_index(drop=True)

    quarantine_df = (
        pd.concat(quarantine_frames, ignore_index=True)
        if quarantine_frames
        else pd.DataFrame()
    )

    stats = {
        "rows_in": rows_in,
        "rows_out": len(df),
        "duplicates_removed": duplicates_removed,
        "rows_quarantined": len(quarantine_df),
        "revenue_parse_errors": revenue_parse_errors,
    }
    # Task 3.1: report unparseable/empty revenue count without touching the
    # pipeline's required one-line summary (that goes to stdout).
    print(f"revenue_parse_errors: {revenue_parse_errors}", file=sys.stderr)
    return df, quarantine_df, stats
