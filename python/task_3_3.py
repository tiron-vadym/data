from pathlib import Path

import pandas as pd

from .clean_events import clean_events


def load_daily(path: str | Path, day: str) -> pd.DataFrame:
    """
    Load clean events for a specific UTC day and aggregate revenue
    by app_id and media_source.

    Signature is unchanged so callers can keep passing (path, day).
    The original implementation mixed parsing, filtering and aggregation
    in a way that failed on the documented CSV quirks; this version
    reuses Task 3.1 cleaning and then aggregates with groupby.
    """
    clean_df, _, _ = clean_events(path)
    target_day = pd.Timestamp(day, tz="UTC").date()
    daily_df = clean_df[clean_df["event_time"].dt.date == target_day].copy()

    result = (
        daily_df.groupby(["app_id", "media_source"], dropna=False, as_index=False)[
            "revenue_usd"
        ]
        .sum()
        .rename(columns={"revenue_usd": "revenue"})
    )
    result["key"] = result["app_id"].astype(str) + "-" + result["media_source"].astype(str)
    return (
        result[["key", "revenue"]]
        .sort_values("revenue", ascending=False)
        .reset_index(drop=True)
    )
