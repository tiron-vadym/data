import argparse
import os
import shutil
import tempfile
from datetime import date
from pathlib import Path

import pandas as pd

from clean_events import clean_events


def _parse_partition_date(partition_dir: Path) -> date | None:
    name = partition_dir.name
    prefix = "event_date="
    if not name.startswith(prefix):
        return None
    try:
        return date.fromisoformat(name[len(prefix) :])
    except ValueError:
        return None


def _publish_partitions(tmp_clean: Path, dest_clean: Path, since_day: date) -> None:
    """Publish partitions and restore the old state if publishing fails."""
    dest_clean.mkdir(parents=True, exist_ok=True)

    for backup in dest_clean.glob(".*.previous"):
        destination = dest_clean / backup.name[1 : -len(".previous")]
        if destination.exists():
            shutil.rmtree(backup)
        else:
            os.replace(backup, destination)

    replacements = (
        [part for part in tmp_clean.iterdir() if part.is_dir()]
        if tmp_clean.exists()
        else []
    )
    new_parts = {part.name for part in replacements}
    stale_parts = [
        part
        for part in dest_clean.iterdir()
        if part.is_dir()
        and part.name not in new_parts
        and (part_day := _parse_partition_date(part)) is not None
        and part_day >= since_day
    ]

    backups: list[tuple[Path, Path]] = []
    installed: list[Path] = []
    try:
        for source in replacements:
            dest = dest_clean / source.name
            if dest.exists():
                backup = dest_clean / f".{source.name}.previous"
                if backup.exists():
                    shutil.rmtree(backup)
                os.replace(dest, backup)
                backups.append((dest, backup))
            shutil.move(str(source), str(dest))
            installed.append(dest)

        for stale in stale_parts:
            backup = dest_clean / f".{stale.name}.previous"
            if backup.exists():
                shutil.rmtree(backup)
            os.replace(stale, backup)
            backups.append((stale, backup))
    except Exception:
        for installed_part in installed:
            if installed_part.exists():
                shutil.rmtree(installed_part)
        for destination, backup in reversed(backups):
            if backup.exists():
                os.replace(backup, destination)
        raise
    else:
        for _, backup in backups:
            if backup.exists():
                shutil.rmtree(backup)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input directory")
    parser.add_argument("--output", required=True, help="Output directory")
    parser.add_argument("--since", required=True, help="Process events from this date")
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    since_day = pd.Timestamp(args.since).date()

    clean_df, quarantine_df, stats = clean_events(input_dir / "events_raw.csv")
    clean_df = clean_df[clean_df["event_time"].dt.date >= since_day].copy()
    clean_df["event_date"] = clean_df["event_time"].dt.date.astype(str)

    tmp_root = Path(tempfile.mkdtemp(prefix="events_pipeline_"))
    try:
        tmp_clean = tmp_root / "events_clean"
        if not clean_df.empty:
            clean_df.to_parquet(
                tmp_clean,
                engine="pyarrow",
                partition_cols=["event_date"],
                index=False,
            )
        _publish_partitions(tmp_clean, output_dir / "events_clean", since_day)

        quarantine_output = output_dir / "quarantine.parquet"
        if quarantine_df.empty:
            if quarantine_output.exists():
                quarantine_output.unlink()
        else:
            tmp_quarantine = tmp_root / "quarantine.parquet"
            quarantine_df.to_parquet(tmp_quarantine, engine="pyarrow", index=False)
            output_dir.mkdir(parents=True, exist_ok=True)
            os.replace(tmp_quarantine, quarantine_output)
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)

    print(
        f"rows in: {stats['rows_in']}, "
        f"rows out: {len(clean_df)}, "
        f"duplicates removed: {stats['duplicates_removed']}, "
        f"rows quarantined: {stats['rows_quarantined']}"
    )
