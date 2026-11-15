from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).resolve().parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).resolve().parent / "data" / "processed"


def source_product_and_dates_from_path(path: Path) -> tuple[str, str | None, str | None]:
    """Infer the source dataset and date window from a raw or cleaned FIRMS filename."""
    name = path.name
    if name.endswith("_cleaned.csv"):
        name = name[: -len("_cleaned.csv")]
    if name.endswith(".csv"):
        name = name[: -len(".csv")]

    match = re.match(r"^(?P<product>.+)_(?P<start>\d{4}-\d{2}-\d{2})_to_(?P<end>\d{4}-\d{2}-\d{2})$", name)
    if not match:
        return name, None, None

    return match.group("product"), match.group("start"), match.group("end")


def print_file_summary(path: Path) -> pd.DataFrame:
    """Inspect one raw file and print its structure before cleaning.

    This helps us work from the real NASA FIRMS CSV layout instead of assuming all
    sensors have the same columns.
    """
    print(f"\n===== Inspecting: {path.name} =====")

    if not path.exists():
        raise FileNotFoundError(f"Raw file not found: {path}")

    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=True, na_values=["", "NA", "N/A", "null"])
    except Exception as exc:
        print(f"Warning: could not read {path.name} as CSV: {exc}")
        raise

    print(f"Columns: {list(df.columns)}")
    print(f"Data types:\n{df.dtypes}")
    print(f"Row count: {len(df)}")
    print("Missing values:\n", df.isna().sum())
    print("Sample records:\n", df.head(5).to_string(index=False))
    return df


def ensure_required_columns(df: pd.DataFrame, path: Path) -> None:
    """Check that a file has the minimum columns needed for cleaning."""
    required = ["latitude", "longitude", "acq_date"]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(
            f"{path.name} is missing required columns: {missing}. "
            "The raw FIRMS file may be empty, malformed, or from an unexpected schema."
        )


def convert_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric fields when they are present but preserve VIIRS categorical confidence values.

    MODIS confidence values are numeric; VIIRS confidence is categorical (n/l/h) and must remain in
    confidence_raw/confidence_category instead of being converted to NaN.
    """
    numeric_like = [
        "brightness",
        "bright_ti4",
        "bright_ti5",
        "bright_t31",
        "scan",
        "track",
        "frp",
    ]

    for column in numeric_like:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    if "confidence" in df.columns:
        df["confidence_raw"] = df["confidence"].astype("string").str.strip()
        df["confidence_category"] = df["confidence_raw"].str.lower().str.strip()
        df["confidence_numeric"] = pd.to_numeric(df["confidence_raw"], errors="coerce")

    for column in ["latitude", "longitude"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def clean_single_file(path: Path) -> pd.DataFrame:
    """Clean one source file and return the cleaned DataFrame."""
    print(f"\nProcessing file: {path.name}")
    df = print_file_summary(path)

    if df.empty:
        print(f"Warning: {path.name} is empty. No rows were processed.")
        return df

    ensure_required_columns(df, path)
    source_product, source_start, source_end = source_product_and_dates_from_path(path)
    df = convert_numeric_columns(df)
    df["source_product"] = source_product
    df["source_file"] = path.name
    df["source_start_date"] = source_start
    df["source_end_date"] = source_end

    before_total = len(df)
    before_duplicates = int(df.duplicated().sum())

    print(f"Before cleaning: {before_total} rows")
    print(f"Exact duplicates before cleaning: {before_duplicates}")

    lat_invalid = ~df["latitude"].between(-90, 90)
    lon_invalid = ~df["longitude"].between(-180, 180)

    if "acq_date" in df.columns:
        df["acq_date_parsed"] = pd.to_datetime(df["acq_date"], errors="coerce", format="%Y-%m-%d")
        date_invalid = df["acq_date_parsed"].isna()
    else:
        date_invalid = pd.Series(True, index=df.index)

    if "acq_time" in df.columns:
        df["acq_time_parsed"] = pd.to_numeric(df["acq_time"], errors="coerce")
        time_invalid = df["acq_time_parsed"].isna()
    else:
        time_invalid = pd.Series(True, index=df.index)

    invalid_mask = lat_invalid | lon_invalid | date_invalid | time_invalid
    invalid_count = int(invalid_mask.sum())

    print(f"Latitude outside [-90, 90]: {int(lat_invalid.sum())}")
    print(f"Longitude outside [-180, 180]: {int(lon_invalid.sum())}")
    print(f"Invalid acquisition dates: {int(date_invalid.sum())}")
    print(f"Invalid acquisition times: {int(time_invalid.sum())}")
    print(f"Rows removed for invalid coordinates or dates/times: {invalid_count}")

    df = df.loc[~invalid_mask].copy()

    duplicates_after_invalid = int(df.duplicated().sum())
    print(f"Exact duplicates within this source file after invalid-row removal: {duplicates_after_invalid}")
    df = df.drop_duplicates()

    after_total = len(df)
    print(f"After cleaning: {after_total} rows")
    print("Missing values after cleaning:\n", df.isna().sum())

    if before_total > 100_000 and after_total < 1_000:
        raise ValueError(
            f"{path.name} unexpectedly collapsed from {before_total} input rows to {after_total} rows. "
            "This suggests the duplicate/validation logic is incorrectly removing valid detections."
        )

    if "acq_date_parsed" in df.columns:
        df["acq_date"] = df["acq_date_parsed"].dt.strftime("%Y-%m-%d")
        df = df.drop(columns=["acq_date_parsed"])

    if "acq_time_parsed" in df.columns:
        df["acq_time"] = df["acq_time_parsed"].astype("Int64")
        df = df.drop(columns=["acq_time_parsed"])

    print("Sample cleaned records:\n", df.head(5).to_string(index=False))
    print("Confidence handling: original sensor-specific values are preserved; no universal confidence threshold is applied.")
    print(
        f"Validation: input_rows={before_total}, duplicate_rows_removed={before_duplicates + duplicates_after_invalid}, "
        f"final_rows={after_total}, date_range={df['acq_date'].min()} to {df['acq_date'].max()}"
    )

    return df


def process_all_files() -> None:
    """Process each downloaded NRT CSV file one at a time to stay memory-friendly."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    raw_files = sorted(RAW_DIR.glob("*NRT*.csv"))

    if not raw_files:
        print(f"No NRT CSV files were found in {RAW_DIR}.")
        return

    print(f"Found {len(raw_files)} raw FIRMS NRT files to inspect.")
    print("Included raw files and date ranges:")
    for raw_file in raw_files:
        source_product, start_date, end_date = source_product_and_dates_from_path(raw_file)
        print(f"- {raw_file.name} | product={source_product} | range={start_date} to {end_date}")

    total_input_rows = 0
    total_output_rows = 0
    total_duplicate_rows_removed = 0

    for raw_file in raw_files:
        try:
            cleaned_df = clean_single_file(raw_file)
            target_name = raw_file.name.replace(".csv", "_cleaned.csv")
            target_path = PROCESSED_DIR / target_name
            cleaned_df.to_csv(target_path, index=False)
            total_input_rows += len(pd.read_csv(raw_file, low_memory=False, dtype=str))
            total_output_rows += len(cleaned_df)
            total_duplicate_rows_removed += max(len(pd.read_csv(raw_file, low_memory=False, dtype=str)) - len(cleaned_df), 0)
            print(f"Saved cleaned CSV: {target_path.name} | rows={len(cleaned_df)}")
        except Exception as exc:
            print(f"Warning: failed to process {raw_file.name}: {exc}")
            print("This file may be empty, malformed, or missing required columns.")
            continue

    print(
        f"Overall validation: input_rows={total_input_rows}, duplicate_rows_removed={total_duplicate_rows_removed}, "
        f"final_rows={total_output_rows}, date_range={raw_files[0].name} to {raw_files[-1].name}"
    )


if __name__ == "__main__":
    try:
        process_all_files()
        print("\nCleaning complete. Processed files are saved in analysis/data/processed/.")
    except Exception as exc:
        print(f"Unexpected failure during cleaning: {exc}")
        sys.exit(1)
