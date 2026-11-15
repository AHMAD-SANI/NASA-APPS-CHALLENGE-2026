from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROCESSED_DIR = Path(__file__).resolve().parent / "data" / "processed"


def source_product_and_dates_from_path(path: Path) -> tuple[str, str | None, str | None]:
    """Infer the source dataset and date window from a cleaned FIRMS filename."""
    name = path.name
    if name.endswith("_cleaned.csv"):
        name = name[: -len("_cleaned.csv")]
    if name.endswith(".csv"):
        name = name[: -len(".csv")]

    match = re.match(r"^(?P<product>.+)_(?P<start>\d{4}-\d{2}-\d{2})_to_(?P<end>\d{4}-\d{2}-\d{2})$", name)
    if not match:
        return name, None, None

    return match.group("product"), match.group("start"), match.group("end")


def detect_overlapping_source_product_ranges(files: list[Path]) -> list[dict[str, object]]:
    """Report overlaps for the same source product when multiple downloaded files cover the same dates."""
    by_product: dict[str, list[tuple[Path, str, str]]] = {}
    for path in files:
        product, start_date, end_date = source_product_and_dates_from_path(path)
        if not start_date or not end_date:
            continue
        by_product.setdefault(product, []).append((path, start_date, end_date))

    overlaps: list[dict[str, object]] = []
    for product, ranges in by_product.items():
        if len(ranges) < 2:
            continue

        ordered = sorted(ranges, key=lambda item: (item[1], item[2]))
        for index, (first_path, first_start, first_end) in enumerate(ordered[:-1]):
            first_start_dt = datetime.strptime(first_start, "%Y-%m-%d").date()
            first_end_dt = datetime.strptime(first_end, "%Y-%m-%d").date()
            for second_path, second_start, second_end in ordered[index + 1 :]:
                second_start_dt = datetime.strptime(second_start, "%Y-%m-%d").date()
                second_end_dt = datetime.strptime(second_end, "%Y-%m-%d").date()
                overlap_start = max(first_start_dt, second_start_dt)
                overlap_end = min(first_end_dt, second_end_dt)
                if overlap_start <= overlap_end:
                    overlaps.append(
                        {
                            "source_product": product,
                            "file_a": first_path.name,
                            "file_b": second_path.name,
                            "overlap_start": overlap_start.isoformat(),
                            "overlap_end": overlap_end.isoformat(),
                            "date_window": f"{overlap_start.isoformat()} to {overlap_end.isoformat()}",
                        }
                    )
    return overlaps


def get_processed_files() -> list[Path]:
    """Return the cleaned sensor CSVs we created in the processed folder."""
    files = sorted(PROCESSED_DIR.glob("*_cleaned.csv"))
    if not files:
        raise FileNotFoundError(
            f"No cleaned FIRMS files were found in {PROCESSED_DIR}. "
            "Run the cleaning step first to generate *_cleaned.csv files."
        )
    return files


def sensor_name_from_file(path: Path) -> str:
    """Infer the sensor label from the cleaned filename, while preserving the original dataset identity."""
    name = path.name.replace("_cleaned.csv", "")
    for prefix in ["MODIS", "VIIRS_NOAA21", "VIIRS_NOAA20", "VIIRS_SNPP"]:
        if name.startswith(prefix):
            return prefix
    return name


def load_one_file(path: Path) -> pd.DataFrame | None:
    """Load one cleaned CSV and add a sensor column while keeping the raw file unchanged."""
    print(f"\nReading: {path.name}")
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as exc:
        print(f"Warning: could not read {path.name}: {exc}")
        return None

    if df.empty:
        print(f"Warning: {path.name} is empty. No analysis will be performed for this file.")
        return None

    required = {"acq_date", "daynight"}
    missing = sorted(required - set(df.columns))
    if missing:
        print(f"Warning: {path.name} is missing needed columns: {missing}. Skipping this file.")
        return None

    df = df.copy()
    source_product, _, _ = source_product_and_dates_from_path(path)
    df["source_product"] = source_product
    df["source_file"] = path.name
    df["sensor"] = sensor_name_from_file(path)
    df["acq_date"] = pd.to_datetime(df["acq_date"], errors="coerce")
    df["daynight"] = df["daynight"].fillna("unknown").astype(str).str.upper().str.strip()
    df["frp_numeric"] = pd.to_numeric(df["frp"], errors="coerce") if "frp" in df.columns else pd.Series([pd.NA] * len(df))
    if "confidence_raw" in df.columns:
        df["confidence_category"] = df["confidence_raw"].astype("string").str.lower().str.strip()
        df["confidence_category"] = df["confidence_category"].replace({"<NA>": pd.NA, "nan": pd.NA})
    else:
        df["confidence_category"] = df.get("confidence", pd.Series([pd.NA] * len(df)))
        df["confidence_category"] = df["confidence_category"].astype("string")
        df["confidence_category"] = df["confidence_category"].replace({"<NA>": pd.NA, "nan": pd.NA})
    return df


def deduplicate_overlapping_product_dates(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Remove exact repeated detections across overlapping same-product files without collapsing valid rows by sensor/date."""
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = combined.dropna(subset=["acq_date"]).copy()

    if combined.empty:
        return combined

    detection_columns = [
        "source_product",
        "sensor",
        "latitude",
        "longitude",
        "acq_date",
        "acq_time",
        "satellite",
        "instrument",
        "daynight",
        "frp",
        "brightness",
        "confidence_raw",
        "confidence_category",
        "scan",
        "track",
        "version",
    ]
    available = [column for column in detection_columns if column in combined.columns]
    if not available:
        return combined

    def safe_join_value(value: object) -> str:
        if pd.isna(value):
            return ""
        return str(value).strip()

    combined["dedup_key"] = combined[available].apply(
        lambda row: "|".join(safe_join_value(value) for value in row.tolist()),
        axis=1,
    )
    deduped = combined.loc[~combined["dedup_key"].duplicated()].copy()
    return deduped.drop(columns=["dedup_key"])


def build_summary_tables(frames: list[pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create summary tables for sensor totals, daily activity, FRP, and confidence categories."""
    if not frames:
        return (
            pd.DataFrame(columns=["sensor", "detection_count"]),
            pd.DataFrame(columns=["sensor", "acq_date", "detection_count", "daytime_count", "nighttime_count"]),
            pd.DataFrame(columns=["sensor", "acq_date", "valid_frp_records", "frp_sum_detected_pixels", "frp_mean_detected_pixels"]),
            pd.DataFrame(columns=["sensor", "confidence_category", "count"]),
        )

    combined_input_rows = int(sum(len(frame) for frame in frames))
    combined = deduplicate_overlapping_product_dates(frames)
    combined = combined.dropna(subset=["acq_date"]).copy()
    duplicate_rows_removed = combined_input_rows - len(combined)

    if combined_input_rows > 100_000 and len(combined) < 1_000:
        raise ValueError(
            f"Analysis unexpectedly collapsed from {combined_input_rows} valid rows to {len(combined)} rows. "
            "This suggests overlapping product/date deduplication is removing too much data."
        )

    print(
        f"Validation: input_rows={combined_input_rows}, rows_removed_as_duplicates={duplicate_rows_removed}, "
        f"final_rows={len(combined)}, date_range={combined['acq_date'].min().strftime('%Y-%m-%d')} to {combined['acq_date'].max().strftime('%Y-%m-%d')}"
    )

    sensor_counts = (
        combined.groupby("sensor", dropna=False)
        .size()
        .reset_index(name="detection_count")
        .sort_values("detection_count", ascending=False)
    )

    daily_activity = (
        combined.groupby(["sensor", "acq_date"], dropna=False)
        .agg(
            detection_count=("sensor", "size"),
            daytime_count=("daynight", lambda s: int((s == "D").sum())),
            nighttime_count=("daynight", lambda s: int((s == "N").sum())),
        )
        .reset_index()
        .sort_values(["sensor", "acq_date"])
    )

    valid_frp = combined.dropna(subset=["frp_numeric"]).copy()
    daily_frp = (
        valid_frp.groupby(["sensor", "acq_date"], dropna=False)
        .agg(
            valid_frp_records=("frp_numeric", "count"),
            frp_sum_detected_pixels=("frp_numeric", "sum"),
            frp_mean_detected_pixels=("frp_numeric", "mean"),
        )
        .reset_index()
        .sort_values(["sensor", "acq_date"])
    )

    overall_frp = pd.DataFrame(
        [{
            "sensor": "ALL_SENSORS",
            "acq_date": "ALL_DATES",
            "valid_frp_records": int(valid_frp["frp_numeric"].count()),
            "frp_sum_detected_pixels": float(valid_frp["frp_numeric"].sum()),
            "frp_mean_detected_pixels": float(valid_frp["frp_numeric"].mean()) if not valid_frp.empty else float("nan"),
        }]
    )
    daily_frp = pd.concat([daily_frp, overall_frp], ignore_index=True)

    confidence_values = combined[["sensor", "confidence_category"]].dropna().copy()
    if not confidence_values.empty:
        confidence_values["confidence_category"] = confidence_values["confidence_category"].astype(str)
        confidence_counts = (
            confidence_values.groupby(["sensor", "confidence_category"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(["sensor", "count"], ascending=[True, False])
        )
    else:
        confidence_counts = pd.DataFrame(columns=["sensor", "confidence_category", "count"])

    return sensor_counts, daily_activity, daily_frp, confidence_counts


def print_analysis_report(sensor_counts: pd.DataFrame, daily_activity: pd.DataFrame, daily_frp: pd.DataFrame, confidence_counts: pd.DataFrame, included_files: list[Path], overlaps: list[dict[str, object]]) -> None:
    """Print a simple terminal report so the user can easily review the real detection summary."""
    print("\n==== FireScope FIRMS NRT analysis report ====")

    print(f"Included {len(included_files)} cleaned source files and date ranges:")
    for path in included_files:
        source_product, start_date, end_date = source_product_and_dates_from_path(path)
        print(f"- {path.name} | product={source_product} | range={start_date} to {end_date}")

    if overlaps:
        print("\nDetected overlapping source-product date ranges; duplicates were removed only for the same source product on the same sensor/date:")
        for overlap in overlaps:
            print(
                "- "
                f"product={overlap['source_product']} | files={overlap['file_a']}, {overlap['file_b']} | "
                f"overlap={overlap['overlap_start']} to {overlap['overlap_end']}"
            )
    else:
        print("\nNo overlapping source-product date windows were detected for the selected files.")

    total_records = int(sensor_counts["detection_count"].sum())
    date_min = pd.to_datetime(daily_activity["acq_date"]).min()
    date_max = pd.to_datetime(daily_activity["acq_date"]).max()

    print(f"\nTotal cleaned detection records across all sensors: {total_records}")
    print(f"Date range: {date_min.strftime('%Y-%m-%d')} to {date_max.strftime('%Y-%m-%d')}")
    print("\nDetection counts by sensor:")
    print(sensor_counts.to_string(index=False))

    print("\nDetection counts by acquisition date:")
    print(daily_activity.to_string(index=False))

    print("\nDaily and overall FRP summary (sum of detected-pixel FRP values):")
    print(daily_frp.to_string(index=False))

    if not confidence_counts.empty:
        print("\nOriginal confidence category counts (preserved as categories; no universal threshold applied):")
        print(confidence_counts.to_string(index=False))
    else:
        print("\nOriginal confidence categories are not available in these processed files.")


def save_summary_csvs(sensor_counts: pd.DataFrame, daily_activity: pd.DataFrame, daily_frp: pd.DataFrame, confidence_counts: pd.DataFrame) -> None:
    """Write useful summary files under analysis/data/processed/ for later exploration or plotting."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    sensor_counts.to_csv(PROCESSED_DIR / "detections_by_sensor.csv", index=False)
    daily_activity.to_csv(PROCESSED_DIR / "daily_fire_activity.csv", index=False)
    daily_frp.to_csv(PROCESSED_DIR / "daily_frp_summary.csv", index=False)
    confidence_counts.to_csv(PROCESSED_DIR / "confidence_category_summary.csv", index=False)

    print(f"\nSaved summary CSV files to {PROCESSED_DIR}")
    print("- detections_by_sensor.csv")
    print("- daily_fire_activity.csv")
    print("- daily_frp_summary.csv")
    print("- confidence_category_summary.csv")


def save_deduplicated_detections(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Export the deduplicated detection rows to a single CSV used by the Django API."""
    combined = deduplicate_overlapping_product_dates(frames)
    combined = combined.dropna(subset=["acq_date"]).copy()
    output_path = PROCESSED_DIR / "fire_detections.csv"
    combined.to_csv(output_path, index=False)
    print(f"Saved deduplicated detection export to {output_path} | rows={len(combined)}")
    return combined


def main() -> int:
    try:
        files = get_processed_files()
    except FileNotFoundError as exc:
        print(exc)
        return 1

    overlaps = detect_overlapping_source_product_ranges(files)
    frames: list[pd.DataFrame] = []
    for path in files:
        df = load_one_file(path)
        if df is not None:
            frames.append(df)

    if not frames:
        print("No valid cleaned datasets were loaded for analysis.")
        return 1

    sensor_counts, daily_activity, daily_frp, confidence_counts = build_summary_tables(frames)
    save_summary_csvs(sensor_counts, daily_activity, daily_frp, confidence_counts)
    save_deduplicated_detections(frames)
    print_analysis_report(sensor_counts, daily_activity, daily_frp, confidence_counts, files, overlaps)
    return 0


if __name__ == "__main__":
    sys.exit(main())
