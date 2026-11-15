from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

PROCESSED_DIR = Path(__file__).resolve().parent / "data" / "processed"
CHARTS_DIR = PROCESSED_DIR / "charts"


def require_file(path: Path) -> pd.DataFrame:
    """Read a summary CSV and raise a clear error if it is missing or unreadable."""
    if not path.exists():
        raise FileNotFoundError(f"Required summary file is missing: {path}")

    try:
        df = pd.read_csv(path)
    except Exception as exc:
        raise ValueError(f"Could not read {path}: {exc}") from exc

    if df.empty:
        raise ValueError(f"Summary file is empty: {path}")

    return df


def ensure_columns(df: pd.DataFrame, required: list[str], file_name: str) -> None:
    """Ensure the expected columns are present before plotting."""
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"{file_name} is missing required columns: {missing}")


def create_bar_chart(detections: pd.DataFrame) -> Path:
    """Bar chart for detection counts across the four sensors."""
    ensure_columns(detections, ["sensor", "detection_count"], "detections_by_sensor.csv")
    plt.figure(figsize=(10, 6))
    bars = plt.bar(detections["sensor"], detections["detection_count"], color=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"])
    plt.title("FireScope NASA FIRMS NRT Detection Counts by Sensor")
    plt.xlabel("Sensor")
    plt.ylabel("Detection Count")
    plt.xticks(rotation=20)
    for bar, value in zip(bars, detections["detection_count"]):
        plt.text(bar.get_x() + bar.get_width() / 2, value + 2000, str(value), ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    path = CHARTS_DIR / "detections_by_sensor.png"
    plt.savefig(path, dpi=200)
    plt.close()
    return path


def create_daily_activity_chart(activity: pd.DataFrame) -> Path:
    """Line chart for daily detection counts separated by sensor."""
    ensure_columns(activity, ["sensor", "acq_date", "detection_count"], "daily_fire_activity.csv")

    plt.figure(figsize=(12, 6))
    for sensor, group in activity.groupby("sensor"):
        group = group.sort_values("acq_date")
        plt.plot(group["acq_date"], group["detection_count"], marker="o", linewidth=2, label=sensor)

    plt.title("NASA FIRMS NRT Daily Detection Counts by Sensor")
    plt.xlabel("Acquisition Date")
    plt.ylabel("Detection Count")
    plt.legend(title="Sensor")
    plt.xticks(rotation=25)
    plt.tight_layout()
    path = CHARTS_DIR / "daily_fire_activity.png"
    plt.savefig(path, dpi=200)
    plt.close()
    return path


def create_daily_frp_chart(frp: pd.DataFrame) -> Path:
    """Line chart for daily mean FRP by sensor."""
    ensure_columns(frp, ["sensor", "acq_date", "frp_mean_detected_pixels"], "daily_frp_summary.csv")

    plt.figure(figsize=(12, 6))
    for sensor, group in frp.groupby("sensor"):
        if sensor == "ALL_SENSORS":
            continue
        group = group.sort_values("acq_date")
        plt.plot(group["acq_date"], group["frp_mean_detected_pixels"], marker="o", linewidth=2, label=sensor)

    plt.title("Daily Mean FRP by Sensor")
    plt.xlabel("Acquisition Date")
    plt.ylabel("Mean FRP (detected pixels)")
    plt.legend(title="Sensor")
    plt.xticks(rotation=25)
    plt.tight_layout()
    path = CHARTS_DIR / "daily_frp.png"
    plt.savefig(path, dpi=200)
    plt.close()
    return path


def create_day_night_chart(activity: pd.DataFrame) -> Path:
    """Bar chart comparing daytime and nighttime detection counts."""
    ensure_columns(activity, ["sensor", "daytime_count", "nighttime_count"], "daily_fire_activity.csv")

    daily_totals = activity.groupby("sensor", as_index=False)[["daytime_count", "nighttime_count"]].sum()
    wide = daily_totals.set_index("sensor")

    plt.figure(figsize=(10, 6))
    x = range(len(wide.index))
    plt.bar(x, wide["daytime_count"], width=0.35, label="Daytime")
    plt.bar([i + 0.35 for i in x], wide["nighttime_count"], width=0.35, label="Nighttime")
    plt.xticks([i + 0.17 for i in x], wide.index)
    plt.title("Daytime vs Nighttime Fire Detections by Sensor")
    plt.xlabel("Sensor")
    plt.ylabel("Detection Count")
    plt.legend()
    plt.tight_layout()
    path = CHARTS_DIR / "day_night_activity.png"
    plt.savefig(path, dpi=200)
    plt.close()
    return path


def main() -> int:
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    try:
        detections = require_file(PROCESSED_DIR / "detections_by_sensor.csv")
        activity = require_file(PROCESSED_DIR / "daily_fire_activity.csv")
        frp = require_file(PROCESSED_DIR / "daily_frp_summary.csv")
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 1

    try:
        detections_path = create_bar_chart(detections)
        activity_path = create_daily_activity_chart(activity)
        frp_path = create_daily_frp_chart(frp)
        day_night_path = create_day_night_chart(activity)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 1

    print(f"Created chart: {detections_path.resolve()}")
    print(f"Created chart: {activity_path.resolve()}")
    print(f"Created chart: {frp_path.resolve()}")
    print(f"Created chart: {day_night_path.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
