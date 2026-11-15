from __future__ import annotations

import argparse
import csv
import os
import random
import re
import time
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
from io import StringIO
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
RAW_DATA_DIR = PROJECT_ROOT / "analysis" / "data" / "raw"

FIRMS_API_BASE = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

# NASA FIRMS Standard Processing (SP) products are intentionally delayed by weeks to months,
# so they are not suitable for recent five-day testing. Keep these historical SP names available
# for later one-year historical collection work.
HISTORICAL_SP_DATASETS = [
    "MODIS_SP",
    "VIIRS_SNPP_SP",
    "VIIRS_NOAA20_SP",
    "VIIRS_NOAA21_SP",
]

# For recent active-fire tests, NASA FIRMS Near Real-Time (NRT) products are the correct choice.
DEFAULT_DATASETS = [
    "MODIS_NRT",
    "VIIRS_SNPP_NRT",
    "VIIRS_NOAA20_NRT",
    "VIIRS_NOAA21_NRT",
]
REQUIRED_COLUMNS = {"latitude", "longitude", "acq_date", "acq_time", "satellite"}


def _retry_delay(attempt: int, retry_after: str | None = None) -> float:
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except (TypeError, ValueError):
            try:
                when = parsedate_to_datetime(retry_after)
                return max(0.0, (when - datetime.now(when.tzinfo)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass
    return min(30.0, 0.5 * (2 ** attempt) + random.uniform(0.0, 0.25))


def _get_with_retries(session, url: str, timeout: int, retries: int = 3):
    for attempt in range(retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            status_code = getattr(response, 'status_code', 200)
            if not isinstance(status_code, int):
                status_code = 200
            if status_code == 429 or status_code >= 500:
                if attempt < retries:
                    time.sleep(_retry_delay(attempt, response.headers.get('Retry-After')))
                    continue
            response.raise_for_status()
            return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt == retries:
                raise
            time.sleep(_retry_delay(attempt))
    raise RuntimeError('NASA FIRMS request exhausted retries.')


def ensure_env_file() -> None:
    """Create a .env file from .env.example if needed so the script can run locally."""
    if ENV_PATH.exists():
        return

    example_path = PROJECT_ROOT / ".env.example"
    if not example_path.exists():
        raise FileNotFoundError(
            f"Missing {ENV_PATH} and no .env.example was found in the project root. "
            "Create a .env file with FIRMS_MAP_KEY=your_key_here."
        )

    example_contents = example_path.read_text(encoding="utf-8").strip()
    if not example_contents:
        raise ValueError(f"{example_path} exists but is empty. Add FIRMS_MAP_KEY to it.")

    ENV_PATH.write_text(f"{example_contents}\n", encoding="utf-8")
    print(f"Created {ENV_PATH.name} from {example_path.name}.")


def load_map_key() -> str:
    """Load the FIRMS key from the project-level .env file."""
    ensure_env_file()
    load_dotenv(dotenv_path=ENV_PATH, override=False)

    map_key = os.getenv("FIRMS_MAP_KEY", "").strip()
    if not map_key:
        raise RuntimeError(
            f"FIRMS_MAP_KEY is missing from {ENV_PATH}. Add it in the Django project root "
            "as: FIRMS_MAP_KEY=your_key_here"
        )

    return map_key


def build_firms_url(
    map_key: str,
    dataset: str,
    start_date: str,
    day_range: int,
    bbox: dict[str, float] | None = None,
) -> str:
    """Build the official FIRMS area CSV endpoint.

    Official NASA FIRMS docs show this route:
    /api/area/csv/[MAP_KEY]/[SOURCE]/[AREA_COORDINATES]/[DAY_RANGE]/[DATE]

    NASA expects a named bbox in west,south,east,north order, not a GeoJSON bbox.
    """
    if bbox is None:
        area_coordinates = "world"
    else:
        area_coordinates = ','.join(str(bbox[key]) for key in ('west', 'south', 'east', 'north'))
    return f"{FIRMS_API_BASE}/{map_key}/{dataset}/{area_coordinates}/{day_range}/{start_date}"


def date_chunks(start: date, end: date, max_days: int = 5) -> Iterable[tuple[date, int]]:
    if not 1 <= max_days <= 5:
        raise ValueError('FIRMS area requests support a maximum of five days per request.')
    current = start
    while current <= end:
        day_range = min(max_days, (end - current).days + 1)
        yield current, day_range
        current += timedelta(days=day_range)


def _valid_csv_response(text: str, dataset: str) -> pd.DataFrame:
    raw_text = text.strip()
    if not raw_text.startswith('latitude'):
        raise ValueError(f'NASA FIRMS returned an invalid CSV response for {dataset}.')
    try:
        dataframe = pd.read_csv(StringIO(raw_text))
    except Exception as exc:
        raise ValueError(f'NASA FIRMS returned an unreadable CSV response for {dataset}.') from exc
    missing_columns = sorted(REQUIRED_COLUMNS - set(dataframe.columns))
    if missing_columns:
        raise ValueError(f'NASA FIRMS response for {dataset} is missing columns: {missing_columns}.')
    return dataframe


def fetch_dataset_chunk(
    dataset: str,
    start_date: date,
    day_range: int,
    bbox: dict[str, float] | None,
    map_key: str,
    session=requests,
    retries: int = 3,
) -> pd.DataFrame:
    """Fetch one bounded FIRMS chunk without writing into the shared raw-data directory."""
    if not 1 <= day_range <= 5:
        raise ValueError('FIRMS area requests support a maximum of five days per request.')
    url = build_firms_url(map_key, dataset, start_date.isoformat(), day_range, bbox)
    try:
        response = _get_with_retries(session, url, timeout=60, retries=retries)
    except requests.exceptions.Timeout as exc:
        raise RuntimeError(f'NASA FIRMS timed out for {dataset} starting {start_date.isoformat()}.') from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(
            f'NASA FIRMS request failed for {dataset} starting {start_date.isoformat()} '
            f'(HTTP {getattr(getattr(exc, "response", None), "status_code", "error")}).'
        ) from exc
    return _valid_csv_response(response.text, dataset)


def fetch_availability_ranges(dataset: str, map_key: str, session=requests) -> list[tuple[date, date]]:
    """Read documented FIRMS availability CSV and return explicit date ranges for a source."""
    url = f'https://firms.modaps.eosdis.nasa.gov/api/data_availability/csv/{map_key}/{dataset}'
    try:
        response = _get_with_retries(session, url, timeout=30)
    except requests.exceptions.RequestException as exc:
        status = getattr(getattr(exc, 'response', None), 'status_code', 'error')
        raise RuntimeError(f'NASA FIRMS availability request failed for {dataset} (HTTP {status}).') from exc

    rows = list(csv.DictReader(StringIO(response.text.strip())))
    if not rows:
        raise ValueError(f'NASA FIRMS returned no availability rows for {dataset}.')
    starts = {'start_date', 'date_start', 'start', 'min_date', 'archive_start_date', 'data_start_date'}
    ends = {'end_date', 'date_end', 'end', 'max_date', 'archive_end_date', 'data_end_date'}
    ranges = []
    for row in rows:
        normalized = {str(key or '').strip().lower(): str(value or '').strip() for key, value in row.items()}
        source = next((value for key, value in normalized.items() if key in {'source', 'sensor', 'dataset'}), '')
        if source and source.upper() not in {dataset.upper(), 'ALL'}:
            continue
        start_value = next((value for key, value in normalized.items() if key in starts and value), None)
        end_value = next((value for key, value in normalized.items() if key in ends and value), None)
        if start_value and end_value:
            try:
                ranges.append((date.fromisoformat(start_value[:10]), date.fromisoformat(end_value[:10])))
            except ValueError:
                continue
        else:
            dates = []
            for value in normalized.values():
                match = re.fullmatch(r'\d{4}-\d{2}-\d{2}', value)
                if match:
                    try:
                        dates.append(date.fromisoformat(value))
                    except ValueError:
                        pass
            if dates:
                ranges.append((min(dates), max(dates)))
    if not ranges:
        raise ValueError(f'NASA FIRMS availability response did not contain valid date ranges for {dataset}.')
    return ranges


def file_for_dataset(dataset: str, start_date: date, day_range: int) -> Path:
    end_date = start_date + timedelta(days=day_range - 1)
    filename = f"{dataset}_{start_date.isoformat()}_to_{end_date.isoformat()}.csv"
    return RAW_DATA_DIR / filename


def download_dataset(dataset: str, start_date: date, day_range: int, overwrite: bool = False) -> pd.DataFrame | None:
    """Download one NASA FIRMS dataset and save it as a raw CSV file."""
    map_key = load_map_key()
    raw_date = start_date.isoformat()
    url = build_firms_url(map_key, dataset, raw_date, day_range)
    output_path = file_for_dataset(dataset, start_date, day_range)

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not overwrite:
        print(
            f"Skipping {dataset} ({raw_date}, {day_range} days): "
            f"{output_path.name} already exists. Use --overwrite to replace it."
        )
        return None

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise RuntimeError(f"Request timed out while downloading {dataset} from NASA FIRMS.") from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"HTTP request failed for {dataset}: {exc}") from exc

    content_type = response.headers.get("Content-Type", "")
    if "csv" not in content_type.lower() and not response.text.strip().startswith("latitude"):
        raise ValueError(
            f"Unexpected response from NASA FIRMS for {dataset}. "
            f"Content-Type: {content_type or 'missing'}"
        )

    if not response.text.strip():
        raise ValueError(f"Empty CSV response received for {dataset}.")

    raw_text = response.text.strip()
    if not raw_text:
        raise ValueError(f"Empty CSV response received for {dataset}.")

    if not raw_text.startswith("latitude"):
        raise ValueError(
            f"Unexpected FIRMS response for {dataset}. The API did not return a valid CSV header. "
            f"Response preview: {raw_text[:200]}"
        )

    try:
        dataframe = pd.read_csv(StringIO(raw_text))
    except Exception as exc:
        raise ValueError(f"Invalid CSV response for {dataset}. Could not parse data.") from exc

    missing_columns = sorted(REQUIRED_COLUMNS - set(dataframe.columns))
    if missing_columns:
        raise ValueError(
            f"Missing expected columns for {dataset}: {missing_columns}. "
            "The FIRMS response does not match the expected active-fire schema."
        )

    if output_path.exists() and overwrite:
        output_path.unlink()

    dataframe.to_csv(output_path, index=False)

    end_date = start_date + timedelta(days=day_range - 1)
    if dataframe.empty:
        print(
            f"WARNING: {dataset} | {start_date.isoformat()} to {end_date.isoformat()} | "
            f"No detections returned for this request. CSV saved at {output_path.name}."
        )
    else:
        print(
            f"Downloaded {dataset} | {start_date.isoformat()} to {end_date.isoformat()} | "
            f"rows={len(dataframe)} | file={output_path.name}"
        )
    return dataframe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download a five-day NASA FIRMS fire-data sample for recent Near Real-Time datasets. "
            "SP products are kept in HISTORICAL_SP_DATASETS for later historical collection work."
        )
    )
    parser.add_argument(
        "--start-date",
        default=(date.today() - timedelta(days=4)).strftime("%Y-%m-%d"),
        help="Start date for the sample in YYYY-MM-DD format (default: last 5 days).",
    )
    parser.add_argument(
        "--day-range",
        type=int,
        default=5,
        help="Number of days to fetch at once. Official FIRMS supports 1-5 days.",
    )
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=DEFAULT_DATASETS,
        help="FIRMS datasets to request. Defaults to the 4 standard-processing datasets.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing CSV files in analysis/data/raw/.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        start_date = date.fromisoformat(args.start_date)
    except ValueError as exc:
        raise SystemExit(f"Invalid start date: {args.start_date}. Please use YYYY-MM-DD.") from exc

    if not 1 <= args.day_range <= 5:
        raise SystemExit("FIRMS day range must be between 1 and 5 days.")

    clean_datasets = []
    for dataset in args.datasets:
        dataset_name = dataset.strip()
        if dataset_name:
            clean_datasets.append(dataset_name)

    if not clean_datasets:
        raise SystemExit("No datasets were provided. Add at least one FIRMS dataset.")

    for dataset in clean_datasets:
        try:
            download_dataset(dataset=dataset, start_date=start_date, day_range=args.day_range, overwrite=args.overwrite)
        except Exception as exc:
            print(f"ERROR: {dataset} - {exc}")
            raise

    print("\nFive-day FIRMS fetch setup complete.")
    print(f"Requested date range: {start_date.isoformat()} to {(start_date + timedelta(days=args.day_range - 1)).isoformat()}.")
    print(f"Saved raw CSV files under: {RAW_DATA_DIR}")


if __name__ == "__main__":
    main()
