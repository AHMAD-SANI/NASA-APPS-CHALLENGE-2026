from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run_stage(stage_name: str, script_name: str, *extra_args: str) -> None:
    """Run a stage script from the project root and stop immediately if it fails."""
    script_path = PROJECT_ROOT / "analysis" / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"{stage_name} failed: {script_path} does not exist.")

    print(f"\n=== Starting {stage_name}: {script_name} ===")
    command = [sys.executable, str(script_path), *extra_args]
    print(f"Command: {' '.join(command)}")

    result = subprocess.run(command, cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        raise RuntimeError(f"{stage_name} failed while running {script_name} (exit code {result.returncode}).")

    print(f"=== Completed {stage_name}: {script_name} ===")


def main() -> int:
    extra_args = sys.argv[1:]

    try:
        run_stage("fetch_data", "fetch_data.py", *extra_args)
        run_stage("clean_data", "clean_data.py")
        run_stage("analyze_data", "analyze_data.py")
        run_stage("visualize_data", "visualize_data.py")
    except Exception as exc:
        print(f"\nPIPELINE FAILED at stage: {exc}")
        return 1

    print("\nPipeline completed successfully: fetch_data -> clean_data -> analyze_data -> visualize_data")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
