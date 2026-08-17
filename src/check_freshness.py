"""
Freshness Checker: Checks whether data/titles.json was updated within the last 5 days.
Returns exit code 0 if stale (needs refresh) or exit code 1 if fresh (skip).
"""

import sys
from datetime import datetime, timezone
import msgspec

try:
    from config import DATA_DIR, EXPORT_CONFIG
except ImportError:
    from .config import DATA_DIR, EXPORT_CONFIG


def is_dataset_stale(threshold_days: float = 5.0) -> bool:
    """Returns True if titles.json is missing or last_updated is older than threshold_days."""
    json_path = DATA_DIR / EXPORT_CONFIG["filename"]
    if not json_path.exists():
        print(f"[!] {json_path.name} not found. Dataset is stale.")
        return True

    try:
        data = msgspec.json.decode(json_path.read_bytes())
        last_updated_str = data.get("stats", {}).get("last_updated")
        if not last_updated_str:
            print("[!] No last_updated timestamp in stats. Dataset is stale.")
            return True

        # Parse format "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD"
        try:
            last_updated = datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            last_updated = datetime.strptime(last_updated_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )

        age = datetime.now(timezone.utc) - last_updated
        age_days = age.total_seconds() / 86400.0

        if age_days >= threshold_days:
            print(
                f"[*] Dataset is {age_days:.1f} days old (>= {threshold_days} days). Needs refresh."
            )
            return True
        else:
            print(
                f"[+] Dataset is fresh ({age_days:.1f} days old < {threshold_days} days). Skipping."
            )
            return False
    except Exception as e:
        print(f"[!] Error inspecting dataset ({e}). Marking as stale.")
        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Check whether IMDb dataset is stale.")
    parser.add_argument(
        "--threshold-days", type=float, default=5.0, help="Staleness threshold in days"
    )
    args = parser.parse_args()

    stale = is_dataset_stale(threshold_days=args.threshold_days)
    sys.exit(0 if stale else 1)


if __name__ == "__main__":
    main()
