"""
Damage the extract, so you can watch the pipeline refuse it.

The validate task quarantines bad rows and fails the run only when more than
5% of them are bad. This script pushes the extract past that line.

Run:  python scripts/corrupt_extract.py          # break 12% of rows (seed=1)
      python scripts/corrupt_extract.py --repair # restore backup
      python scripts/corrupt_extract.py --fraction 0.20 --seed 42  # custom
      python scripts/corrupt_extract.py --dry-run --verbose  # preview
"""
import argparse
import logging
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

RAW = Path(__file__).resolve().parents[1] / "data" / "raw" / "wdbc.csv"
BACKUP = RAW.with_suffix(".csv.orig")

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repair", action="store_true", help="restore from backup")
    ap.add_argument("--fraction", type=float, default=0.12, help="fraction of rows to corrupt (default: 0.12)")
    ap.add_argument("--seed", type=int, default=1, help="random seed for reproducibility (default: 1)")
    ap.add_argument("--dry-run", action="store_true", help="preview without writing")
    ap.add_argument("--verbose", "-v", action="store_true", help="detailed logging")
    args = ap.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.info(f"Source: {RAW}")
    log.info(f"Backup: {BACKUP}")

    if args.repair:
        if BACKUP.exists():
            shutil.copyfile(BACKUP, RAW)
            log.info(f"✓ Restored {RAW.name} from {BACKUP.name}")
        else:
            log.warning("No backup found; nothing to restore")
        return

    if not RAW.exists():
        log.error(f"Source file missing: {RAW}")
        return

    # Create backup on first corruption
    if not BACKUP.exists():
        shutil.copyfile(RAW, BACKUP)
        log.info(f"✓ Backup created: {BACKUP.name}")

    # Load and corrupt
    frame = pd.read_csv(RAW)
    n = int(len(frame) * args.fraction)

    log.debug(f"Parameters: fraction={args.fraction:.1%}, seed={args.seed}, dry_run={args.dry_run}")
    log.debug(f"Corrupting {n} of {len(frame)} rows")

    # Use seed for reproducible row selection
    hit = frame.sample(n, random_state=args.seed).index

    if args.dry_run:
        log.info(f"[DRY RUN] Would corrupt {n} rows ({n / len(frame):.1%}) (above 5% limit)")
        log.info(f"[DRY RUN] Affected indices: {sorted(hit.tolist())[:10]}{'...' if len(hit) > 10 else ''}")
        return

    frame.loc[hit, "mean_radius"] = np.nan
    frame.to_csv(RAW, index=False)

    log.info(f"✓ Corrupted {n} rows ({n / len(frame):.1%}) — above the 5% limit")
    log.info("→ Re-run DAG: `airflow dags test wdbc_pipeline 2026-08-25`")
    log.info("→ View validate task logs to see rejection reasons")
    log.info("→ Restore with: `python scripts/corrupt_extract.py --repair`")


if __name__ == "__main__":
    main()
