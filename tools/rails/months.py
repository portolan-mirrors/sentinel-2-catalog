#!/usr/bin/env python3
"""Print one YYYY-MM per line from the first month to the last month,
both included.

The fetch array job reads this list, one line per array task, and
audit_year.sbatch uses it for the twelve months of a year.

    python3 tools/rails/months.py 2015-10 2026-09 > months.txt   # 132 lines
    sbatch --array=0-131%8 tools/rails/fetch_months.sbatch
"""
import sys


def months(first: str, last: str) -> list[str]:
    y0, m0 = (int(x) for x in first.split("-"))
    y1, m1 = (int(x) for x in last.split("-"))
    out = []
    y, m = y0, m0
    while (y, m) <= (y1, m1):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


if __name__ == "__main__":
    print("\n".join(months(sys.argv[1], sys.argv[2])))
