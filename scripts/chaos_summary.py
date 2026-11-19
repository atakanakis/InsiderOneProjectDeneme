#!/usr/bin/env python3
"""Summarise a chaos soak run.

    CHAOS_RUNS=200 go test -count=1 -run TestInvariantsHoldUnderRandomFaults \\
        -v -timeout 60m ./internal/delivery/ | tee chaos.log
    python3 scripts/chaos_summary.py chaos.log
"""
import re
import sys
from collections import Counter

LINE = re.compile(r"CHAOS_RESULT (.*)")


def parse(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = LINE.search(line)
            if m:
                rows.append(dict(kv.split("=", 1) for kv in m.group(1).split()))
    return rows


def pct(n, total):
    return f"{n}/{total} ({100 * n / total:.1f}%)" if total else "0/0"


def main():
    rows = parse(sys.argv[1])
    total = len(rows)
    if not total:
        sys.exit("no CHAOS_RESULT lines found")
    passed = sum(r["passed"] == "true" for r in rows)
    within = sum(int(r["max_window"]) <= int(r["limit"]) for r in rows)
    no_dupes = sum(r["duplicates"] == "0" for r in rows)
    delivered = sum(r["all_delivered"] == "true" for r in rows)
    http = sum(r["http"] == "true" for r in rows)
    arrivals = sum(int(r["arrivals"]) for r in rows)
    accepted = sum(int(r["accepted"]) for r in rows)
    windows = Counter(int(r["max_window"]) for r in rows)

    print(f"scenarios               {total} ({http} through the real HTTP client)")
    print(f"all invariants held     {pct(passed, total)}")
    print(f"  rate limit held       {pct(within, total)}")
    print(f"  nothing lost          {pct(delivered, total)}")
    print(f"  no double delivery    {pct(no_dupes, total)}")
    print(f"provider requests       {arrivals} for {accepted} notifications "
          f"({100 * (arrivals - accepted) / max(accepted, 1):.1f}% retries after injected failures)")
    print("max in any 1s window   " + ", ".join(f"{k}: {v} runs" for k, v in sorted(windows.items())))
    failed = [r["seed"] for r in rows if r["passed"] != "true"]
    if failed:
        print("failed seeds            " + " ".join(failed))


if __name__ == "__main__":
    main()
