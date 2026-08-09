#!/usr/bin/env python3
"""
Compare a JMH run against the committed baseline and decide whether to block the merge.

Two tiers, because allocation counters and wall-clock timings have very different noise
characteristics and a single threshold applied to both either produces false failures or lets
real regressions through.

  TIER 1 - deterministic counters (gc.alloc.rate.norm)
      Bytes allocated per operation is derived from thread-allocation accounting, not from a
      clock. It does not move with runner load, so a tight 2% band is real signal. Failures here
      are never retried.

  TIER 2 - throughput / latency
      Blocked only when BOTH the 99.9% confidence intervals are disjoint AND the point estimate
      is more than 10% worse. Overlapping intervals are never a failure regardless of the point
      estimate; that rule is what removes the false-failure mode that gets perf gates disabled.

Additionally blocks if the geometric mean across the whole suite regresses more than 5% with
disjoint intervals - the death-by-a-thousand-cuts case where every benchmark degrades 4% and
none individually trips.

Usage:
    python compare.py --baseline baseline.json --current results.json
    python compare.py --baseline baseline.json --current results.json --update
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from typing import Any

ALLOC_TOLERANCE = 0.02      # Tier 1: 2% more bytes/op blocks.
THROUGHPUT_TOLERANCE = 0.10  # Tier 2: 10% worse, and only with disjoint CIs.
GEOMEAN_TOLERANCE = 0.05     # Suite-wide drift.

# Benchmarks whose mode means "bigger is better".
HIGHER_IS_BETTER = {"thrpt"}


def load(path: str) -> dict[str, dict[str, Any]]:
    """Index a JMH JSON report by benchmark+mode."""
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)

    out: dict[str, dict[str, Any]] = {}
    for entry in raw:
        key = f"{entry['benchmark']}:{entry['mode']}"
        metric = entry["primaryMetric"]
        record: dict[str, Any] = {
            "mode": entry["mode"],
            "score": metric["score"],
            "error": metric.get("scoreError"),
            "unit": metric["scoreUnit"],
        }
        # -prof gc contributes secondary metrics; alloc rate norm is the Tier-1 gate.
        secondary = metric.get("scoreConfidence")
        if secondary and len(secondary) == 2:
            record["ci"] = (secondary[0], secondary[1])
        alloc = entry.get("secondaryMetrics", {}).get("gc.alloc.rate.norm")
        if alloc:
            record["alloc"] = alloc["score"]
        out[key] = record
    return out


def ci_of(record: dict[str, Any]) -> tuple[float, float] | None:
    """99.9% CI if JMH reported one; fall back to score +/- error."""
    if "ci" in record:
        lo, hi = record["ci"]
        if not (math.isnan(lo) or math.isnan(hi)):
            return lo, hi
    err = record.get("error")
    if err is None or math.isnan(err):
        return None
    return record["score"] - err, record["score"] + err


def disjoint(a: tuple[float, float] | None, b: tuple[float, float] | None) -> bool:
    """True when the two intervals do not overlap at all."""
    if a is None or b is None:
        # Without an interval we cannot claim significance. Treat as overlapping, i.e. no block.
        return False
    return a[1] < b[0] or b[1] < a[0]


def pct(new: float, old: float) -> float:
    return ((new - old) / old) * 100.0 if old else 0.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--current", required=True)
    ap.add_argument("--update", action="store_true",
                    help="Overwrite the baseline with the current run (merge job on main only).")
    ap.add_argument("--summary", default=os.environ.get("GITHUB_STEP_SUMMARY"))
    args = ap.parse_args()

    if args.update:
        with open(args.current, encoding="utf-8") as src:
            data = src.read()
        with open(args.baseline, "w", encoding="utf-8") as dst:
            dst.write(data)
        print(f"Baseline updated from {args.current}")
        return 0

    if not os.path.exists(args.baseline):
        print(f"No baseline at {args.baseline}; nothing to compare against.")
        print("This is expected on the first run. Commit the current results as the baseline.")
        return 0

    base = load(args.baseline)
    curr = load(args.current)

    tier1: list[str] = []
    tier2: list[str] = []
    rows: list[tuple[str, str, str, str, str]] = []
    ratios: list[float] = []

    for key in sorted(curr):
        if key not in base:
            rows.append((key, "-", f"{curr[key]['score']:.3f}", "NEW", ""))
            continue

        b, c = base[key], curr[key]
        higher_better = c["mode"] in HIGHER_IS_BETTER

        # --- Tier 2: throughput / latency -------------------------------------------------
        delta = pct(c["score"], b["score"])
        # Normalise so that "worse" is always a positive number.
        worse_pct = -delta if higher_better else delta
        ratio = (b["score"] / c["score"]) if higher_better and c["score"] else (
            (c["score"] / b["score"]) if b["score"] else 1.0)
        ratios.append(ratio if ratio > 0 else 1.0)

        significant = disjoint(ci_of(b), ci_of(c))
        verdict = "ok"
        if worse_pct > THROUGHPUT_TOLERANCE * 100 and significant:
            verdict = "REGRESSION"
            tier2.append(f"{key}: {worse_pct:+.1f}% (CIs disjoint)")
        elif worse_pct > THROUGHPUT_TOLERANCE * 100:
            # Point estimate is bad but the intervals overlap - explicitly not a failure.
            verdict = "noisy"

        # --- Tier 1: allocation ------------------------------------------------------------
        alloc_note = ""
        if "alloc" in b and "alloc" in c and b["alloc"]:
            alloc_delta = pct(c["alloc"], b["alloc"])
            alloc_note = f"{alloc_delta:+.1f}%"
            if alloc_delta > ALLOC_TOLERANCE * 100:
                verdict = "ALLOC REGRESSION"
                tier1.append(f"{key}: allocation {alloc_delta:+.1f}% "
                             f"({b['alloc']:.0f} -> {c['alloc']:.0f} B/op)")

        rows.append((key, f"{b['score']:.3f}", f"{c['score']:.3f}",
                     f"{delta:+.1f}%", alloc_note or verdict))

    # --- suite-wide drift -----------------------------------------------------------------
    geo_note = ""
    if ratios:
        geomean = math.exp(sum(math.log(r) for r in ratios) / len(ratios))
        drift = (geomean - 1.0) * 100
        geo_note = f"{drift:+.2f}%"
        if drift > GEOMEAN_TOLERANCE * 100:
            tier2.append(f"suite geometric mean regressed {drift:+.2f}%")

    # --- report ---------------------------------------------------------------------------
    lines = ["### Benchmark comparison", "",
             "| benchmark | baseline | current | delta | alloc / verdict |",
             "|---|---:|---:|---:|---|"]
    for r in rows:
        lines.append("| " + " | ".join(r) + " |")
    lines += ["", f"Suite geometric-mean drift: **{geo_note or 'n/a'}**", ""]

    if tier1:
        lines += ["#### Tier 1 failures (allocation - deterministic, no retry)", ""]
        lines += [f"- {m}" for m in tier1] + [""]
    if tier2:
        lines += ["#### Tier 2 failures (throughput - CIs disjoint)", ""]
        lines += [f"- {m}" for m in tier2] + [""]
    if not tier1 and not tier2:
        lines += ["No regressions detected.", ""]

    report = "\n".join(lines)
    print(report)
    if args.summary:
        with open(args.summary, "a", encoding="utf-8") as fh:
            fh.write(report + "\n")

    if tier1 or tier2:
        print("\nBLOCKED: see failures above.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
