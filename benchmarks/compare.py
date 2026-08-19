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
import re
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
        # Params belong in the key. Without them every @Param row of a parameterized benchmark
        # collapsed onto one entry and the last one silently won: SchemaCacheCliffBenchmark
        # .rotateThroughSchemas has six values of distinctSchemas, so five of its six rows were
        # discarded on load and never gated. That benchmark exists specifically to expose a
        # cliff at capacity 100, which is exactly the row that was being thrown away.
        key = f"{entry['benchmark']}:{entry['mode']}"
        params = entry.get("params") or {}
        if params:
            key += "[" + ",".join(f"{k}={params[k]}" for k in sorted(params)) + "]"
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


def load_waivers(path: str) -> list[dict[str, Any]]:
    """Parse benchmarks/waivers.yml.

    Hand-rolled rather than PyYAML on purpose: this gate's whole value is that it always runs,
    and adding a third-party import to it is a way for it to stop running on some machine. The
    file's shape is fixed and documented in its own header, so a 30-line reader is enough.

    Before 2026-08-19 nothing read this file at all. It documented a mechanism -- including
    "Expired waivers FAIL the build. That is deliberate." -- that did not exist, so the first
    person to write a waiver would have been blocked by the very regression they had just
    declared acceptable.
    """
    if not os.path.exists(path):
        return []
    waivers: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    in_list = False
    for raw in open(path, encoding="utf-8"):
        line = raw.split("#", 1)[0].rstrip() if not raw.lstrip().startswith("#") else ""
        if not line.strip():
            continue
        if re.match(r"^waivers:\s*\[\s*\]\s*$", line):
            return []
        if re.match(r"^waivers:\s*$", line):
            in_list = True
            continue
        if not in_list:
            continue
        item = re.match(r"^\s*-\s*(\w+):\s*(.*)$", line)
        if item:
            if current:
                waivers.append(current)
            current = {item.group(1): item.group(2).strip()}
            continue
        field = re.match(r"^\s+(\w+):\s*(.*)$", line)
        if field and current is not None:
            current[field.group(1)] = field.group(2).strip()
    if current:
        waivers.append(current)
    return waivers


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


def waiver_for(waivers: list[dict[str, Any]], key: str, metric: str) -> float | None:
    """Accepted regression percentage for this benchmark+metric, or None."""
    benchmark = key.rsplit(":", 1)[0]
    for w in waivers:
        if w.get("benchmark") == benchmark and w.get("metric") == metric:
            try:
                return float(w.get("accepted_regression", 0))
            except ValueError:
                return None
    return None


def expired_waivers(waivers: list[dict[str, Any]], today: str) -> list[str]:
    """Waivers past their expiry, and waivers too malformed to honour.

    An unparseable or undated waiver is reported rather than ignored: the failure mode this
    guards against is an author believing the gate has been told about an accepted trade when
    it has not. Dates are ISO-8601, so string comparison is chronological.
    """
    problems = []
    for w in waivers:
        name = w.get("benchmark", "<no benchmark>")
        if "benchmark" not in w or "metric" not in w or "expires" not in w:
            problems.append(f"{name}: waiver must name benchmark, metric and expires")
            continue
        if w["metric"] not in ("throughput", "alloc"):
            problems.append(f"{name}: metric must be 'throughput' or 'alloc', got {w['metric']!r}")
            continue
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", w["expires"]):
            problems.append(f"{name}: expires must be YYYY-MM-DD, got {w['expires']!r}")
            continue
        if w["expires"] < today:
            problems.append(f"{name}: waiver expired on {w['expires']}; re-decide or remove it")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--current", required=True)
    ap.add_argument("--update", action="store_true",
                    help="Overwrite the baseline with the current run (merge job on main only).")
    ap.add_argument("--throughput", choices=("blocking", "advisory"), default="blocking",
                    help="Whether Tier 2 (timing) can fail the build. Allocation is ALWAYS "
                         "blocking. Use 'advisory' when the baseline was not recorded on the same "
                         "machine class as the current run - see below.")
    ap.add_argument("--waivers",
                    default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                         "waivers.yml"),
                    help="Accepted, time-boxed regressions. Expired waivers fail the build.")
    ap.add_argument("--today", default=None,
                    help="Override today's date (ISO) for waiver-expiry tests.")
    ap.add_argument("--summary", default=os.environ.get("GITHUB_STEP_SUMMARY"))
    args = ap.parse_args()

    # Why --throughput advisory exists.
    #
    # The first CI run of this gate failed every timing benchmark by +100% to +125%, uniformly,
    # while every allocation metric passed. A real regression is not uniform across unrelated
    # code paths; a machine change is. The baseline had been recorded on a developer workstation
    # and compared against a GitHub shared-vCPU runner, which is roughly half the speed.
    #
    # That is a property of the metric, not a tuning problem. gc.alloc.rate.norm is a counter
    # derived from thread-allocation accounting and is identical on any machine; wall-clock is
    # not. So allocation gates hard everywhere, and throughput only gates against a baseline
    # recorded on the same runner class.

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
    waivers = load_waivers(args.waivers)

    tier1: list[str] = []
    tier2: list[str] = []
    tier2_advisory: list[str] = []
    structural: list[str] = []
    rows: list[tuple[str, str, str, str, str]] = []
    ratios: list[float] = []

    # ---- FAIL CLOSED ---------------------------------------------------------------------
    #
    # Three ways this script used to report "No blocking regressions detected" and exit 0 while
    # gating nothing whatsoever. All three were found by reading it rather than by watching it
    # go green, which is the only way this class of defect is ever found.
    #
    # (1) EMPTY RESULT FILE. load("[]") returns {}; the comparison loop never runs; ratios stays
    #     empty so the geomean block is skipped; tier1 and tier2 stay empty; exit 0. A MISSING
    #     file correctly failed (open() raises), but an empty or fully-excluded JMH run passed
    #     clean. The project has already been bitten once by a false pass in this exact drill --
    #     docs/ANTI_REGRESSION.md:257 records an exit 1 that came from a FileNotFoundError rather
    #     than from a gate decision.
    #
    # (2) MISSING BENCHMARK. The loop iterates `sorted(curr)` -- the CURRENT run only -- so
    #     anything present in the baseline and absent from the run was never compared. Rename a
    #     benchmark, break its @Setup so JMH drops it, mistype a filter, or have one class fail
    #     to load, and its Tier-1 check simply does not happen while the summary still says
    #     everything is fine. This is the third instance of this defect class in this repository.
    #
    # (3) MISSING gc PROFILER. Tier 1 sat inside `if "alloc" in b and "alloc" in c`, and `alloc`
    #     is populated only from secondaryMetrics["gc.alloc.rate.norm"]. Drop -prof gc and the
    #     ONLY blocking tier evaluates on zero benchmarks. Since CI passes --throughput advisory,
    #     Tier 2 cannot block either: the entire gate becomes decorative, and says so in green.
    #     Allocation is this project's headline metric precisely because it is deterministic, so
    #     its absence must be an error, never a skip.
    if not curr:
        structural.append(
            f"{args.current} contains no benchmark results. A run that measured nothing is not "
            f"a run that found nothing.")
    missing = sorted(set(base) - set(curr))
    if missing:
        structural.append(
            f"{len(missing)} benchmark(s) in the baseline did not run: {', '.join(missing)}. "
            f"Deleting or renaming a benchmark requires updating the baseline in the same "
            f"commit; until then its regression check does not exist.")

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
        # One vote per BENCHMARK, not one per mode. Every class declares
        # @BenchmarkMode({Throughput, AverageTime}), so a dual-mode benchmark produced two
        # entries and was weighted TWICE in the suite geometric mean while the throughput-only
        # consolidate_batch1000 was weighted once. The published "suite geometric-mean drift"
        # was a weighted average with weights nobody chose. avgt is preferred because every
        # dual-mode benchmark has it; a thrpt-only benchmark still contributes its single vote.
        if c["mode"] == "avgt" or f"{key.rsplit(':', 1)[0]}:avgt" not in curr:
            ratios.append(ratio if ratio > 0 else 1.0)

        significant = disjoint(ci_of(b), ci_of(c))
        verdict = "ok"
        throughput_waiver = waiver_for(waivers, key, "throughput")
        if (worse_pct > THROUGHPUT_TOLERANCE * 100 and significant
                and not (throughput_waiver is not None
                         and worse_pct <= throughput_waiver)):
            verdict = "REGRESSION"
            (tier2 if args.throughput == "blocking" else tier2_advisory).append(
                f"{key}: {worse_pct:+.1f}% (CIs disjoint)")
        elif worse_pct > THROUGHPUT_TOLERANCE * 100:
            # Point estimate is bad but the intervals overlap - explicitly not a failure.
            verdict = "noisy"

        # --- Tier 1: allocation ------------------------------------------------------------
        alloc_note = ""
        if "alloc" in b and b["alloc"] and "alloc" not in c:
            # The baseline measured allocation for this benchmark and the current run did not.
            # That is a broken instrument, not a clean result.
            verdict = "NO ALLOC DATA"
            structural.append(
                f"{key}: baseline records gc.alloc.rate.norm but the current run does not. "
                f"The gc profiler did not report; re-run with -prof gc. Tier 1 is the only "
                f"blocking tier and it cannot be allowed to disappear silently.")
        elif "alloc" in b and "alloc" in c and b["alloc"]:
            alloc_delta = pct(c["alloc"], b["alloc"])
            alloc_note = f"{alloc_delta:+.1f}%"
            if alloc_delta > ALLOC_TOLERANCE * 100:
                allowed = waiver_for(waivers, key, "alloc")
                if allowed is not None and alloc_delta <= allowed:
                    alloc_note += " (waived)"
                else:
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
            (tier2 if args.throughput == "blocking" else tier2_advisory).append(
                f"suite geometric mean regressed {drift:+.2f}%")

    # --- report ---------------------------------------------------------------------------
    lines = ["### Benchmark comparison", "",
             "| benchmark | baseline | current | delta | alloc / verdict |",
             "|---|---:|---:|---:|---|"]
    for r in rows:
        lines.append("| " + " | ".join(r) + " |")
    lines += ["", f"Suite geometric-mean drift: **{geo_note or 'n/a'}**", ""]

    import datetime
    stale = expired_waivers(waivers, args.today or datetime.date.today().isoformat())
    structural += [f"waivers.yml: {m}" for m in stale]

    if structural:
        lines += ["#### Structural failures (the measurement is not trustworthy)", ""]
        lines += [f"- {m}" for m in structural] + [""]
    if tier1:
        lines += ["#### Tier 1 failures (allocation - deterministic, no retry)", ""]
        lines += [f"- {m}" for m in tier1] + [""]
    if tier2:
        lines += ["#### Tier 2 failures (throughput - CIs disjoint)", ""]
        lines += [f"- {m}" for m in tier2] + [""]
    if tier2_advisory:
        lines += ["#### Tier 2 (throughput) — ADVISORY, not blocking", "",
                  "The baseline was not recorded on this runner class, so timing is reported "
                  "rather than gated. Allocation remains blocking.", ""]
        lines += [f"- {m}" for m in tier2_advisory] + [""]
    if not tier1 and not tier2 and not structural:
        lines += [f"No blocking regressions detected across {len(rows)} benchmarks.", ""]

    report = "\n".join(lines)
    print(report)
    if args.summary:
        with open(args.summary, "a", encoding="utf-8") as fh:
            fh.write(report + "\n")

    if tier1 or tier2 or structural:
        print("\nBLOCKED: see failures above.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
