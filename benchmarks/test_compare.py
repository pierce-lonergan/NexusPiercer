#!/usr/bin/env python3
"""Drills for compare.py.

benchmarks/README.md says "A gate is not live until it has been seen to fail". Until 2026-08-19
nothing exercised compare.py at all: docs/ANTI_REGRESSION.md:253 records exactly two MANUAL
drills, both of which injected a regression and watched it block. Neither covered the opposite
and far more dangerous direction -- the states where the gate reports success because it
measured nothing.

That direction is not hypothetical here. The same section records that the first attempt at the
second manual drill was itself a FALSE PASS: "it exited 1, but from a FileNotFoundError rather
than a gate decision". Exit code alone lied once already, so every assertion below checks the
REPORTED REASON as well as the code.

Run:  python benchmarks/test_compare.py
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
COMPARE = os.path.join(HERE, "compare.py")


def entry(name, mode="avgt", score=100.0, error=1.0, alloc=1000.0, ci=None):
    metric = {"score": score, "scoreError": error, "scoreUnit": "us/op"}
    if ci is not None:
        metric["scoreConfidence"] = list(ci)
    e = {"benchmark": f"io.github.pierce.bench.FlattenBenchmark.{name}",
         "mode": mode, "primaryMetric": metric}
    if alloc is not None:
        e["secondaryMetrics"] = {"gc.alloc.rate.norm": {"score": alloc}}
    return e


BASE = [entry("consolidate_wideFlat"), entry("consolidate_arrayHeavy", score=200.0, alloc=5000.0)]


def run(base, curr, *extra, waivers=None):
    with tempfile.TemporaryDirectory() as d:
        bp, cp = os.path.join(d, "b.json"), os.path.join(d, "c.json")
        json.dump(base, open(bp, "w"))
        json.dump(curr, open(cp, "w"))
        args = [sys.executable, COMPARE, "--baseline", bp, "--current", cp]
        wp = os.path.join(d, "w.yml")
        open(wp, "w").write(waivers if waivers is not None else "waivers: []\n")
        args += ["--waivers", wp, *extra]
        p = subprocess.run(args, capture_output=True, text=True)
        return p.returncode, p.stdout + p.stderr


FAILURES = []


def expect(label, condition, detail=""):
    if condition:
        print(f"  PASS  {label}")
    else:
        print(f"  FAIL  {label}  {detail}")
        FAILURES.append(label)


def main():
    print("compare.py drills")

    # ---- the gate must still pass a clean run, or it is merely broken, not strict ----------
    code, out = run(BASE, BASE)
    expect("baseline vs itself exits 0", code == 0, f"code={code}")
    expect("baseline vs itself reports no regressions", "No blocking regressions" in out)
    expect("baseline vs itself reports +0.00% drift", "+0.00%" in out, out)

    # ---- sub-threshold change must still pass ---------------------------------------------
    ok = [entry("consolidate_wideFlat", alloc=1015.0),
          entry("consolidate_arrayHeavy", score=200.0, alloc=5000.0)]
    code, out = run(BASE, ok)
    expect("+1.5% allocation is under the 2% band and passes", code == 0, out)

    # ---- VACUOUS PASS 1: empty results ------------------------------------------------------
    code, out = run(BASE, [])
    expect("empty results file FAILS", code == 1, f"code={code}")
    expect("empty results names the empty run", "no benchmark results" in out, out)

    # ---- VACUOUS PASS 2: a baseline benchmark did not run -----------------------------------
    code, out = run(BASE, [entry("consolidate_wideFlat")])
    expect("missing benchmark FAILS", code == 1, f"code={code}")
    expect("missing benchmark is named", "consolidate_arrayHeavy" in out, out)

    # ---- VACUOUS PASS 3: gc profiler absent -------------------------------------------------
    nogc = [entry("consolidate_wideFlat", alloc=None),
            entry("consolidate_arrayHeavy", score=200.0, alloc=None)]
    code, out = run(BASE, nogc)
    expect("dropped gc profiler FAILS", code == 1, f"code={code}")
    expect("dropped gc profiler names the metric", "gc.alloc.rate.norm" in out, out)

    # ---- the tier that was always meant to block --------------------------------------------
    bad = [entry("consolidate_wideFlat", alloc=1025.0),
           entry("consolidate_arrayHeavy", score=200.0, alloc=5000.0)]
    code, out = run(BASE, bad)
    expect("+2.5% allocation FAILS", code == 1, f"code={code}")
    expect("+2.5% allocation names the benchmark", "consolidate_wideFlat" in out, out)
    expect("+2.5% allocation is reported as Tier 1", "Tier 1" in out, out)

    # ---- waivers: documented since day one, implemented on 2026-08-19 -----------------------
    w = ("waivers:\n"
         "  - benchmark: io.github.pierce.bench.FlattenBenchmark.consolidate_wideFlat\n"
         "    metric: alloc\n"
         "    accepted_regression: 5\n"
         "    expires: 2099-01-01\n"
         "    justification: drill\n")
    code, out = run(BASE, bad, waivers=w)
    expect("in-date waiver suppresses a 2.5% regression it covers", code == 0, out)

    code, out = run(BASE, [entry("consolidate_wideFlat", alloc=1100.0),
                           entry("consolidate_arrayHeavy", score=200.0, alloc=5000.0)],
                    waivers=w)
    expect("waiver does NOT suppress beyond its accepted percentage", code == 1, out)

    expired = w.replace("2099-01-01", "2020-01-01")
    code, out = run(BASE, BASE, waivers=expired)
    expect("expired waiver FAILS even with no regression", code == 1, f"code={code}")
    expect("expired waiver says so", "expired" in out, out)

    malformed = ("waivers:\n"
                 "  - benchmark: io.github.pierce.bench.FlattenBenchmark.consolidate_wideFlat\n"
                 "    metric: alloc\n"
                 "    accepted_regression: 5\n"
                 "    justification: no expiry field\n")
    code, out = run(BASE, BASE, waivers=malformed)
    expect("waiver with no expiry FAILS rather than being ignored", code == 1, out)

    # ---- empty waiver list must not change any verdict --------------------------------------
    code_a, out_a = run(BASE, ok, waivers="waivers: []\n")
    code_b, out_b = run(BASE, ok, waivers="# only comments\nwaivers: []\n")
    expect("empty waiver list is inert", (code_a, out_a) == (code_b, out_b))

    # ---- geometric mean counts each benchmark once ------------------------------------------
    dual_base = [entry("a", mode="avgt"), entry("a", mode="thrpt"), entry("b", mode="thrpt")]
    dual_curr = [entry("a", mode="avgt", score=110.0), entry("a", mode="thrpt", score=110.0),
                 entry("b", mode="thrpt")]
    code, out = run(dual_base, dual_curr)
    drift = [l for l in out.splitlines() if "geometric-mean drift" in l]
    expect("geomean line is emitted", bool(drift), out)
    if drift:
        # 'a' regresses 10% in avgt. Counted once across 2 benchmarks -> sqrt(1.10) = +4.88%.
        # Counted twice across 3 entries the old way it was a different, unchosen weighting.
        expect("dual-mode benchmark is weighted once, not twice",
               "+4.88%" in drift[0], drift[0])

    # ---- parameterized benchmarks: every @Param row is its own gated row --------------------
    # Keyed on benchmark+mode alone, all six rows of SchemaCacheCliffBenchmark collapsed onto
    # one entry and five were discarded on load. The cliff row was among them.
    def param_entry(v, alloc):
        e = entry("rotateThroughSchemas", mode="avgt", alloc=alloc)
        e["params"] = {"distinctSchemas": str(v)}
        return e

    pbase = [param_entry(2, 1000.0), param_entry(101, 2000.0)]
    pcurr = [param_entry(2, 1000.0), param_entry(101, 2500.0)]
    code, out = run(pbase, pcurr)
    expect("a regression in a non-final @Param row is caught", code == 1, out)
    expect("the failing @Param row is named", "distinctSchemas=101" in out, out)

    code, out = run(pbase, [param_entry(2, 1000.0)])
    expect("a dropped @Param row is caught", code == 1, out)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} DRILL(S) FAILED: {FAILURES}")
        return 1
    print("all drills passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
