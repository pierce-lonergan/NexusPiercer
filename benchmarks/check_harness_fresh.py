#!/usr/bin/env python3
"""Refuse to benchmark a harness older than the code it is supposed to measure.

benchmarks/ is a separate Maven reactor that depends on the library ARTIFACT, not on its sources.
So `java -jar benchmarks/target/benchmarks.jar` happily measures whatever was last installed into
the local repository, and if you skipped `./mvnw install -DskipTests` it measures the PREVIOUS
build while reporting a perfectly clean 0.00% delta against the baseline. Nothing in compare.py
or in the documented procedure detects that.

This is not a theoretical hole. It was hit during the 2026-08-19 performance pass, by the person
who had just finished writing up the hole: a change was reverted in the source, the harness was
not rebuilt, and the resulting "baseline" recorded the reverted code's numbers. It was caught
only because the figure was recognisable from an earlier run. Nothing would have caught it
otherwise, and the wrong numbers would have become the committed reference for every future gate
decision.

The check is a modification-time comparison, which is coarse but catches the real failure: a
harness jar older than any source file it is meant to include.

Usage:
    python benchmarks/check_harness_fresh.py
Exit 0 when the harness is at least as new as every tracked source file, 1 otherwise.
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JAR = os.path.join(ROOT, "benchmarks", "target", "benchmarks.jar")
SOURCE_ROOTS = [
    os.path.join(ROOT, "src", "main", "java"),
    os.path.join(ROOT, "benchmarks", "src", "main", "java"),
]


def newest_source() -> tuple[str, float]:
    newest_path, newest_mtime = "", 0.0
    for root in SOURCE_ROOTS:
        for dirpath, _dirs, files in os.walk(root):
            for name in files:
                if not name.endswith(".java"):
                    continue
                path = os.path.join(dirpath, name)
                mtime = os.path.getmtime(path)
                if mtime > newest_mtime:
                    newest_path, newest_mtime = path, mtime
    return newest_path, newest_mtime


def main() -> int:
    if not os.path.exists(JAR):
        print(f"::error::{JAR} does not exist. Run:\n"
              f"  ./mvnw install -DskipTests\n"
              f"  ./mvnw -f benchmarks/pom.xml package", file=sys.stderr)
        return 1

    jar_mtime = os.path.getmtime(JAR)
    path, mtime = newest_source()
    if not path:
        print("::error::no Java sources found; the freshness check itself is broken",
              file=sys.stderr)
        return 1

    if mtime > jar_mtime:
        rel = os.path.relpath(path, ROOT)
        print(f"::error::STALE HARNESS. {rel} is newer than benchmarks/target/benchmarks.jar, "
              f"so a benchmark run would measure the PREVIOUS build and report a clean delta. "
              f"Rebuild both, in this order:\n"
              f"  ./mvnw install -DskipTests\n"
              f"  ./mvnw -f benchmarks/pom.xml package", file=sys.stderr)
        return 1

    print(f"Harness is fresh: benchmarks.jar is newer than {os.path.relpath(path, ROOT)}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
