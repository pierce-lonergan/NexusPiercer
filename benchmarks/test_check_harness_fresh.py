#!/usr/bin/env python3
"""Drills for check_harness_fresh.py.

The 2026-08-19 adversarial review drilled the shipped freshness check against the exact incident
its own docstring described -- a stale nexus-piercer artifact in ~/.m2, a harness rebuilt from it,
every source file older than the resulting jar -- and the check printed "Harness is fresh" and
exited 0. It compared timestamps and never opened the jar. The run it blessed reported
consolidate_wideFlat at 622,541 B/op against a true 398,449, and compare.py rated that a clean
0.00% because a stale run is self-consistent in both directions.

A gate is not live until it has been seen to fail. That one shipped for a day in a state where it
never could, so the rewrite arrives with drills, and the second block below is that incident
reproduced in a temporary directory: a jar that post-dates every source and carries the wrong
bytes.

Run:  python benchmarks/test_check_harness_fresh.py
"""

from __future__ import annotations

import io
import os
import re
import sys
import tempfile
import zipfile
from contextlib import redirect_stderr, redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import check_harness_fresh as sut  # noqa: E402  (importing the script under test by path)

README = os.path.join(HERE, "README.md")

OLD = b"\xca\xfe\xba\xbe old bytecode"
NEW = b"\xca\xfe\xba\xbe new bytecode"

SOURCE_T = 1_000_000_000
CLASS_T = SOURCE_T + 10
JAR_T = SOURCE_T + 20

FAILURES: list[str] = []
DRILLS = 0


def expect(label: str, condition: bool, detail: str = "") -> None:
    global DRILLS
    DRILLS += 1
    if condition:
        print(f"  PASS  {label}")
    else:
        print(f"  FAIL  {label}  {detail}")
        FAILURES.append(label)


def write(path: str, data: bytes, mtime: float) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as handle:
        handle.write(data)
    os.utime(path, (mtime, mtime))


def build_jar(root: str, library: dict[str, bytes], mtime: float,
              harness: dict[str, bytes] | None = None) -> None:
    path = sut.jar_path(root)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with zipfile.ZipFile(path, "w") as jar:
        for name, data in library.items():
            jar.writestr(f"io/github/pierce/{name}", data)
        for name, data in (harness or {}).items():
            jar.writestr(f"io/github/pierce/bench/{name}", data)
        # A real uber-jar is mostly third-party classes; they must be ignored, not compared.
        jar.writestr("com/fasterxml/jackson/databind/ObjectMapper.class", b"third party")
    os.utime(path, (mtime, mtime))


def tree(root: str, *, disk: bytes = NEW, in_jar: bytes | None = None,
         source_t: float = SOURCE_T, class_t: float = CLASS_T, jar_t: float = JAR_T) -> None:
    """A minimal but structurally honest repository: sources, target/classes, uber-jar."""
    write(os.path.join(root, "src", "main", "java", "io", "github", "pierce", "Flattener.java"),
          b"class Flattener {}", source_t)
    write(os.path.join(root, "benchmarks", "src", "main", "java", "io", "github", "pierce",
                       "bench", "Corpus.java"), b"class Corpus {}", source_t)
    write(os.path.join(root, "target", "classes", "io", "github", "pierce", "Flattener.class"),
          disk, class_t)
    build_jar(root, {"Flattener.class": disk if in_jar is None else in_jar}, jar_t,
              harness={"Corpus.class": b"harness only"})


def run(root: str) -> tuple[int, str]:
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = sut.main(["--root", root])
    return code, out.getvalue() + err.getvalue()


def main() -> int:
    print("check_harness_fresh.py drills")

    # ---- it must still pass a genuinely fresh tree, or it is broken rather than strict --------
    with tempfile.TemporaryDirectory() as root:
        tree(root)
        code, out = run(root)
        expect("a fresh harness exits 0", code == 0, out)
        expect("a fresh harness says what it verified",
               "byte-identical to target/classes" in out, out)

    # ---- THE INCIDENT: jar newer than every source, built from a stale ~/.m2 artifact ---------
    with tempfile.TemporaryDirectory() as root:
        tree(root, disk=NEW, in_jar=OLD)
        expect("the drill reproduces the precondition: the jar post-dates every source",
               os.path.getmtime(sut.jar_path(root)) > sut.newest_source(root)[1])
        code, out = run(root)
        expect("a jar carrying the PREVIOUS build FAILS", code == 1, out)
        expect("it is reported as a stale artifact, not a stale timestamp",
               "STALE ARTIFACT" in out, out)
        expect("the differing class is named", "io/github/pierce/Flattener.class" in out, out)
        expect("it records why an mtime check cannot see this",
               "which is exactly why an mtime check passes this state" in out, out)

    # ---- a class the library gained but the jar never picked up ------------------------------
    with tempfile.TemporaryDirectory() as root:
        tree(root)
        write(os.path.join(root, "target", "classes", "io", "github", "pierce", "Added.class"),
              NEW, CLASS_T)
        code, out = run(root)
        expect("a class absent from the jar FAILS", code == 1, out)
        expect("the absent class is named", "io/github/pierce/Added.class" in out, out)

    # ---- a class the library deleted but the jar still carries -------------------------------
    with tempfile.TemporaryDirectory() as root:
        tree(root)
        build_jar(root, {"Flattener.class": NEW, "Deleted.class": OLD}, JAR_T)
        code, out = run(root)
        expect("a class the jar still carries after deletion FAILS", code == 1, out)
        expect("the deleted class is named", "io/github/pierce/Deleted.class" in out, out)

    # ---- the harness's own classes have no target/classes twin and must not be compared ------
    with tempfile.TemporaryDirectory() as root:
        tree(root)
        build_jar(root, {"Flattener.class": NEW}, JAR_T,
                  harness={"Corpus.class": b"x", "FlattenBenchmark.class": b"y"})
        code, out = run(root)
        expect("io/github/pierce/bench/** is excluded from the comparison", code == 0, out)

    # ---- the cheap catch, kept from the first version: edited a source, rebuilt nothing -------
    with tempfile.TemporaryDirectory() as root:
        tree(root, source_t=JAR_T + 100)
        code, out = run(root)
        expect("a source newer than the jar FAILS", code == 1, out)
        expect("that is reported as a stale harness", "STALE HARNESS" in out, out)

    # ---- target/classes is the content check's reference, so it must not be stale itself -----
    with tempfile.TemporaryDirectory() as root:
        # Source newer than target/classes, jar newer still: the jar and the classes agree with
        # each other and both describe code that no longer exists.
        tree(root, source_t=CLASS_T + 5, jar_t=CLASS_T + 50)
        code, out = run(root)
        expect("a source newer than target/classes FAILS", code == 1, out)
        expect("that is reported as stale target/classes", "STALE target/classes" in out, out)

    # ---- the states where a check reports success because it measured nothing ----------------
    with tempfile.TemporaryDirectory() as root:
        tree(root)
        os.remove(sut.jar_path(root))
        code, out = run(root)
        expect("a missing jar FAILS", code == 1, out)

    with tempfile.TemporaryDirectory() as root:
        tree(root)
        os.remove(os.path.join(root, "target", "classes", "io", "github", "pierce",
                               "Flattener.class"))
        os.removedirs(os.path.join(root, "target", "classes", "io", "github", "pierce"))
        code, out = run(root)
        expect("no target/classes FAILS rather than skipping the content check", code == 1, out)
        expect("it says the check must not skip", "MUST NOT SKIP" in out, out)

    with tempfile.TemporaryDirectory() as root:
        tree(root)
        build_jar(root, {}, JAR_T, harness={"Corpus.class": b"x"})
        code, out = run(root)
        expect("a jar with no library classes at all FAILS", code == 1, out)
        expect("it blames the shade configuration", "shade configuration" in out, out)

    # ---- VERIFY THE COUNT, NEVER THE EXIT CODE -----------------------------------------------
    # benchmarks/README.md published "23 drills" for test_compare.py while it emitted 24, which is
    # the same class of defect this repository's doctrine exists to catch. Neither file publishes
    # a count that is not checked against the count that ran.
    with open(README, encoding="utf-8") as handle:
        published = re.search(r"`test_check_harness_fresh\.py` runs \*\*(\d+) drills\*\*",
                              handle.read())
    expect(f"benchmarks/README.md publishes {DRILLS + 1} drills for this file",
           published is not None and int(published.group(1)) == DRILLS + 1,
           "README says " + (published.group(1) if published else
                             "nothing in the form '`test_check_harness_fresh.py` runs **N "
                             "drills**' - THE ANCHOR MUST BIND, rewording it out of existence "
                             "stops the count being checked at all"))

    print()
    if FAILURES:
        print(f"{len(FAILURES)} of {DRILLS} DRILL(S) FAILED: {FAILURES}")
        return 1
    print(f"all {DRILLS} drills passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
