#!/usr/bin/env python3
"""Refuse to benchmark a harness that does not CONTAIN the code it is supposed to measure.

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

THE FIRST VERSION OF THIS SCRIPT DID NOT CATCH THAT INCIDENT EITHER, and the 2026-08-19
adversarial review proved it by drilling exactly the documented failure. It compared the jar's
modification time against the .java sources and never looked at the artifact in between. Run only
the second of the two build commands -- `./mvnw -f benchmarks/pom.xml clean package` -- with a
stale library in ~/.m2 and the jar comes out NEWER than every source file, so the check printed
"Harness is fresh" and exited 0 while the jar carried the previous build's
JsonFlattenerConsolidator. The measured consequence was recorded: consolidate_wideFlat reported
622,541 B/op for a tree whose real value is 398,449, and compare.py cannot see it, because a
self-consistent stale run produces a clean 0.00%. A freshness check that reasons about timestamps
instead of contents is a fifth vacuous pass wearing a hard hat.

So the primary check is now byte-for-byte CONTENT, not mtime:

  1. CONTENT  - every io/github/pierce/**.class inside benchmarks.jar is byte-identical to the
                same class under target/classes, and neither side has classes the other lacks.
                This is the one that detects a stale ~/.m2 artifact, regardless of timestamps.
                (The benchmark harness's own io/github/pierce/bench/** classes are excluded: they
                are compiled by the benchmarks reactor itself and have no target/classes twin.)
  2. COMPILED - no src/main/java source is newer than the newest class in target/classes, or the
                two sides of check 1 are both stale and agreeing with each other is worthless.
  3. MTIME    - no tracked source is newer than the jar. Kept from the first version: it is the
                cheap catch for "edited and rebuilt nothing", and it fires before check 1 has to
                explain a hash.

Usage:
    python benchmarks/check_harness_fresh.py [--root DIR]
Exit 0 when the harness contains and post-dates the code it measures, 1 otherwise.

Drilled by benchmarks/test_check_harness_fresh.py. A gate is not live until it has been seen to
fail, and this one shipped for a day in a state where it never could.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import zipfile

LIBRARY_PREFIX = "io/github/pierce/"
HARNESS_PREFIX = "io/github/pierce/bench/"
REBUILD = ("  ./mvnw install -DskipTests\n"
           "  ./mvnw -f benchmarks/pom.xml clean package")


def default_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def jar_path(root: str) -> str:
    return os.path.join(root, "benchmarks", "target", "benchmarks.jar")


def classes_dir(root: str) -> str:
    return os.path.join(root, "target", "classes")


def source_roots(root: str) -> list[str]:
    return [os.path.join(root, "src", "main", "java"),
            os.path.join(root, "benchmarks", "src", "main", "java")]


def _newest(root: str, suffix: str) -> tuple[str, float]:
    newest_path, newest_mtime = "", 0.0
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            if not name.endswith(suffix):
                continue
            path = os.path.join(dirpath, name)
            mtime = os.path.getmtime(path)
            if mtime > newest_mtime:
                newest_path, newest_mtime = path, mtime
    return newest_path, newest_mtime


def newest_source(root: str) -> tuple[str, float]:
    newest_path, newest_mtime = "", 0.0
    for src in source_roots(root):
        path, mtime = _newest(src, ".java")
        if mtime > newest_mtime:
            newest_path, newest_mtime = path, mtime
    return newest_path, newest_mtime


def library_classes_in_jar(path: str) -> dict[str, str]:
    """md5 of every library class the uber-jar carries, keyed by its jar entry name."""
    with zipfile.ZipFile(path) as jar:
        return {name: hashlib.md5(jar.read(name)).hexdigest()
                for name in jar.namelist()
                if name.startswith(LIBRARY_PREFIX)
                and name.endswith(".class")
                and not name.startswith(HARNESS_PREFIX)}


def library_classes_on_disk(root: str) -> dict[str, str]:
    base = classes_dir(root)
    out: dict[str, str] = {}
    for dirpath, _dirs, files in os.walk(os.path.join(base, *LIBRARY_PREFIX.split("/")[:-1])):
        for name in files:
            if not name.endswith(".class"):
                continue
            path = os.path.join(dirpath, name)
            rel = os.path.relpath(path, base).replace(os.sep, "/")
            with open(path, "rb") as handle:
                out[rel] = hashlib.md5(handle.read()).hexdigest()
    return out


def check(root: str) -> int:
    jar = jar_path(root)
    if not os.path.exists(jar):
        print(f"::error::{jar} does not exist. Run:\n{REBUILD}", file=sys.stderr)
        return 1

    classes = classes_dir(root)
    if not os.path.isdir(classes):
        print(f"::error::{classes} does not exist, so the jar's contents cannot be checked "
              f"against the code they are supposed to be. THIS CHECK MUST NOT SKIP: the whole "
              f"failure it exists to catch produces a jar that looks perfect from the outside. "
              f"Run:\n{REBUILD}", file=sys.stderr)
        return 1

    # ---- 3. MTIME: the cheap catch, reported first because its remedy is the most obvious ----
    source, source_mtime = newest_source(root)
    if not source:
        print("::error::no Java sources found; the freshness check itself is broken",
              file=sys.stderr)
        return 1
    if source_mtime > os.path.getmtime(jar):
        rel = os.path.relpath(source, root)
        print(f"::error::STALE HARNESS. {rel} is newer than benchmarks/target/benchmarks.jar, "
              f"so a benchmark run would measure the PREVIOUS build and report a clean delta. "
              f"Rebuild both, in this order:\n{REBUILD}", file=sys.stderr)
        return 1

    # ---- 2. COMPILED: target/classes is check 1's reference, so it must not be stale itself ----
    lib_source, lib_source_mtime = _newest(os.path.join(root, "src", "main", "java"), ".java")
    newest_class, newest_class_mtime = _newest(classes, ".class")
    if lib_source and newest_class and lib_source_mtime > newest_class_mtime:
        rel = os.path.relpath(lib_source, root)
        print(f"::error::STALE target/classes. {rel} is newer than every class in target/classes, "
              f"so the library was never recompiled and comparing the jar against it would "
              f"compare two equally stale copies. Rebuild both, in this order:\n{REBUILD}",
              file=sys.stderr)
        return 1

    # ---- 1. CONTENT: the check the first version of this script was missing ----
    in_jar = library_classes_in_jar(jar)
    on_disk = library_classes_on_disk(root)
    if not in_jar:
        print(f"::error::benchmarks.jar contains no {LIBRARY_PREFIX}**.class entries at all. "
              f"The shade configuration is not bundling the library, so the harness is measuring "
              f"something other than this project.", file=sys.stderr)
        return 1

    missing = sorted(set(on_disk) - set(in_jar))
    extra = sorted(set(in_jar) - set(on_disk))
    differing = sorted(name for name in set(in_jar) & set(on_disk)
                       if in_jar[name] != on_disk[name])

    if missing or extra or differing:
        detail = []
        if differing:
            detail.append(f"{len(differing)} class(es) differ, e.g. " + ", ".join(differing[:5]))
        if missing:
            detail.append(f"{len(missing)} class(es) absent from the jar, e.g. "
                          + ", ".join(missing[:5]))
        if extra:
            detail.append(f"{len(extra)} class(es) in the jar that no longer exist, e.g. "
                          + ", ".join(extra[:5]))
        print(f"::error::STALE ARTIFACT. benchmarks.jar does not contain the code in "
              f"target/classes: " + "; ".join(detail) + ". The jar is newer than every source "
              f"file, which is exactly why an mtime check passes this state: the harness was "
              f"rebuilt against a stale nexus-piercer artifact in ~/.m2. Any number it produces "
              f"describes the PREVIOUS build, and compare.py will report a clean 0.00% because "
              f"the stale run is self-consistent. Rebuild both, in this order:\n{REBUILD}",
              file=sys.stderr)
        return 1

    print(f"Harness is fresh: benchmarks.jar carries {len(in_jar)} library classes, all "
          f"byte-identical to target/classes, and post-dates "
          f"{os.path.relpath(source, root)}.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=default_root(),
                        help="repository root (defaults to the parent of benchmarks/)")
    args = parser.parse_args(argv)
    return check(args.root)


if __name__ == "__main__":
    sys.exit(main())
