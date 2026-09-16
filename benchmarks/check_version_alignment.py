#!/usr/bin/env python3
"""Assert the benchmark harness measures the version the root POM ships.

WHY THIS IS A SCRIPT AND NOT THREE LINES OF SHELL. It was three lines of shell, and it failed
every scheduled run from the day it was added. The shell read the dependency version with

    BENCH=$(mvn help:evaluate -Dexpression=nexus.piercer.version -DforceStdout 2>/dev/null \\
            || grep -A2 ... )

and both halves were wrong. `nexus.piercer.version` is not a property of benchmarks/pom.xml and
never has been, so the expression could not resolve -- but `help:evaluate` does NOT exit non-zero
on an unresolvable expression. It prints the literal text "null object or invalid expression" on
stdout and exits 0. So the `||` fallback never ran, BENCH became that sentence, the comparison
against the root version failed, and CI reported a version drift that did not exist. The versions
were identical the whole time.

That is the third exit-status trap in this repository's history (`grep -q` under `pipefail` twice,
now this), and the lesson each time is the same: a check whose correctness depends on a tool's exit
code is only as good as your belief about that exit code. This script reads the files, compares the
strings, and is drilled by test_check_version_alignment.py in both directions.

Usage:
    python benchmarks/check_version_alignment.py            # repo root
    python benchmarks/check_version_alignment.py --root .   # explicit
Exit 0 when aligned, 1 when not, 2 when a version could not be determined at all -- which is a
distinct outcome on purpose, because "I could not read it" must never look like "they match".
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

COMMENT = re.compile(r"<!--.*?-->", re.S)
PROPERTY = re.compile(r"\$\{([^}]+)\}")


def strip_comments(xml: str) -> str:
    """Remove XML comments so a commented-out dependency cannot satisfy the check."""
    return COMMENT.sub("", xml)


def project_version(xml: str) -> str | None:
    """The <version> of the project itself: the first one not inside <parent> or a dependency.

    Matched as the version sibling of the project's own <artifactId>, so a <parent> block or a
    dependency declared earlier in the file cannot be mistaken for it.
    """
    body = strip_comments(xml)
    body = re.sub(r"<parent>.*?</parent>", "", body, flags=re.S)
    body = re.sub(r"<dependencies>.*?</dependencies>", "", body, flags=re.S)
    body = re.sub(r"<build>.*?</build>", "", body, flags=re.S)
    m = re.search(r"<artifactId>[^<]+</artifactId>\s*<version>([^<]+)</version>", body)
    return m.group(1).strip() if m else None


def dependency_version(xml: str, group: str, artifact: str) -> str | None:
    """The declared <version> of one dependency, comments stripped."""
    body = strip_comments(xml)
    pattern = re.compile(
        r"<groupId>\s*" + re.escape(group) + r"\s*</groupId>\s*"
        r"<artifactId>\s*" + re.escape(artifact) + r"\s*</artifactId>\s*"
        r"<version>([^<]+)</version>",
        re.S,
    )
    m = pattern.search(body)
    return m.group(1).strip() if m else None


def properties(xml: str) -> dict[str, str]:
    """Every <properties> entry, so a version written as ${foo} can be resolved."""
    body = strip_comments(xml)
    out: dict[str, str] = {}
    for block in re.findall(r"<properties>(.*?)</properties>", body, flags=re.S):
        for name, value in re.findall(r"<([A-Za-z0-9_.\-]+)>([^<]*)</\1>", block):
            out[name] = value.strip()
    return out


def resolve(value: str | None, props: dict[str, str], depth: int = 0) -> str | None:
    """Expand ${...} references. Returns None when a reference cannot be resolved.

    Returning None rather than the raw ${foo} text is the whole point: an unresolvable property is
    the exact condition the previous shell check turned into a false drift report, and it must be
    distinguishable from a real value.
    """
    if value is None or depth > 10:
        return None
    m = PROPERTY.fullmatch(value)
    if not m:
        return None if PROPERTY.search(value) else value
    key = m.group(1)
    if key in ("project.version", "pom.version"):
        return None  # caller substitutes; not resolvable from properties alone
    if key not in props:
        return None
    return resolve(props[key], props, depth + 1)


def check(root: Path) -> tuple[int, list[str]]:
    lines: list[str] = []
    root_pom = root / "pom.xml"
    bench_pom = root / "benchmarks" / "pom.xml"

    for p in (root_pom, bench_pom):
        if not p.is_file():
            return 2, [f"{p} does not exist; cannot compare versions."]

    root_xml = root_pom.read_text(encoding="utf-8")
    bench_xml = bench_pom.read_text(encoding="utf-8")

    root_props = properties(root_xml)
    bench_props = properties(bench_xml)

    shipped = resolve(project_version(root_xml), root_props)
    harness = resolve(project_version(bench_xml), bench_props)
    declared = dependency_version(bench_xml, "io.github.pierce-lonergan", "nexus-piercer")
    if declared is not None and PROPERTY.fullmatch(declared):
        key = PROPERTY.fullmatch(declared).group(1)
        declared = harness if key in ("project.version", "pom.version") else resolve(declared, bench_props)

    lines.append(f"root pom version          : {shipped}")
    lines.append(f"benchmarks own version    : {harness}")
    lines.append(f"benchmarks -> nexus-piercer: {declared}")

    unreadable = [n for n, v in (("root pom version", shipped),
                                 ("benchmarks own version", harness),
                                 ("benchmarks nexus-piercer dependency", declared)) if not v]
    if unreadable:
        lines.append("")
        lines.append("Could not determine: " + ", ".join(unreadable) + ".")
        lines.append("This is exit 2, NOT a pass. An unreadable version is the condition the "
                     "previous shell check silently turned into a false drift report.")
        return 2, lines

    if shipped == harness == declared:
        lines.append("")
        lines.append(f"Aligned at {shipped}: the harness measures the bytecode that ships.")
        return 0, lines

    lines.append("")
    if declared != shipped:
        lines.append(f"DRIFT: benchmarks depend on nexus-piercer:{declared} but the root pom "
                     f"ships {shipped}. The harness would measure a stale installed jar and "
                     f"report real-looking numbers for bytecode that is no longer what ships.")
    if harness != shipped:
        lines.append(f"DRIFT: benchmarks/pom.xml is itself version {harness} while the root pom "
                     f"is {shipped}.")
    lines.append("Bump benchmarks/pom.xml -- both its own <version> and the nexus-piercer "
                 "dependency -- to match the root pom.")
    return 1, lines


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=".", help="repository root (default: .)")
    args = ap.parse_args()
    code, lines = check(Path(args.root))
    for line in lines:
        print(line)
    return code


if __name__ == "__main__":
    sys.exit(main())
