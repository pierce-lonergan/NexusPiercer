#!/usr/bin/env python3
"""Drills for check_version_alignment.py.

The check it replaces was never drilled against the shape that actually broke it. These run in
BOTH directions: an aligned tree must PASS, every way of being misaligned must FAIL, and -- the
case that caused the incident -- a version that cannot be READ must exit 2 rather than looking
like agreement.

    python benchmarks/test_check_version_alignment.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_version_alignment import check  # noqa: E402

PASSED = 0
FAILED = 0


def tree(root_version: str, bench_version: str, dep_block: str, root_props: str = "",
         bench_props: str = "") -> Path:
    d = Path(tempfile.mkdtemp())
    (d / "benchmarks").mkdir()
    (d / "pom.xml").write_text(
        f"""<project>
  <groupId>io.github.pierce-lonergan</groupId>
  <artifactId>nexus-piercer</artifactId>
  <version>{root_version}</version>
  <properties>{root_props}</properties>
</project>""", encoding="utf-8")
    (d / "benchmarks" / "pom.xml").write_text(
        f"""<project>
  <groupId>io.github.pierce-lonergan</groupId>
  <artifactId>nexus-piercer-benchmarks</artifactId>
  <version>{bench_version}</version>
  <properties>{bench_props}</properties>
  <dependencies>
{dep_block}
  </dependencies>
</project>""", encoding="utf-8")
    return d


def dep(version: str) -> str:
    return (f"    <dependency>\n"
            f"      <groupId>io.github.pierce-lonergan</groupId>\n"
            f"      <artifactId>nexus-piercer</artifactId>\n"
            f"      <version>{version}</version>\n"
            f"    </dependency>")


def drill(name: str, root: Path, expected_code: int, expect_text: str | None = None) -> None:
    global PASSED, FAILED
    code, lines = check(root)
    body = "\n".join(lines)
    ok = code == expected_code and (expect_text is None or expect_text in body)
    if ok:
        PASSED += 1
        print(f"  PASS  {name}")
    else:
        FAILED += 1
        print(f"  FAIL  {name}: expected exit {expected_code}"
              f"{' containing ' + repr(expect_text) if expect_text else ''}, got {code}")
        print("        " + body.replace("\n", "\n        "))


def main() -> int:
    V = "2.1.0-SNAPSHOT"
    print("GOOD INPUT MUST PASS")
    drill("aligned literal versions", tree(V, V, dep(V)), 0, "Aligned at")
    drill("aligned via a property",
          tree(V, "${np.version}", dep("${np.version}"),
               bench_props=f"<np.version>{V}</np.version>"), 0, "Aligned at")
    drill("dependency version via ${project.version}",
          tree(V, V, dep("${project.version}")), 0, "Aligned at")

    print("\nDRIFT MUST BLOCK")
    drill("dependency behind the root pom", tree(V, V, dep("2.0.0")), 1, "DRIFT")
    drill("benchmarks own version behind", tree(V, "2.0.0", dep(V)), 1, "DRIFT")
    drill("both drifted", tree(V, "2.0.0", dep("2.0.0")), 1, "DRIFT")
    drill("the real incident shape: root bumped, benchmarks left at the release",
          tree("2.1.0-SNAPSHOT", "2.0.0", dep("2.0.0")), 1, "stale installed jar")

    print("\nUNREADABLE MUST EXIT 2, NOT LOOK LIKE AGREEMENT")
    drill("dependency version is an undefined property",
          tree(V, V, dep("${nexus.piercer.version}")), 2, "exit 2")
    drill("dependency absent entirely", tree(V, V, "    <!-- nothing -->"), 2, "Could not determine")
    drill("dependency present only inside a comment",
          tree(V, V, "    <!--\n" + dep(V) + "\n    -->"), 2, "Could not determine")
    drill("benchmarks pom missing", Path(tempfile.mkdtemp()), 2, "does not exist")

    print(f"\n{PASSED} passed, {FAILED} failed")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
