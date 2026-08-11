#!/usr/bin/env bash
#
# THE GROOVY ONE-WAY DOOR.
#
# The Groovy toolchain was removed on 2026-08-11: the last 17 .groovy test sources were ported to
# Java, gmavenplus-plugin was deleted from the POM, and the Groovy runtime and Spock were dropped
# as dependencies. This asserts it stays removed.
#
# WHY THIS IS A SCRIPT AND NOT AN INLINE WORKFLOW STEP
#
# The predecessor of this check lived inline in benchmark.yml, in a workflow whose pull_request
# trigger is path-filtered to src/main, benchmarks, pom.xml and itself. A PR adding
# src/test/groovy/FooTest.groovy matched none of those patterns, so the whole workflow — and
# therefore the gate — was skipped. The script was correct and unreachable. Adversarial review
# caught it; a local drill could not have, because a local shell cannot exercise a GitHub path
# filter.
#
# Two consequences, both deliberate:
#   1. The gate now runs from ci.yml, whose push and pull_request triggers carry no path filter.
#      A Groovy-anywhere invariant must not sit behind a path filter — the whole point is that
#      Groovy can appear anywhere.
#   2. The logic lives here so that `bash scripts/assert-groovy-free.sh` on a workstation runs
#      byte-for-byte what CI runs. Drilling an inline `run:` block only ever tests a copy.
#
# Exit 0 = clean. Exit 1 = Groovy has returned somewhere.

set -uo pipefail

status=0

# `git ls-files`, not `find`. `find .` also walks .claude/worktrees, which is gitignored and holds
# full working copies of the repository including pre-port .groovy files, so a find-based gate
# fails on a developer machine for a reason that has nothing to do with the repository's contents.
# git ls-files measures exactly the tracked tree: no build output, no scratch worktrees.
sources=$(git ls-files '*.groovy')
if [[ -n "${sources}" ]]; then
  count=$(printf '%s\n' "${sources}" | wc -l | tr -d '[:space:]')
  echo "::error::${count} .groovy file(s) tracked in the repository. The port to Java is a one-way door: Groovy costs dynamic dispatch on the per-record path and defers type errors to runtime."
  printf '%s\n' "${sources}"
  status=1
else
  echo "OK: no .groovy files are tracked."
fi

# Every POM in the repository, not just the root one. benchmarks/ is a second Maven module and the
# original check never looked at it.
#
# XML comments are stripped first. This is load-bearing, not defensive: pom.xml's dependency block
# carries a comment naming org.apache.groovy:groovy and org.spockframework:spock-core to record
# why they were removed, and benchmarks/pom.xml explains why it never used gmavenplus. A gate that
# fired on its own removal notes would be switched off within a day.
poms=$(git ls-files '*pom.xml')
if [[ -z "${poms}" ]]; then
  echo "::error::No pom.xml found via git ls-files. Refusing to report a passing gate on an empty scan."
  exit 1
fi

while IFS= read -r pom; do
  [[ -n "${pom}" ]] || continue
  stripped=$(python3 -c '
import re, sys
src = open(sys.argv[1], encoding="utf-8").read()
sys.stdout.write(re.sub(r"<!--.*?-->", "", src, flags=re.S))
' "${pom}")

  # The compiler plugin.
  if hits=$(printf '%s' "${stripped}" | grep -n 'gmavenplus'); then
    echo "::error file=${pom}::${pom} references the Groovy compiler plugin. The Groovy toolchain was removed; it must not return."
    printf '%s\n' "${hits}"
    status=1
  fi

  # The runtime and the Groovy-native test framework. Spock matters as much as Groovy itself:
  # it is a Groovy framework and drags the Groovy runtime back onto the test classpath
  # transitively, which would leave a live Groovy runtime with zero .groovy files and no
  # gmavenplus — a state both halves of the original gate would have passed.
  if hits=$(printf '%s' "${stripped}" | grep -nE '<groupId>[[:space:]]*(org\.apache\.groovy|org\.codehaus\.groovy|org\.spockframework)[[:space:]]*</groupId>'); then
    echo "::error file=${pom}::${pom} declares a Groovy or Spock coordinate. Spock is Groovy-native and pulls the Groovy runtime back onto the test classpath."
    printf '%s\n' "${hits}"
    status=1
  fi
# A pipe would run the loop in a subshell and discard every `status=1` it set — the loop would
# print errors and the script would still exit 0. Fed by a here-string instead.
done <<< "${poms}"

if [[ "${status}" -eq 0 ]]; then
  echo "OK: no POM declares gmavenplus, a Groovy runtime, or Spock ($(printf '%s\n' "${poms}" | wc -l | tr -d '[:space:]') POM(s) scanned)."
fi

# The resolved test classpath, which is the only place a TRANSITIVE reintroduction shows up. The
# checks above read declarations; this one reads what Maven actually puts on the classpath.
#
# Scoped to the root module on purpose: it is the module that ships, and resolving benchmarks/
# as well would double an already slow step for a dev-only module whose declarations are covered
# above. Skipped entirely when SKIP_DEPENDENCY_SCAN is set, so the fast half stays runnable
# offline.
if [[ -n "${SKIP_DEPENDENCY_SCAN:-}" ]]; then
  echo "SKIPPED: dependency-tree scan (SKIP_DEPENDENCY_SCAN set)."
  exit "${status}"
fi

tree=$(mktemp)
if ! ./mvnw -B -ntp dependency:list -DoutputFile="${tree}" -DappendOutput=false -DincludeScope=test > /dev/null 2>&1; then
  echo "::error::dependency:list failed. Refusing to report a passing gate on an unresolved classpath."
  rm -f "${tree}"
  exit 1
fi

if [[ ! -s "${tree}" ]]; then
  echo "::error::dependency:list produced no output. Refusing to report a passing gate on an empty scan."
  rm -f "${tree}"
  exit 1
fi

if hits=$(grep -E 'org\.apache\.groovy|org\.codehaus\.groovy|org\.spockframework' "${tree}"); then
  echo "::error::A Groovy or Spock artifact is on the resolved test classpath, transitively or otherwise."
  printf '%s\n' "${hits}"
  status=1
else
  echo "OK: the resolved test classpath contains no Groovy or Spock artifact."
fi
rm -f "${tree}"

exit "${status}"
