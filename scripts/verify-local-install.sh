#!/usr/bin/env bash
#
# Proves the non-Maven-Central install routes actually work, by performing them.
#
# docs/INSTALL.md describes four ways to get this library without Maven Central. Documentation
# that has never been executed is a guess — writing that file and then running it caught three
# errors in it, including a shade profile that had been failing on every invocation while leaving
# a stale jar behind that made it look successful.
#
# This script runs the routes end to end and consumes each artifact from a throwaway project, so
# "installable" means installed-and-used, not built.
#
#   ./scripts/verify-local-install.sh
#
# Exits non-zero on the first route that does not work.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cd "$ROOT"
VERSION="$(./mvnw -B -ntp -q help:evaluate -Dexpression=project.version -DforceStdout | tr -d '[:space:]')"

pass() { printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; exit 1; }
step() { printf '\n== %s\n' "$1"; }

printf 'Verifying install routes for nexus-piercer %s\n' "$VERSION"

# ---------------------------------------------------------------- route 3: build from source
step "Route 3 — build from source and install locally"

./mvnw -B -ntp -q install -DskipTests
[[ -f "target/nexus-piercer-${VERSION}.jar" ]] || fail "plain jar not produced"
pass "plain jar built and installed to ~/.m2"

# A consumer project that resolves the coordinates from the local repository. If the POM we
# install is wrong — bad coordinates, missing dependencies — this fails where a `ls target/`
# check would not.
mkdir -p "$WORK/consumer/src/main/java"
cat > "$WORK/consumer/pom.xml" <<POM
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>test</groupId><artifactId>consumer</artifactId><version>1</version>
  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>
  <dependencies>
    <dependency>
      <groupId>io.github.pierce-lonergan</groupId>
      <artifactId>nexus-piercer</artifactId>
      <version>${VERSION}</version>
    </dependency>
  </dependencies>
</project>
POM

cat > "$WORK/consumer/src/main/java/Consumer.java" <<'JAVA'
import io.github.pierce.MapFlattener;
import io.github.pierce.JsonReconstructor;
import java.util.*;

public class Consumer {
    public static void main(String[] args) {
        Map<String, Object> src = new LinkedHashMap<>();
        Map<String, Object> user = new LinkedHashMap<>();
        user.put("id", 1);
        src.put("user", user);
        src.put("user_id", "literal");   // the collision case 2.0 exists to fix

        Map<String, Object> flat = new MapFlattener().flatten(src);
        if (flat.size() != 2) {
            throw new AssertionError("expected 2 distinct keys, got " + flat);
        }
        Map<String, Object> back = JsonReconstructor.quickReconstruct(flat);
        if (!src.equals(back)) {
            throw new AssertionError("round trip failed: " + src + " -> " + back);
        }
        System.out.println("consumer OK: " + flat);
    }
}
JAVA

( cd "$WORK/consumer" && mvn -B -ntp -q compile exec:java -Dexec.mainClass=Consumer 2>&1 | tail -3 ) \
    || fail "consumer project could not resolve or run against the installed artifact"
pass "downstream project resolved the coordinates and round-tripped a colliding document"

# ---------------------------------------------------------------- route 3b: the uber jar
step "Route 3b — self-contained uber jar"

./mvnw -B -ntp -q package -Pshade -DskipTests
UBER="target/nexus-piercer-${VERSION}-uber.jar"
[[ -f "$UBER" ]] || fail "uber jar not produced (the shade profile silently failed before)"

# Size alone is the tell: a shade failure leaves a jar the size of the plain one.
SIZE=$(wc -c < "$UBER")
(( SIZE > 5000000 )) || fail "uber jar is only ${SIZE} bytes — dependencies were not bundled"
pass "uber jar is $(( SIZE / 1048576 )) MB"

# Listing captured once, then searched in-memory. `unzip -l ... | grep -q` looks natural and is
# wrong under `set -o pipefail`: grep -q exits on the first match, unzip takes SIGPIPE, and the
# pipeline reports failure for a SUCCESSFUL search. That trap already cost one debugging round in
# the CI ratchet; it is the same shape here.
LISTING="$(unzip -l "$UBER")"
grep -c "com/fasterxml/jackson/" <<<"$LISTING" >/dev/null || fail "uber jar has no Jackson"
grep -c "org/apache/avro/"       <<<"$LISTING" >/dev/null || fail "uber jar has no Avro"
if grep -c "groovyjarjar\|/groovy/" <<<"$LISTING" >/dev/null 2>&1; then
    fail "uber jar still bundles Groovy — src/main is pure Java, this should be test scope"
fi
pass "uber jar bundles Jackson and Avro, and no Groovy"

# Run it with NOTHING else on the classpath. This is the claim "self-contained" actually makes.
javac -cp "$UBER" -d "$WORK/uber-out" "$WORK/consumer/src/main/java/Consumer.java"
java -cp "$UBER:$WORK/uber-out" Consumer > "$WORK/uber.log" 2>&1 \
    || { cat "$WORK/uber.log"; fail "uber jar could not run with no other dependencies"; }
pass "ran against the uber jar alone: $(tail -1 "$WORK/uber.log")"

# ---------------------------------------------------------------- route 2: install-file
step "Route 2 — install a downloaded jar with install:install-file"

mvn -B -ntp -q install:install-file \
    -Dfile="target/nexus-piercer-${VERSION}.jar" \
    -DpomFile=pom.xml \
    -DlocalRepositoryPath="$WORK/repo2" >/dev/null
[[ -d "$WORK/repo2/io/github/pierce-lonergan/nexus-piercer/${VERSION}" ]] \
    || fail "install:install-file did not populate the repository"
pass "install:install-file works, as documented for the GitHub release jar"

# ---------------------------------------------------------------- route 4: offline
step "Route 4 — offline build"

# -o makes any attempted remote resolution a hard failure rather than a silent fetch, so a
# passing offline build is evidence rather than a hope.
./mvnw -B -ntp -q -o package -DskipTests >/dev/null 2>&1 \
    || fail "offline build failed — something still wants the network"
pass "built with -o (fully offline)"

printf '\n\033[32mAll documented install routes verified.\033[0m\n'
