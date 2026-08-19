# Installing NexusPiercer

Four routes, in order of convenience. **Route 1 is the one you want** — 2.0.0 is released on Maven
Central. The other three exist for contributors, auditors and locked-down networks, and all three
work without Central.

> **Versions.** The current release is **2.0.0**. Development on `main` is `2.1.0-SNAPSHOT`;
> throughout this document that snapshot version is written `${nexus.version}` wherever it appears
> in a locally built artifact's filename, so that a version bump cannot silently invalidate the
> instructions. Substitute the value in the root `pom.xml`.

| Route | Needs Central? | Needs network? | Best for |
|---|---|---|---|
| [1. Maven Central](#1-maven-central) | Yes | Yes | **Almost everyone** |
| [2. GitHub release JAR](#2-github-release-jar) | No | Yes, once | No Central access |
| [3. Build from source](#3-build-from-source) | No | Yes, for deps | Contributors, auditors |
| [4. Fully offline / air-gapped](#4-fully-offline--air-gapped) | No | **No** | Locked-down networks |

---

## 1. Maven Central

```xml
<dependency>
    <groupId>io.github.pierce-lonergan</groupId>
    <artifactId>nexus-piercer</artifactId>
    <version>2.0.0</version>
</dependency>
```

> **Do not use 1.0.8.** It is still on Central and it carries two defects that 2.0.0 fixes.
> Flattened keys are not injective, so a field named `user_id` collides with the nested path
> `user` → `id` and one silently overwrites the other; and reconstruction of ordinary snake_case
> field names can exhaust the heap. Both are **fixed and released in 2.0.0**. See
> [SECURITY.md](../SECURITY.md), which also lists the defects that remain open in 2.0.x.

---

## 2. GitHub release JAR

Download from the [Releases page](https://github.com/pierce-lonergan/NexusPiercer/releases), then
install into your local repository:

```bash
mvn install:install-file -Dfile=nexus-piercer-2.0.0.jar -DpomFile=nexus-piercer-2.0.0.pom
```

Or with Gradle, drop the jar in `libs/`:

```groovy
dependencies { implementation files('libs/nexus-piercer-2.0.0.jar') }
```

**Verify what you downloaded** — checksums are published beside each artifact:

```bash
sha256sum -c nexus-piercer-2.0.0.jar.sha256
```

Note that the plain jar does **not** carry transitive dependencies. If you are not using Maven or
Gradle to resolve them, use the shaded jar from route 3 instead.

---

## 3. Build from source

Requires **JDK 17+**. Nothing else — the Maven wrapper is committed, so no Maven installation is
needed.

```bash
git clone https://github.com/pierce-lonergan/NexusPiercer.git
```
```bash
cd NexusPiercer && ./mvnw install -DskipTests
```

That installs into `~/.m2/repository`, after which the ordinary coordinates resolve. Use the
version from the root `pom.xml` — building `main` gives you the snapshot, not the release:

```xml
<dependency>
    <groupId>io.github.pierce-lonergan</groupId>
    <artifactId>nexus-piercer</artifactId>
    <version>${nexus.version}</version> <!-- 2.1.0-SNAPSHOT on main; 2.0.0 for the release tag -->
</dependency>
```

Run the full suite (2,689 tests, about 5 minutes — 2,689 test invocations on Maven's summary line)
if you want to verify the build yourself:

```bash
./mvnw verify
```

### A self-contained jar with dependencies bundled

For a classpath drop-in with no dependency resolution at all:

```bash
./mvnw package -Pshade -DskipTests
```

This produces `target/nexus-piercer-${nexus.version}-uber.jar` — on `main` today,
`nexus-piercer-2.1.0-SNAPSHOT-uber.jar`. Usable directly:

```bash
java -cp nexus-piercer-${nexus.version}-uber.jar:your-app.jar com.example.Main
```

> Shaded jars relocate and embed their dependencies. If your application already carries Jackson or
> Avro, prefer the plain jar and let your build tool resolve versions, or you risk two copies of
> Jackson on the classpath.

---

## 4. Fully offline / air-gapped

No network at build or run time. Two machines: one with connectivity, one without.

### On the connected machine

```bash
git clone https://github.com/pierce-lonergan/NexusPiercer.git && cd NexusPiercer
```

Resolve every dependency, including plugins, into a self-contained directory:

```bash
./mvnw -Dmaven.repo.local=./offline-repo dependency:go-offline
```
```bash
./mvnw -Dmaven.repo.local=./offline-repo install -DskipTests
```

Then archive `offline-repo/` together with the source tree and move both across.

### On the air-gapped machine

```bash
./mvnw -o -Dmaven.repo.local=/path/to/offline-repo install -DskipTests
```

`-o` forces Maven fully offline; the build fails rather than silently reaching out if anything is
missing. Point your own project at the same local repository:

```bash
mvn -o -Dmaven.repo.local=/path/to/offline-repo package
```

### Verifying the offline claim

Do not take it on trust. Run the build with no route to the network and confirm it still succeeds —
if anything tries to resolve remotely, `-o` turns that into a hard failure rather than a hang.

---

## Which Java version?

Built and tested on **JDK 17 and 21**, Linux and Windows, on every commit. Targets Java 17
bytecode, so it runs on 17 and later. It will not run on 11 or 8.

Spark integration is compiled against **Spark 3.5.x / Scala 2.12** and those dependencies are
`provided` — supply your own on the cluster.

---

## Troubleshooting

**`Could not resolve io.github.pierce-lonergan:nexus-piercer:2.1.0-SNAPSHOT`**
Snapshots are never published to Central — that coordinate only exists in a local `~/.m2` after you
have run route 3's `./mvnw install`. Either run it, or depend on the released `2.0.0` from Central
(route 1).

**`UnsupportedClassVersionError`**
You are on a JDK below 17. Check with `java -version`.

**`NoClassDefFoundError: com/fasterxml/jackson/...`**
You took the plain jar without resolving transitive dependencies. Use the shaded jar, or let
Maven/Gradle resolve them.

**Spark tests skip locally but run in CI**
Expected. Some environments cannot open the NIO selector Netty needs — commonly Windows with
endpoint security. The tests self-skip rather than fail; see `SparkAvailability`. They run on
Linux CI, so the coverage is not lost.

**Build fails on `mvnw: Permission denied`**
`chmod +x mvnw`. The executable bit is committed, but some archive formats drop it.
