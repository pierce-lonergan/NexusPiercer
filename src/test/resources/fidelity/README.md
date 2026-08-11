# Round-trip fidelity corpus

**`manifest.json` is the contract.** For every document in this directory it states exactly what a
flatten → reconstruct round trip preserves and what it destroys. Read a line there and rely on it.

**The published, consumer-facing form of that contract is
[`docs/ROUND_TRIP_FIDELITY.md`](../../../../docs/ROUND_TRIP_FIDELITY.md)** — the fixture table, the
classification counts and the up-front known-lossy list all live there, *generated* from
`manifest.json` by `FidelityDocGenerator` and pinned by `RoundTripFidelityDocTest`.

No count is restated here on purpose. A hand-written total in a second file is a number that goes
stale the first time a fixture is reclassified, which is the failure this corpus exists to prevent.

## Layout

```
manifest.json                the contract: id -> classification + detail
<family>/<id>.json           one fixture: input, config, metadata, recorded behaviour
```

Six families: `structural`, `value-domain`, `naming`, `avro`, `limits`, `real-world`.

## Reading one fixture

Every fixture file stands alone. Alongside the input document it carries:

- **`rationale`** — why the fixture exists and what it is worth.
- **`catchesBugClass`** — the class of regression it detects.
- **`cannotCatch`** — an honest statement of what it does *not* prove. Read this before
  concluding that a green result means "safe".
- **`predicted`** — the designer's hand-traced prediction, kept verbatim **even where
  measurement disagreed with it**, so the disagreement stays visible.
- **`measurementNote`** — present only where measurement corrected the prediction.
- **`expected`** — the measured renderings the harness asserts:
  `flat` (the MAP stack's flattened intermediate), `flatJson` (the JSON stack's, on `BOTH`
  fixtures — the two flatteners parse with different mappers and can diverge), `*Baseline` (the
  source), `*Doc` (the reconstruction), the per-stack `lossless*` verdicts, and the
  `*DefaultsMatch` flags recording whether the same row comes out of the library's *default*
  reconstruction entry point.

Values in `expected` are rendered with a runtime-type prefix — `S:` string, `I:` int, `L:` long,
`D:` double, `BD:` BigDecimal, `BI:` BigInteger, `B:` boolean — so `1`, `1L` and `"1"` are three
different values, `-0.0` stays distinct from `0.0`, and `37.7740` stays distinct from `37.774`.

## The property that makes this a guarantee

`RoundTripFidelityCorpusTest` asserts the **exact** recorded output, not merely "the round trip
failed". So **fixing a defect turns the build red.** That is deliberate. A fix changes what is
promised to consumers, so it must land together with a manifest update rather than silently
loosening the contract.

**Never resolve a red result by editing a fixture to match new behaviour.** Decide what the new
guarantee is, write it in the manifest, then re-record.

## Re-recording after a deliberate change

```bash
mvn -o test-compile dependency:build-classpath -Dmdep.outputFile=target/test-cp.txt
java -cp "target/classes;target/test-classes;$(cat target/test-cp.txt)" \
     io.github.pierce.fidelity.FidelityCorpusRecorder src/test/resources/fidelity
```

The recorder rewrites only `expected` and `measuredLossless`. It never edits `manifest.json`
and never decides a classification — it prints the fixtures that now disagree with the manifest
and stops. Whether a measured loss is `ACCEPTED_LOSS` or `DEFECT` is a human judgement.
