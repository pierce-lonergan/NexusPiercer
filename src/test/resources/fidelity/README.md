# Round-trip fidelity corpus

**`manifest.json` is the contract.** For 108 documents it states exactly what a flatten →
reconstruct round trip preserves and what it destroys. Read a line there and rely on it.

| classification | meaning | how many |
| --- | --- | --- |
| `LOSSLESS` | reproduces the source exactly, including runtime types | 32 |
| `ACCEPTED_LOSS` | does not reproduce the source; the loss is understood, bounded, and the right trade | 12 |
| `DEFECT` | does not reproduce the source, and the loss is wrong | 64 |

## Layout

```
manifest.json                the contract: id -> classification + detail
<family>/<id>.json           one fixture: input, config, metadata, recorded behaviour
```

Six families: `structural`, `value-domain`, `naming`, `avro`, `limits`, `real-world` (18 each).

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
  `flat` (the flattened intermediate), `*Baseline` (the source), `*Doc` (the reconstruction),
  and the per-stack `lossless*` verdicts.

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
