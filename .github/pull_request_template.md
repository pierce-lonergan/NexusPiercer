## What and why

<!-- What changes, and what problem it solves. Link the issue or the finding id from
     docs/audit/FINDINGS.md (e.g. perf-avro/RECON-02). -->

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Performance
- [ ] Refactor (no behaviour change)
- [ ] Build / CI
- [ ] Documentation
- [ ] Breaking change

## Testing

<!-- What you added or ran. "Existing tests pass" is not sufficient for new behaviour. -->

- [ ] Added or updated tests
- [ ] `./mvnw verify` passes locally
- [ ] Coverage did not drop (raise `jacoco.minimum.coverage` if it went up)

## Performance

<!-- Required if this touches a flatten, reconstruct, or conversion path. Delete otherwise. -->

| Benchmark | Before | After | Delta |
|---|---:|---:|---:|
|  |  |  |  |

- [ ] `gc.alloc.rate.norm` did not regress more than 2%
- [ ] Throughput did not regress more than 10%
- [ ] A waiver was added to `benchmarks/waivers.yml` for any intentional regression

## Checklist

- [ ] One logical change — refactor, behaviour, and performance work are not mixed
- [ ] Conventional Commit message
- [ ] `CHANGELOG.md` updated under `[Unreleased]`
- [ ] Public API changes are documented in Javadoc
- [ ] No binaries committed
- [ ] No new `System.out` / `System.err` / `printStackTrace` in `src/main`
