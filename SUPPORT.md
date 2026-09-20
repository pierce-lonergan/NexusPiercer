# Getting help with NexusPiercer

GitHub Discussions are **not** enabled on this repository, and blank issues are turned off. That
left no working route for anyone with a question, which is what this file fixes.

## Before you ask

Three documents answer most questions faster than an issue will:

- **"Will my data survive?"** → [`docs/ROUND_TRIP_FIDELITY.md`](docs/ROUND_TRIP_FIDELITY.md). Every
  shape the library is known to lose information on is listed there, per fixture, with the reason.
  It is generated from the corpus that runs on every build, so it cannot drift from the code.
- **"Why doesn't it do X?"** → the **"Still true today"** list in [`README.md`](README.md). These
  are the known limitations, stated plainly rather than buried.
- **"How do I install it without Maven Central?"** → [`docs/INSTALL.md`](docs/INSTALL.md), which
  documents three routes that need no Central access.

Note that the README describes some behaviour marked **2.1.0**, which is on `main` and not yet
published. If a type or option named there does not exist for you, check your version first —
`2.0.0` is the current release.

## Asking a usage question

Open a [bug report issue](https://github.com/pierce-lonergan/NexusPiercer/issues/new/choose) and
say in the first line that it is a question rather than a defect. That is not elegant, but it is
the only channel currently open, and a question in the tracker is better than a question nobody
can ask.

## Reporting a bug

Use the [bug report form](https://github.com/pierce-lonergan/NexusPiercer/issues/new/choose).

The single most useful thing you can include is **a minimal input that reproduces it** — a small
JSON document, or a small `.avsc` schema, plus the flattener or reconstructor options you used.
This project's whole fidelity contract is expressed as fixtures, so a reproducing input can often
become one directly, which is the fastest path from your report to a test that stops it recurring.

## Reporting a security vulnerability

**Do not open a public issue.** [`SECURITY.md`](SECURITY.md) has the private advisory link and the
contact address, along with the response times the project commits to for vulnerabilities.

## What to expect

NexusPiercer is maintained by one person. Issues and questions are handled on a best-effort basis
with no committed response time — the service levels in `SECURITY.md` apply to security reports
only and should not be read as applying here.

A question that turns out to be a published limitation will usually be answered with a link to the
fidelity document or the README list, and closed. That is not a brush-off: those documents are the
contract, and pointing at the specific row is the answer.
