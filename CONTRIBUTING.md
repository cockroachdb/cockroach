# Contributing to CockroachDB

## Getting Started

1. Find something to work on: [good first issues](https://github.com/cockroachdb/cockroach/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22)
2. [Build from source](docs/building.md)
3. Read the [style guide](docs/style.md)

**Trivial changes:** We typically do not accept trivial spelling/wording
changes to comments or error messages unless they are part of a larger
change, as they can cause merge conflicts in ongoing development.

## Submitting a PR

1. Sign the [Contributor License Agreement](https://cla.crdb.dev/cockroachdb/cockroach).
2. Create a feature branch and make your changes.
3. Write tests. CockroachDB uses several test frameworks:
   - **Go unit tests** (`TestXXX` in the same package) — most common.
   - **Data-driven tests** via [`datadriven.RunTest`](https://github.com/cockroachdb/datadriven), with files in `testdata/`.
   - **SQL logic tests** via `logictest.RunLogicTest` — for SQL behavior changes.
   - **Roachtests** in `pkg/cmd/roachtest` — for tests needing `cockroach start` or long runtimes.
4. Format with `crlfmt -w -tab 2 <file>.go`.
5. Run `./dev generate && ./dev lint --short && ./dev test pkg/your/package`.
6. Follow the [commit message guidelines](docs/commits-and-prs.md):
   commit message is the primary record, not the PR description.
   Include release note annotations.
7. Push and [create a PR](https://help.github.com/articles/creating-a-pull-request).
8. Merge via `/trunk merge` (not the green button).

## Code Review

As a reviewer:
- Respond within a few business hours for small PRs, 24 hours for larger ones.
- Discuss design before details. If you foresee multiple passes, say so upfront.
- Check [release notes](docs/commits-and-prs.md): user-facing changes
  must be mentioned, backward-incompatible changes highlighted.
- Prefix minor style suggestions with "nit:".

As an author:
- Expect comments on test coverage, [Go idioms](docs/style.md), and nits.
- Read the style guide before your first PR.

## Other Resources

- [Building from source](docs/building.md)
- [Go style guide](docs/style.md)
- [Commit and PR guidelines](docs/commits-and-prs.md)
- [Backporting](docs/backporting.md)
- [Configuration UX guidelines for cluster settings, flags and knobs](docs/configuration.md)
- [Architecture overview](https://www.cockroachlabs.com/docs/stable/architecture/overview.html)

## Community

Join our [Community Slack](https://go.crdb.dev/p/slack) (#contributors channel)
to ask questions or connect with other contributors.

## Code of Conduct

Please read and follow our
[Code of Conduct](https://github.com/cockroachdb/code-of-conduct).
