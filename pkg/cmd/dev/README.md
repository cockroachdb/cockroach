`dev` is the general-purpose dev tool for working on cockroachdb/cockroach; it's
powered by Bazel underneath the hood. With it you can:

- build various binaries (cockroach, roachprod, optgen, ...)
- run arbitrary tests (unit tests, logic tests, ...) under various configurations (stress, race, ...)
- generate code (bazel files, docs, protos, ...)

You can use the top-level `./dev` script instead of building a `dev` binary
ahead of time. This script just does it for you whatever version is checked out
and immediately runs it with the supplied arguments. Run `dev --help` to see
what it can do!