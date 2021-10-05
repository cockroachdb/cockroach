WARNING: The migration to Bazel at Cockroach is still in-progress.
Proceed at your own risk :)

`dev` is a general-purpose tool for folks working on `cockroach`. Unlike
builds and tests performed w/ `make`, everything `dev` does is driven by
Bazel as a build system.

You can use the script `./dev` at top-level instead of building `dev`
ahead of time. This script just builds `dev` and immediately runs it
with the supplied arguments.

Run `./dev help` to see what `dev` can do!