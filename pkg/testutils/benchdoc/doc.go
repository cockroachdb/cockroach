// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package benchdoc

/*
# benchdoc — Benchmark CI Configuration

The benchdoc package allows you to configure how benchmarks are run and
compared in CI by adding a `benchmark-ci:` comment directly above your
benchmark function. A nogo linter validates the syntax of these comments
at build time.

## Usage

Add a comment line starting with `benchmark-ci:` to your benchmark
function's doc block. The comment contains comma-separated key=value
pairs:

	// benchmark-ci: count=10, benchtime=50x, timeout=20m
	func BenchmarkFoo(b *testing.B) { ... }

All parameters are optional. A benchmark function with no `benchmark-ci:`
comment uses the defaults and is included in the weekly suite.

## Run Arguments

These control how the benchmark is executed:

  - count — Number of times to run each benchmark (maps to `-count`).
  - benchtime — Duration or iteration count per run (maps to `-benchtime`),
    e.g. "5s" or "100x".
  - timeout — Maximum time for the benchmark run as a Go duration, e.g. "20m".
  - suite — Which CI suite to include this benchmark in. Values: "weekly"
    (default) or "manual". Manual benchmarks are only run on explicit request.

## Compare Arguments

These control how benchmark results are compared against previous runs:

  - post — Action to take when a regression is detected. Values:
    "none" (default, no issue filed), "notify" (file an informational
    issue), or "release-blocker" (file a release-blocking issue).
  - threshold — Regression sensitivity as a float in [0.0, 1.0]. A lower
    value means smaller regressions are flagged. For example, 0.3 flags
    regressions of 30% or more.

## Examples

Minimal configuration (weekly suite, defaults for everything):

	func BenchmarkSimple(b *testing.B) { ... }

Custom run parameters:

	// benchmark-ci: count=5, benchtime=100x, timeout=10m
	func BenchmarkCustomRun(b *testing.B) { ... }

File a release-blocker on regression:

	// benchmark-ci: post=release-blocker, threshold=0.2
	func BenchmarkCriticalPath(b *testing.B) { ... }

Multiline parameters are allowed:

	// benchmark-ci: count=5
	// benchmark-ci: benchtime=100x, timeout=10m
	func BenchmarkCustomRun(b *testing.B) { ... }

Manual-only benchmark:

	// benchmark-ci: suite=manual
	func BenchmarkExpensive(b *testing.B) { ... }

*/
