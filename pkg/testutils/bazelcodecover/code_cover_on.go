// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build bazel_code_cover

// Package bazelcodecover allows instrumented binaries to output code coverage
// data.
package bazelcodecover

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	_ "unsafe"

	"github.com/bazelbuild/rules_go/go/tools/coverdata"
)

// MaybeInitCodeCoverage sets up dumping of coverage counters on exit.
//
// BAZEL_COVER_DIR must be set; this is where a new .gocov coverage file will be
// created. To build Cockroach in this mode, use:
//
//	bazel build --collect_code_coverage --bazel_code_coverage //pkg/cmd/cockroach:cockroach
//
// The `--collect_code_coverage` flag enables Bazel-driven coverage
// instrumentation. The `--bazel_code_coverage` flag is an alias for
// `//build/toolchains:bazel_code_coverage_flag` which is conceptually similar
// to setting the `bazel_code_cover` build tag (it causes the build to use
// `code_cover_on.go`).
func MaybeInitCodeCoverage() {
	// Go 1.20 adds binary instrumentation for integration tests [1]. However, we
	// can't yet build in this mode through bazel (tracked in [2]).
	//
	// Instead, we hack into Bazel's coverage infrastructure and set up an exit
	// hook to dump the counters. The approach follows Go's implementation (see
	// [3]).
	//
	// [1] https://go.dev/testing/coverage/
	// [2] https://github.com/bazelbuild/rules_go/issues/3513
	// [3] https://github.com/golang/go/blob/dfbf809f2af753db69537a9431d6419142dfe80b/src/runtime/coverage/hooks.go

	dir := os.Getenv("BAZEL_COVER_DIR")
	if dir == "" {
		fmt.Fprintln(os.Stderr, "warning: BAZEL_COVER_DIR not set, no coverage data emitted")
		return
	}
	runtime_addExitHook(func() { emitCoverageCounters(dir) }, true /* runOnNonZeroExit */)
}

//go:linkname runtime_addExitHook runtime.addExitHook
func runtime_addExitHook(f func(), runOnNonZeroExit bool)

// emitCoverageCounters emits the counters to a new .gocov file in the given
// directory; any error is reported to stderr.
func emitCoverageCounters(dir string) {
	if err := emitCoverageCountersImpl(dir); err != nil {
		fmt.Fprintf(os.Stderr, "error emitting coverage data: %s\n", err)
	}
}

func emitCoverageCountersImpl(dir string) (err error) {
	filepath := filepath.Join(dir, fmt.Sprintf("cockroach-%012X.gocov", rand.Int63n(1<<48)))
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(file, "mode: set\n"); err != nil {
		file.Close()
		return err
	}

	for name, counts := range coverdata.Counters {
		if strings.HasPrefix(name, "external/") ||
			strings.HasPrefix(name, "bazel-out/") {
			continue
		}
		blocks := coverdata.Blocks[name]
		for i := range counts {
			count := atomic.LoadUint32(&counts[i]) // For -mode=atomic.
			_, err := fmt.Fprintf(file, "%s:%d.%d,%d.%d %d %d\n", name,
				blocks[i].Line0, blocks[i].Col0,
				blocks[i].Line1, blocks[i].Col1,
				blocks[i].Stmts,
				count)
			if err != nil {
				file.Close()
				return err
			}
		}
	}
	return file.Close()
}
