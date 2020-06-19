// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build lint

package lint

import (
	"bufio"
	"bytes"
	"fmt"
	"go/build"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	_ "github.com/cockroachdb/cockroach/pkg/testutils/buildutil"
	"github.com/cockroachdb/errors"
	"github.com/ghemawat/stream"
	"github.com/jordanlewis/gcassert"
	"golang.org/x/tools/go/packages"
)

const cockroachDB = "github.com/cockroachdb/cockroach"

func dirCmd(
	dir string, name string, args ...string,
) (*exec.Cmd, *bytes.Buffer, stream.Filter, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, err
	}
	stderr := new(bytes.Buffer)
	cmd.Stderr = stderr
	return cmd, stderr, stream.ReadLines(stdout), nil
}

// vetCmd executes commands like dirCmd, but stderr is used as the output
// instead of stdout, as produced by programs like `go vet`.
func vetCmd(t *testing.T, dir, name string, args []string, filters []stream.Filter) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	var b bytes.Buffer
	cmd.Stdout = &b
	cmd.Stderr = &b
	switch err := cmd.Run(); err.(type) {
	case nil:
	case *exec.ExitError:
		// Non-zero exit is expected.
	default:
		t.Fatal(err)
	}
	filters = append([]stream.Filter{
		stream.FilterFunc(func(arg stream.Arg) error {
			scanner := bufio.NewScanner(&b)
			for scanner.Scan() {
				if s := scanner.Text(); strings.TrimSpace(s) != "" {
					arg.Out <- s
				}
			}
			return scanner.Err()
		})}, filters...)

	var msgs strings.Builder
	if err := stream.ForEach(stream.Sequence(filters...), func(s string) {
		fmt.Fprintln(&msgs, s)
	}); err != nil {
		t.Error(err)
	}
	if msgs.Len() > 0 {
		t.Errorf("\n%s", strings.ReplaceAll(msgs.String(), "\\n++", "\n"))
	}
}

// TestLint runs a suite of linters on the codebase. This file is
// organized into two sections. First are the global linters, which
// run on the entire repo every time. Second are the package-scoped
// linters, which can be restricted to a specific package with the PKG
// makefile variable. Linters that require anything more than a `git
// grep` should preferably be added to the latter group (and within
// that group, adding to Megacheck is better than creating a new
// test).
//
// Linters may be skipped for two reasons: The "short" flag (i.e.
// `make lintshort`), which skips the most expensive linters (more for
// memory than for CPU usage), and the PKG variable. Some linters in
// the global group may be skipped if the PKG flag is set regardless
// of the short flag since they cannot be restricted to the package.
// It should be reasonable to run `make lintshort` and `make lint
// PKG=some/modified/pkg` locally and rely on CI for the more
// expensive linters.
//
// Linters which run in a single process without internal
// parallelization, and which have reasonable memory consumption
// should be marked with t.Parallel(). As a rule of thumb anything
// that requires type-checking the go code needs too much memory to
// parallelize here (although it's fine for such tests to run multiple
// goroutines internally using a shared loader object).
//
// Performance notes: This needs a lot of memory and CPU time. As of
// 2018-07-13, the largest consumers of memory are
// TestMegacheck/staticcheck (9GB) and TestUnused (6GB). Memory
// consumption of staticcheck could be reduced by running it on a
// subset of the packages at a time, although this comes at the
// expense of increased running time.
func TestLint(t *testing.T) {
	crdb, err := build.Import(cockroachDB, "", build.FindOnly)
	if err != nil {
		t.Fatal(err)
	}
	pkgDir := filepath.Join(crdb.Dir, "pkg")

	pkgVar, pkgSpecified := os.LookupEnv("PKG")

	t.Run("TestLowercaseFunctionNames", func(t *testing.T) {
		t.Parallel()
		reSkipCasedFunction, err := regexp.Compile(`^(Binary file.*|[^:]+:\d+:(` +
			`query error .*` + // OK when in logic tests
			`|` +
			`\s*(//|#).*` + // OK when mentioned in comment
			`|` +
			`.*lint: uppercase function OK` + // linter annotation at end of line
			`))$`)
		if err != nil {
			t.Fatal(err)
		}

		var names []string
		for _, name := range builtins.AllBuiltinNames {
			switch name {
			case "extract", "trim", "overlay", "position", "substring", "st_x", "st_y":
				// Exempt special forms: EXTRACT(... FROM ...), etc.
			default:
				names = append(names, strings.ToUpper(name))
			}
		}

		cmd, stderr, filter, err := dirCmd(crdb.Dir,
			"git", "grep", "-nE", fmt.Sprintf(`[^_a-zA-Z](%s)\(`, strings.Join(names, "|")),
			"--", "pkg")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			if reSkipCasedFunction.MatchString(s) {
				// OK when mentioned in comment or lint disabled.
				return
			}
			if strings.Contains(s, "FAMILY"+"(") {
				t.Errorf("\n%s <- forbidden; use \"FAMILY (\" (with space) or "+
					"lowercase \"family(\" for the built-in function", s)
			} else {
				t.Errorf("\n%s <- forbidden; use lowercase for SQL built-in functions", s)
			}
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestCopyrightHeaders", func(t *testing.T) {
		t.Parallel()

		bslHeader := regexp.MustCompile(`// Copyright 20\d\d The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
`)

		cclHeader := regexp.MustCompile(`// Copyright 20\d\d The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License \(the "License"\); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt
`)

		// These extensions identify source files that should have copyright headers.
		extensions := []string{
			"*.go", "*.cc", "*.h", "*.js", "*.ts", "*.tsx", "*.s", "*.S", "*.styl", "*.proto", "*.rl",
		}

		cmd, stderr, filter, err := dirCmd(pkgDir, "git", append([]string{"ls-files"}, extensions...)...)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(filter,
			stream.GrepNot(`\.pb\.go`),
			stream.GrepNot(`\.pb\.gw\.go`),
			stream.GrepNot(`\.og\.go`),
			stream.GrepNot(`\.eg\.go`),
			stream.GrepNot(`_string\.go`),
			stream.GrepNot(`_generated\.go`),
			stream.GrepNot(`/embedded.go`),
			stream.GrepNot(`geo/geographiclib/geodesic\.c$`),
			stream.GrepNot(`geo/geographiclib/geodesic\.h$`),
		), func(filename string) {
			isCCL := strings.Contains(filename, "ccl/")
			var expHeader *regexp.Regexp
			if isCCL {
				expHeader = cclHeader
			} else {
				expHeader = bslHeader
			}

			file, err := os.Open(filepath.Join(pkgDir, filename))
			if err != nil {
				t.Error(err)
				return
			}
			defer file.Close()
			data := make([]byte, 1024)
			n, err := file.Read(data)
			if err != nil {
				t.Errorf("reading start of %s: %s", filename, err)
			}
			data = data[0:n]

			if expHeader.Find(data) == nil {
				t.Errorf("did not find expected license header (ccl=%v) in %s", isCCL, filename)
			}
		}); err != nil {
			t.Fatal(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestMissingLeakTest", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "util/leaktest/check-leaktest.sh")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestNoContextTODOInTests", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`context.TODO\(\)`,
			"--",
			"*_test.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use context.Background() in tests.", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestTabsInShellScripts", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", "^ *\t", "--", "*.sh")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf(`%s <- tab detected, use spaces instead`, s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestOptfmt", func(t *testing.T) {
		t.Parallel()
		if pkgSpecified {
			t.Skip("PKG specified")
		}

		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "ls-files", "*.opt", ":!*/testdata/*")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if err := stream.ForEach(
			stream.Sequence(
				filter,
				stream.Map(func(s string) string {
					return filepath.Join(pkgDir, s)
				}),
				stream.Xargs("optfmt", "-l"),
			), func(s string) {
				fmt.Fprintln(&buf, s)
			}); err != nil {
			t.Error(err)
		}
		errs := buf.String()
		if len(errs) > 0 {
			t.Errorf("\n%s", errs)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestHttputil", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			re       string
			excludes []string
		}{
			{re: `\bhttp\.(Get|Put|Head)\(`},
		} {
			cmd, stderr, filter, err := dirCmd(
				pkgDir,
				"git",
				append([]string{
					"grep",
					"-nE",
					tc.re,
					"--",
					"*.go",
				}, tc.excludes...)...,
			)
			if err != nil {
				t.Fatal(err)
			}

			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}

			if err := stream.ForEach(filter, func(s string) {
				t.Errorf("\n%s <- forbidden; use 'httputil' instead", s)
			}); err != nil {
				t.Error(err)
			}

			if err := cmd.Wait(); err != nil {
				if out := stderr.String(); len(out) > 0 {
					t.Fatalf("err=%s, stderr=%s", err, out)
				}
			}
		}
	})

	t.Run("TestEnvutil", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			re       string
			excludes []string
		}{
			{re: `\bos\.(Getenv|LookupEnv)\("COCKROACH`},
			{
				re: `\bos\.(Getenv|LookupEnv)\(`,
				excludes: []string{
					":!acceptance",
					":!ccl/acceptanceccl/backup_test.go",
					":!ccl/backupccl/backup_cloud_test.go",
					":!storage/cloudimpl",
					":!ccl/workloadccl/fixture_test.go",
					":!cmd",
					":!nightly",
					":!testutils/lint",
					":!util/envutil/env.go",
					":!util/log/tracebacks.go",
					":!util/sdnotify/sdnotify_unix.go",
				},
			},
		} {
			cmd, stderr, filter, err := dirCmd(
				pkgDir,
				"git",
				append([]string{
					"grep",
					"-nE",
					tc.re,
					"--",
					"*.go",
				}, tc.excludes...)...,
			)
			if err != nil {
				t.Fatal(err)
			}

			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}

			if err := stream.ForEach(filter, func(s string) {
				t.Errorf("\n%s <- forbidden; use 'envutil' instead", s)
			}); err != nil {
				t.Error(err)
			}

			if err := cmd.Wait(); err != nil {
				if out := stderr.String(); len(out) > 0 {
					t.Fatalf("err=%s, stderr=%s", err, out)
				}
			}
		}
	})

	t.Run("TestSyncutil", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\bsync\.(RW)?Mutex`,
			"--",
			"*.go",
			":!*/doc.go",
			":!util/syncutil/mutex_sync.go",
			":!util/syncutil/mutex_sync_race.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'syncutil.{,RW}Mutex' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestSQLTelemetryDirectCount", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`[^[:alnum:]]telemetry\.Count\(`,
			"--",
			"sql",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'sqltelemetry.xxxCounter()' / `telemetry.Inc' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestSQLTelemetryGetCounter", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`[^[:alnum:]]telemetry\.GetCounter`,
			"--",
			"sql",
			":!sql/sqltelemetry",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'sqltelemetry.xxxCounter() instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestCBOPanics", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			fmt.Sprintf(`[^[:alnum:]]panic\((%s|"|[a-z]+Error\{errors\.(New|Errorf)|fmt\.Errorf)`, "`"),
			"--",
			"sql/opt",
			":!sql/opt/optgen",
			":!sql/opt/testutils",
			":!*.pb.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use panic(errors.AssertionFailedf()) instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestInternalErrorCodes", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`[^[:alnum:]]pgerror\.(NewError|Wrap).*pgerror\.CodeInternalError`,
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use errors.AssertionFailedf() instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestTodoStyle", func(t *testing.T) {
		t.Parallel()
		// TODO(tamird): enforce presence of name.
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", `\sTODO\([^)]+\)[^:]`, "--", "*.go")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf(`%s <- use 'TODO(...): ' instead`, s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestNonZeroOffsetInTests", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", `hlc\.NewClock\([^)]+, 0\)`, "--", "*_test.go")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf(`%s <- use non-zero clock offset`, s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestTimeutil", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\btime\.(Now|Since|Unix)\(`,
			"--",
			"*.go",
			":!**/embedded.go",
			":!util/timeutil/time.go",
			":!util/timeutil/now_unix.go",
			":!util/timeutil/now_windows.go",
			":!util/tracing/tracer_span.go",
			":!util/tracing/tracer.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'timeutil' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestContext", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\bcontext\.With(Deadline|Timeout)\(`,
			"--",
			"*.go",
			":!util/contextutil/context.go",
			// TODO(jordan): ban these too?
			":!server/debug/**",
			":!workload/**",
			":!*_test.go",
			":!cli/debug_synctest.go",
			":!cmd/**",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'contextutil.RunWithTimeout' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestGrpc", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\bgrpc\.NewServer\(`,
			"--",
			"*.go",
			":!rpc/context_test.go",
			":!rpc/context.go",
			":!rpc/nodedialer/nodedialer_test.go",
			":!util/grpcutil/grpc_util_test.go",
			":!cli/systembench/network_test_server.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'rpc.NewServer' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestProtoClone", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`proto\.Clone\([^)]`,
			"--",
			"*.go",
			":!util/protoutil/clone_test.go",
			":!util/protoutil/clone.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`protoutil\.Clone\(`),
		), func(s string) {
			t.Errorf("\n%s <- forbidden; use 'protoutil.Clone' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestTParallel", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\.Parallel\(\)`,
			"--",
			"*.go",
			":!testutils/lint/*.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`// SAFE FOR TESTING`),
		), func(s string) {
			t.Errorf("\n%s <- forbidden, use a sync.WaitGroup instead (cf https://github.com/golang/go/issues/31651)", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestProtoMarshal", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\.Marshal\(`,
			"--",
			"*.go",
			":!sql/*.pb.go",
			":!util/protoutil/marshal.go",
			":!util/protoutil/marshaler.go",
			":!settings/settings_test.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`(json|yaml|protoutil|xml|\.Field|ewkb|wkb|wkt)\.Marshal\(`),
		), func(s string) {
			t.Errorf("\n%s <- forbidden; use 'protoutil.Marshal' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestProtoUnmarshal", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\.Unmarshal\(`,
			"--",
			"*.go",
			":!*.pb.go",
			":!util/protoutil/marshal.go",
			":!util/protoutil/marshaler.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`(json|jsonpb|yaml|xml|protoutil|toml|Codec|ewkb|wkb)\.Unmarshal\(`),
		), func(s string) {
			t.Errorf("\n%s <- forbidden; use 'protoutil.Unmarshal' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestProtoMessage", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nEw",
			`proto\.Message`,
			"--",
			"*.go",
			":!*.pb.go",
			":!*.pb.gw.go",
			":!sql/pgwire/pgerror/severity.go",
			":!sql/pgwire/pgerror/with_candidate_code.go",
			":!sql/colexecbase/colexecerror/error.go",
			":!util/protoutil/jsonpb_marshal.go",
			":!util/protoutil/marshal.go",
			":!util/protoutil/marshaler.go",
			":!util/tracing/tracer_span.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
		), func(s string) {
			t.Errorf("\n%s <- forbidden; use 'protoutil.Message' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestYaml", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", `\byaml\.Unmarshal\(`, "--", "*.go")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'yaml.UnmarshalStrict' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestImportNames", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", `^(import|\s+)(\w+ )?"database/sql"$`, "--", "*.go")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`gosql "database/sql"`),
		), func(s string) {
			t.Errorf("\n%s <- forbidden; import 'database/sql' as 'gosql' to avoid confusion with 'cockroach/sql'", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestMisspell", func(t *testing.T) {
		t.Parallel()
		if pkgSpecified {
			t.Skip("PKG specified")
		}
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "ls-files")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		ignoredRules := []string{
			"licence",
			"mitre",   // PostGIS commands spell these as mitre.
			"analyse", // required by SQL grammar
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`.*\.lock`),
			stream.GrepNot(`^storage\/rocksdb_error_dict\.go$`),
			stream.GrepNot(`^workload/tpcds/tpcds.go$`),
			stream.GrepNot(`^geo/geoprojbase/projections.go$`),
			stream.GrepNot(`^sql/logictest/testdata/logic_test/pg_extension$`),
			stream.Map(func(s string) string {
				return filepath.Join(pkgDir, s)
			}),
			stream.Xargs("misspell", "-locale", "US", "-i", strings.Join(ignoredRules, ",")),
		), func(s string) {
			t.Errorf("\n%s", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestGofmtSimplify", func(t *testing.T) {
		t.Parallel()
		if pkgSpecified {
			t.Skip("PKG specified")
		}

		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "ls-files", "*.go", ":!*/testdata/*", ":!*_generated.go")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if err := stream.ForEach(
			stream.Sequence(
				filter,
				stream.Map(func(s string) string {
					return filepath.Join(pkgDir, s)
				}),
				stream.Xargs("gofmt", "-s", "-d", "-l"),
			), func(s string) {
				fmt.Fprintln(&buf, s)
			}); err != nil {
			t.Error(err)
		}
		errs := buf.String()
		if len(errs) > 0 {
			t.Errorf("\n%s", errs)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestCrlfmt", func(t *testing.T) {
		t.Parallel()
		if pkgSpecified {
			t.Skip("PKG specified")
		}
		ignore := `\.(pb(\.gw)?)|(\.[eo]g)\.go|/testdata/|^sql/parser/sql\.go$|_generated\.go$`
		cmd, stderr, filter, err := dirCmd(pkgDir, "crlfmt", "-fast", "-ignore", ignore, "-tab", "2", ".")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if err := stream.ForEach(filter, func(s string) {
			fmt.Fprintln(&buf, s)
		}); err != nil {
			t.Error(err)
		}
		errs := buf.String()
		if len(errs) > 0 {
			t.Errorf("\n%s", errs)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}

		if t.Failed() {
			args := append([]string(nil), cmd.Args[1:len(cmd.Args)-1]...)
			args = append(args, "-w", pkgDir)
			for i := range args {
				args[i] = strconv.Quote(args[i])
			}
			t.Logf("run the following to fix your formatting:\n"+
				"\nbin/crlfmt %s\n\n"+
				"Don't forget to add amend the result to the correct commits.",
				strings.Join(args, " "),
			)
		}
	})

	t.Run("TestAuthorTags", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-lE", "^// Author:")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- please remove the Author comment within", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	// Things that are packaged scoped are below here.
	pkgScope := pkgVar
	if !pkgSpecified {
		pkgScope = "./pkg/..."
	}

	t.Run("TestForbiddenImports", func(t *testing.T) {
		t.Parallel()

		// forbiddenImportPkg -> permittedReplacementPkg
		forbiddenImports := map[string]string{
			"golang.org/x/net/context":                    "context",
			"log":                                         "util/log",
			"github.com/golang/protobuf/proto":            "github.com/gogo/protobuf/proto",
			"github.com/satori/go.uuid":                   "util/uuid",
			"golang.org/x/sync/singleflight":              "github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight",
			"syscall":                                     "sysutil",
			"errors":                                      "github.com/cockroachdb/errors",
			"github.com/pkg/errors":                       "github.com/cockroachdb/errors",
			"github.com/cockroachdb/errors/assert":        "github.com/cockroachdb/errors",
			"github.com/cockroachdb/errors/barriers":      "github.com/cockroachdb/errors",
			"github.com/cockroachdb/errors/contexttags":   "github.com/cockroachdb/errors",
			"github.com/cockroachdb/errors/domains":       "github.com/cockroachdb/errors",
			"github.com/cockroachdb/errors/errbase":       "github.com/cockroachdb/errors",
			"github.com/cockroachdb/errors/errutil":       "github.com/cockroachdb/errors",
			"github.com/cockroachdb/errors/issuelink":     "github.com/cockroachdb/errors",
			"github.com/cockroachdb/errors/markers":       "github.com/cockroachdb/errors",
			"github.com/cockroachdb/errors/report":        "github.com/cockroachdb/errors",
			"github.com/cockroachdb/errors/safedetails":   "github.com/cockroachdb/errors",
			"github.com/cockroachdb/errors/secondary":     "github.com/cockroachdb/errors",
			"github.com/cockroachdb/errors/telemetrykeys": "github.com/cockroachdb/errors",
			"github.com/cockroachdb/errors/withstack":     "github.com/cockroachdb/errors",
		}

		// grepBuf creates a grep string that matches any forbidden import pkgs.
		var grepBuf bytes.Buffer
		grepBuf.WriteByte('(')
		for forbiddenPkg := range forbiddenImports {
			grepBuf.WriteByte('|')
			grepBuf.WriteString(regexp.QuoteMeta(forbiddenPkg))
		}
		grepBuf.WriteString(")$")

		filter := stream.FilterFunc(func(arg stream.Arg) error {
			for _, useAllFiles := range []bool{false, true} {
				buildContext := build.Default
				buildContext.CgoEnabled = true
				buildContext.UseAllFiles = useAllFiles

				pkgPath := filepath.Join(cockroachDB, pkgScope)
				pkgs, err := packages.Load(
					&packages.Config{
						Mode: packages.NeedImports | packages.NeedName,
					},
					pkgPath,
				)
				if err != nil {
					return errors.Wrapf(err, "error loading package %s", pkgPath)
				}
				for _, pkg := range pkgs {
					for _, s := range pkg.Imports {
						arg.Out <- pkg.PkgPath + ": " + s.PkgPath
					}
				}
			}
			return nil
		})
		settingsPkgPrefix := `github.com/cockroachdb/cockroach/pkg/settings`
		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.Sort(),
			stream.Uniq(),
			stream.Grep(`^`+settingsPkgPrefix+`: | `+grepBuf.String()),
			stream.GrepNot(`cockroach/pkg/cmd/`),
			stream.GrepNot(`cockroach/pkg/testutils/lint: log$`),
			stream.GrepNot(`cockroach/pkg/util/sysutil: syscall$`),
			stream.GrepNot(`cockroach/pkg/util/log: github\.com/pkg/errors$`),
			stream.GrepNot(`cockroach/pkg/(base|release|security|util/(log|randutil|stop)): log$`),
			stream.GrepNot(`cockroach/pkg/(server/serverpb|ts/tspb): github\.com/golang/protobuf/proto$`),

			stream.GrepNot(`cockroach/pkg/util/uuid: github\.com/satori/go\.uuid$`),
		), func(s string) {
			pkgStr := strings.Split(s, ": ")
			importingPkg, importedPkg := pkgStr[0], pkgStr[1]

			// Test that a disallowed package is not imported.
			if replPkg, ok := forbiddenImports[importedPkg]; ok {
				t.Errorf("\n%s <- please use %q instead of %q", s, replPkg, importedPkg)
			}

			// Test that the settings package does not import CRDB dependencies.
			if importingPkg == settingsPkgPrefix && strings.HasPrefix(importedPkg, cockroachDB) {
				switch {
				case strings.HasSuffix(s, "humanizeutil"):
				case strings.HasSuffix(s, "protoutil"):
				case strings.HasSuffix(s, "testutils"):
				case strings.HasSuffix(s, "syncutil"):
				case strings.HasSuffix(s, settingsPkgPrefix):
				default:
					t.Errorf("%s <- please don't add CRDB dependencies to settings pkg", s)
				}
			}
		}); err != nil {
			t.Error(err)
		}
	})

	// TODO(tamird): replace this with errcheck.NewChecker() when
	// https://github.com/dominikh/go-tools/issues/57 is fixed.
	t.Run("TestErrCheck", func(t *testing.T) {
		if testing.Short() {
			t.Skip("short flag")
		}
		excludesPath, err := filepath.Abs(filepath.Join("testdata", "errcheck_excludes.txt"))
		if err != nil {
			t.Fatal(err)
		}
		// errcheck uses 2GB of ram (as of 2017-07-13), so don't parallelize it.
		cmd, stderr, filter, err := dirCmd(
			crdb.Dir,
			"errcheck",
			"-exclude",
			excludesPath,
			pkgScope,
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("%s <- unchecked error", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestReturnCheck", func(t *testing.T) {
		if testing.Short() {
			t.Skip("short flag")
		}
		// returncheck uses 2GB of ram (as of 2017-07-13), so don't parallelize it.
		cmd, stderr, filter, err := dirCmd(crdb.Dir, "returncheck", pkgScope)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("%s <- unchecked error", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestGolint", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(crdb.Dir, "golint", pkgScope)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(
			stream.Sequence(
				filter,
				stream.GrepNot("sql/.*exported func .* returns unexported type sql.planNode"),
				stream.GrepNot("pkg/sql/types/types.go.* var Uuid should be UUID"),
				stream.GrepNot("pkg/sql/oidext/oidext.go.*don't use underscores in Go names; const T_"),
			), func(s string) {
				t.Errorf("\n%s", s)
			}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestStaticCheck", func(t *testing.T) {
		// staticcheck uses 2.4GB of ram (as of 2019-05-10), so don't parallelize it.
		if testing.Short() {
			t.Skip("short flag")
		}
		cmd, stderr, filter, err := dirCmd(
			crdb.Dir,
			"staticcheck",
			"-unused.whole-program",
			pkgScope,
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(
			stream.Sequence(
				filter,
				// Skip .pb.go and .pb.gw.go generated files.
				stream.GrepNot(`pkg/.*\.pb(\.gw|)\.go:`),
				// Skip generated file.
				stream.GrepNot(`pkg/ui/distoss/bindata.go`),
				stream.GrepNot(`pkg/ui/distccl/bindata.go`),
				// sql.go is the generated parser, which sets sqlDollar in all cases,
				// even if it might not be used again.
				stream.GrepNot(`pkg/sql/parser/sql.go:.*this value of sqlDollar is never used`),
				// Generated file containing many unused postgres error codes.
				stream.GrepNot(`pkg/sql/pgwire/pgcode/codes.go:.* var .* is unused`),
				// The methods in exprgen.customFuncs are used via reflection.
				stream.GrepNot(`pkg/sql/opt/optgen/exprgen/custom_funcs.go:.* func .* is unused`),
				// Using deprecated method to COPY.
				stream.GrepNot(`pkg/cli/nodelocal.go:.* stmt.Exec is deprecated: .*`),
				// Cause is a method used by pkg/cockroachdb/errors (through an unnamed
				// interface).
				stream.GrepNot(`pkg/.*.go:.* func .*\.Cause is unused`),
				// Using deprecated WireLength call.
				stream.GrepNot(`pkg/rpc/stats_handler.go:.*v.WireLength is deprecated: This field is never set.*`),
			), func(s string) {
				t.Errorf("\n%s", s)
			}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestVectorizedPanics", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			fmt.Sprintf(`panic\(.*\)`),
			"--",
			// NOTE: if you're adding a new package to the list here because it
			// uses "panic-catch" error propagation mechanism of the vectorized
			// engine, don't forget to "register" the newly added package in
			// sql/colexecbase/colexecerror/error.go file.
			"sql/col*",
			":!sql/colexecbase/colexecerror/error.go",
			":!sql/colexec/execpb/stats.pb.go",
			":!sql/colflow/vectorized_panic_propagation_test.go",
			// This exception is because execgen itself uses panics during code
			// generation - not at execution time. The (glob,exclude) directive
			// (see git help gitglossary) makes * behave like a normal, single dir
			// glob, and exclude is the synonym of !.
			":(glob,exclude)sql/colexec/execgen/*.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use either colexecerror.InternalError() or colexecerror.ExpectedError() instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestVectorizedAllocator", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			// We prohibit usage of:
			// - coldata.NewMemBatch
			// - coldata.NewMemBatchWithSize
			// - coldata.NewMemColumn
			// - coldata.Batch.AppendCol
			// TODO(yuzefovich): prohibit call to coldata.NewMemBatchNoCols.
			fmt.Sprintf(`(coldata\.NewMem(Batch|BatchWithSize|Column)|\.AppendCol)\(`),
			"--",
			// TODO(yuzefovich): prohibit calling coldata.* methods from other
			// sql/col* packages.
			"sql/colexec",
			"sql/colflow",
			":!sql/colexec/simple_project.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use colmem.Allocator object instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestVectorizedAppendColumn", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			// We prohibit usage of Allocator.MaybeAppendColumn outside of
			// vectorTypeEnforcer and batchSchemaPrefixEnforcer.
			fmt.Sprintf(`(MaybeAppendColumn)\(`),
			"--",
			"sql/col*",
			":!sql/colexec/operator.go",
			":!sql/colmem/allocator.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use colexec.vectorTypeEnforcer "+
				"or colexec.batchSchemaPrefixEnforcer", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestVectorizedTypeSchemaCopy", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			// We prohibit appending to the type schema and require allocating
			// a new slice. See the comment in execplan.go file.
			fmt.Sprintf(`(yps|ypes) = append\(`),
			"--",
			"sql/colexec/execplan.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; allocate a new []*types.T slice", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestGCAssert", func(t *testing.T) {
		t.Parallel()
		var buf strings.Builder
		if err := gcassert.GCAssert("../../sql/colexec", &buf); err != nil {
			t.Fatal(err)
		}
		output := buf.String()
		if len(output) > 0 {
			t.Fatalf("failed gcassert:\n%s", output)
		}
	})

	t.Run("TestTypesSlice", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			fmt.Sprintf(`\[\]types.T`),
			"--",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use []*types.T", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	// RoachVet is expensive memory-wise and thus should not run with t.Parallel().
	// RoachVet includes all of the passes of `go vet` plus first-party additions.
	// See pkg/cmd/roachvet.
	t.Run("TestRoachVet", func(t *testing.T) {
		if testing.Short() {
			t.Skip("short flag")
		}
		// The -printfuncs functionality is interesting and
		// under-documented. It checks two things:
		//
		// - that functions that accept formatting specifiers are given
		//   arguments of the proper type.
		// - that functions that don't accept formatting specifiers
		//   are not given any.
		//
		// Whether a function takes a format string or not is determined
		// as follows: (comment taken from the source of `go vet`)
		//
		//    A function may be a Printf or Print wrapper if its last argument is ...interface{}.
		//    If the next-to-last argument is a string, then this may be a Printf wrapper.
		//    Otherwise it may be a Print wrapper.
		printfuncs := strings.Join([]string{
			"ErrEvent",
			"ErrEventf",
			"Error",
			"Errorf",
			"ErrorfDepth",
			"Event",
			"Eventf",
			"Fatal",
			"Fatalf",
			"FatalfDepth",
			"Info",
			"Infof",
			"InfofDepth",
			"AssertionFailedf",
			"AssertionFailedWithDepthf",
			"NewAssertionErrorWithWrappedErrf",
			"DangerousStatementf",
			"pgerror.New",
			"pgerror.NewWithDepthf",
			"pgerror.Newf",
			"SetDetailf",
			"SetHintf",
			"Unimplemented",
			"Unimplementedf",
			"UnimplementedWithDepthf",
			"UnimplementedWithIssueDetailf",
			"UnimplementedWithIssuef",
			"VEvent",
			"VEventf",
			"Warning",
			"Warningf",
			"WarningfDepth",
			"Wrapf",
			"WrapWithDepthf",
			"redact.Fprint",
			"redact.Fprintf",
			"redact.Sprint",
			"redact.Sprintf",
		}, ",")

		filters := []stream.Filter{
			// Ignore generated files.
			stream.GrepNot(`pkg/.*\.pb\.go:`),
			stream.GrepNot(`pkg/col/coldata/.*\.eg\.go:`),
			stream.GrepNot(`pkg/col/colserde/arrowserde/.*_generated\.go:`),
			stream.GrepNot(`pkg/sql/colexec/.*\.eg\.go:`),
			stream.GrepNot(`pkg/sql/colexec/.*_generated\.go:`),
			stream.GrepNot(`pkg/sql/pgwire/hba/conf.go:`),

			// Ignore types that can change by system.
			stream.GrepNot(`pkg/util/sysutil/sysutil_unix.go:`),

			stream.GrepNot(`declaration of "?(pE|e)rr"? shadows`),
			stream.GrepNot(`\.pb\.gw\.go:[0-9:]+: declaration of "?ctx"? shadows`),
			stream.GrepNot(`\.[eo]g\.go:[0-9:]+: declaration of ".*" shadows`),
			// This exception is for hash.go, which re-implements runtime.noescape
			// for efficient hashing.
			stream.GrepNot(`pkg/sql/colexec/hash.go:[0-9:]+: possible misuse of unsafe.Pointer`),
			stream.GrepNot(`^#`), // comment line
			// This exception is for the colexec generated files.
			stream.GrepNot(`pkg/sql/colexec/.*\.eg.go:[0-9:]+: self-assignment of .* to .*`),
			// Roachpb generated switch on `error`. It's OK for now because
			// the inner error is always unwrapped (it's a protobuf
			// enum). Eventually we want to use generalized error
			// encode/decode instead and drop the linter exception.
			stream.GrepNot(`pkg/roachpb/batch_generated\.go:.*invalid direct cast on error object`),
			// Roachpb's own error package takes ownership of error unwraps
			// (by enforcing that errors can never been wrapped under a
			// roachpb.Error, which is an inconvenient limitation but it is
			// what it is). Once this code is simplified to use generalized
			// error encode/decode, it can be dropped from the linter
			// exception as well.
			stream.GrepNot(`pkg/roachpb/errors\.go:.*invalid direct cast on error object`),
			// pgerror's pgcode logic uses its own custom cause recursion
			// algorithm and thus cannot use errors.If() which mandates a
			// different recursion order.
			//
			// It's a bit unfortunate that the entire file is added
			// as an exception here, given that only one function
			// really needs the linter. We could consider splitting
			// that function to a different file to limit the scope
			// of the exception.
			stream.GrepNot(`pkg/sql/pgwire/pgerror/pgcode\.go:.*invalid direct cast on error object`),
			// The crash reporting code uses its own custom cause recursion
			// algorithm and thus cannot use errors.Is.  However, it's also
			// due an overhaul - it's really redundant with the error
			// redaction code already present in the errors library.
			//
			// TODO(knz): remove the code in log and replace by the errors'
			// own redact code.
			stream.GrepNot(`pkg/util/log/crash_reporting\.go:.*invalid direct cast on error object`),
			stream.GrepNot(`pkg/util/log/crash_reporting\.go:.*invalid direct comparison of error object`),
			// The logging package translates log.Fatal calls into errors.
			// We can't use the regular exception mechanism via functions.go
			// because addStructured takes its positional argument as []interface{},
			// instead of ...interface{}.
			stream.GrepNot(`pkg/util/log/structured\.go:\d+:\d+: addStructured\(\): format argument is not a constant expression`),
			// roachtest is not collecting redactable logs so we don't care
			// about printf hygiene there as much.
			stream.GrepNot(`pkg/cmd/roachtest/log\.go:.*format argument is not a constant expression`),
		}

		roachlint, err := exec.LookPath("roachvet")
		if err != nil {
			t.Fatalf("failed to find roachvet: %v", err)
		}
		vetCmd(t, crdb.Dir, "go",
			[]string{"vet", "-vettool", roachlint, "-all", "-printf.funcs", printfuncs, pkgScope},
			filters)

	})
}
