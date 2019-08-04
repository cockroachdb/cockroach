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
	"github.com/ghemawat/stream"
	"github.com/pkg/errors"
	"golang.org/x/tools/go/buildutil"
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
		t.Skip(err)
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
			case "extract", "trim", "overlay", "position", "substring":
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
			stream.GrepNot(`_string\.go`),
			stream.GrepNot(`_generated\.go`),
			stream.GrepNot(`/embedded.go`),
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

	// TestTabsInOptgen verifies tabs aren't used in optgen (.opt) files.
	t.Run("TestTabsInOptgen", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", "^ *\t", "--", "*.opt")
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
					":!ccl/storageccl/export_storage_test.go",
					":!ccl/workloadccl/fixture_test.go",
					":!cmd",
					":!nightly",
					":!testutils/lint",
					":!util/envutil/env.go",
					":!util/log/clog.go",
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
			`\.Clone\([^)]`,
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
			stream.GrepNot(`(json|yaml|protoutil|xml|\.Field)\.Marshal\(`),
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
			stream.GrepNot(`(json|jsonpb|yaml|xml|protoutil|toml|Codec)\.Unmarshal\(`),
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
			"analyse", // required by SQL grammar
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`.*\.lock`),
			stream.GrepNot(`^storage\/engine\/rocksdb_error_dict\.go$`),
			stream.GrepNot(`^workload/tpcds/tpcds.go$`),
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
			"path":                                        "path/filepath",
			"github.com/golang/protobuf/proto":            "github.com/gogo/protobuf/proto",
			"github.com/satori/go.uuid":                   "util/uuid",
			"golang.org/x/sync/singleflight":              "github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight",
			"syscall":                                     "sysutil",
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
			outer:
				for path := range buildutil.ExpandPatterns(&buildContext, []string{filepath.Join(cockroachDB, pkgScope)}) {
					importPkg, err := buildContext.Import(path, crdb.Dir, 0)
					switch err.(type) {
					case nil:
						for _, s := range importPkg.Imports {
							arg.Out <- importPkg.ImportPath + ": " + s
						}
						for _, s := range importPkg.TestImports {
							arg.Out <- importPkg.ImportPath + ": " + s
						}
						for _, s := range importPkg.XTestImports {
							arg.Out <- importPkg.ImportPath + ": " + s
						}
					case *build.NoGoError:
					case *build.MultiplePackageError:
						if useAllFiles {
							continue outer
						}
					default:
						return errors.Wrapf(err, "error loading package %s", path)
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
			stream.GrepNot(`cockroach/pkg/(base|security|util/(log|randutil|stop)): log$`),
			stream.GrepNot(`cockroach/pkg/(server/serverpb|ts/tspb): github\.com/golang/protobuf/proto$`),
			stream.GrepNot(`cockroach/pkg/server/debug/pprofui: path$`),
			stream.GrepNot(`cockroach/pkg/util/caller: path$`),
			stream.GrepNot(`cockroach/pkg/ccl/storageccl: path$`),
			stream.GrepNot(`cockroach/pkg/ccl/workloadccl: path$`),
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

	t.Run("TestVet", func(t *testing.T) {
		t.Parallel()
		runVet := func(t *testing.T, args ...string) {
			args = append(append([]string{"vet"}, args...), pkgScope)
			cmd := exec.Command("go", args...)
			cmd.Dir = crdb.Dir
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

			if err := stream.ForEach(stream.Sequence(
				stream.FilterFunc(func(arg stream.Arg) error {
					scanner := bufio.NewScanner(&b)
					for scanner.Scan() {
						if s := scanner.Text(); strings.TrimSpace(s) != "" {
							arg.Out <- s
						}
					}
					return scanner.Err()
				}),
				stream.GrepNot(`declaration of "?(pE|e)rr"? shadows`),
				stream.GrepNot(`\.pb\.gw\.go:[0-9:]+: declaration of "?ctx"? shadows`),
				stream.GrepNot(`\.[eo]g\.go:[0-9:]+: declaration of ".*" shadows`),
				stream.GrepNot(`^#`), // comment line
			), func(s string) {
				t.Errorf("\n%s", s)
			}); err != nil {
				t.Error(err)
			}
		}
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
		}, ",")

		// Unfortunately if an analyzer is passed via -vettool like shadow, it seems
		// we cannot also run the core analyzers (or at least cannot pass them flags
		// like -printfuncs).
		t.Run("shadow", func(t *testing.T) { runVet(t, "-vettool=bin/shadow") })
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
		runVet(t, "-all", "-printfuncs", printfuncs)
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
				stream.GrepNot(`pkg/sql/pgwire/pgcode/codes.go:.* const .* is unused`),
				// The methods in exprgen.customFuncs are used via reflection.
				stream.GrepNot(`pkg/sql/opt/optgen/exprgen/custom_funcs.go:.* func .* is unused`),
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
}
