// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build lint
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

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/internal/codeowners"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	_ "github.com/cockroachdb/cockroach/pkg/testutils/buildutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/errors"
	"github.com/ghemawat/stream"
	"github.com/jordanlewis/gcassert"
	"github.com/stretchr/testify/require"
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

// TestLint runs a suite of linters on the codebase.
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
// should be marked with t.Parallel().
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
			stream.GrepNot(`_generated(_test)?\.go`),
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
			skip.IgnoreLint(t, "PKG specified")
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
					":!build/bazel",
					":!ccl/acceptanceccl/backup_test.go",
					":!ccl/backupccl/backup_cloud_test.go",
					// KMS requires AWS credentials from environment variables.
					":!ccl/backupccl/backup_test.go",
					":!cloud",
					":!ccl/workloadccl/fixture_test.go",
					":!internal/reporoot/reporoot.go",
					":!cmd",
					":!util/cgroups/cgroups.go",
					":!nightly",
					":!testutils/lint",
					":!util/envutil/env.go",
					":!testutils/data_path.go",
					":!util/log/tracebacks.go",
					":!util/sdnotify/sdnotify_unix.go",
					":!util/grpcutil", // GRPC_GO_* variables
					":!roachprod",     // roachprod requires AWS environment variables
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
			":!sql/importer",
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
			`\btime\.(Now|Since|Unix|LoadLocation)\(`,
			"--",
			"*.go",
			":!**/embedded.go",
			":!util/timeutil/time.go",
			":!util/timeutil/zoneinfo.go",
			":!util/tracing/span.go",
			":!util/tracing/crdbspan.go",
			":!util/tracing/tracer.go",
			":!cmd/roachtest/tests/gorm_helpers.go",
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

	// Forbid timeutil.Now().Sub(t) in favor of timeutil.Since(t).
	t.Run("TestNowSub", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\btime(util)?\.Now\(\)\.Sub\(`,
			"--",
			"*.go",
			":!*/lint_test.go", // This file.
			":!cmd/dev/**",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'timeutil.Since(t)' instead "+
				"because it is more efficient", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestOsErrorIs", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\bos\.Is(Exist|NotExist|Timeout|Permission)`,
			"--",
			"*.go",
			":!cmd/dev/**",
			":!cmd/mirror/**",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'oserror' instead", s)
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
			":!roachprod", // TODO: switch to contextutil
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`nolint:context`),
		), func(s string) {
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
			":!server/testserver.go",
			":!util/tracing/*_test.go",
			":!ccl/sqlproxyccl/tenantdirsvr/test_directory_svr.go",
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

	t.Run("TestNumCPU", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`runtime\.NumCPU\(\)`,
			"--",
			"*.go",
			":!testutils/lint/*.go",
			":!util/system/*.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden, use system.NumCPU instead (after reading that function's comment)", s)
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

	t.Run("TestTSkipNotUsed", func(t *testing.T) {
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			// Search for benchmarks and tests that run testing.TB.Skip. The
			// convention is to use t or b for the test variable, but sometimes people
			// use something like `testingT` as their variable name, so include the
			// capital letters as well. This isn't foolproof, but searching for
			// any users of a .Skip method isn't great because there are other, non
			// testing methods called Skip out there.
			`[tTbB]\.Skipf?\(`,
			"--",
			"*.go",
			":!testutils/skip/skip.go",
			":!cmd/roachtest/*.go",
			":!acceptance/compose/*.go",
			":!util/syncutil/*.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- t.Skip banned: please use skip.WithIssue, skip.IgnoreLint, etc", s)
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
			":!rpc/codec.go",
			":!rpc/codec_test.go",
			":!settings/settings_test.go",
			":!sql/types/types_jsonpb.go",
			":!sql/schemachanger/scplan/internal/scgraphviz/graphviz.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`(json|jsonpb|yaml|protoutil|xml|\.Field|ewkb|wkb|wkt)\.Marshal\(`),
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
			":!util/encoding/encoding.go",
			":!util/hlc/timestamp.go",
			":!rpc/codec.go",
			":!rpc/codec_test.go",
			":!sql/types/types_jsonpb.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`(json|jsonpb|yaml|xml|protoutil|toml|Codec|ewkb|wkb|wkt)\.Unmarshal\(`),
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

	// TestProtoEqual forbids use of proto's Equal() function. It panics
	// on types which alias the Go string type.
	t.Run("TestProtoEqual", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nEw",
			`proto\.Equal`,
			"--",
			"*.go",
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
			t.Errorf("\n%s <- forbidden; use '.Equal()' method instead or  reflect.DeepEqual()", s)
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
			":!kv/kvclient/kvcoord/lock_spans_over_budget_error.go",
			":!roachpb/replica_unavailable_error.go",
			":!sql/pgwire/pgerror/constraint_name.go",
			":!sql/pgwire/pgerror/severity.go",
			":!sql/pgwire/pgerror/with_candidate_code.go",
			":!sql/pgwire/pgwirebase/too_big_error.go",
			":!sql/protoreflect/redact.go",
			":!sql/colexecerror/error.go",
			":!util/contextutil/timeout_error.go",
			":!util/protoutil/jsonpb_marshal.go",
			":!util/protoutil/marshal.go",
			":!util/protoutil/marshaler.go",
			":!util/tracing/span.go",
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

	t.Run("TestOsExit", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nEw",
			`os\.Exit`,
			"--",
			"*.go",
			":!*_test.go",
			":!acceptance",
			":!cmd",
			":!cli/exit",
			":!bench/cmd",
			":!sql/opt/optgen",
			":!sql/colexec/execgen",
			":!roachpb/gen/main.go",
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
			t.Errorf("\n%s <- forbidden; use 'exit.WithCode' instead", s)
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

	t.Run("TestGofmtSimplify", func(t *testing.T) {
		t.Parallel()
		if pkgSpecified {
			skip.IgnoreLint(t, "PKG specified")
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
			skip.IgnoreLint(t, "PKG specified")
		}
		ignore := `\.(pb(\.gw)?)|(\.[eo]g)\.go|/testdata/|^sql/parser/sql\.go$|_generated(_test)?\.go$`
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
			"oserror":                                     "github.com/cockroachdb/errors/oserror",
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
			stream.GrepNot(`cockroach/pkg/roachprod/logger: log$`),
			stream.GrepNot(`cockroach/pkg/testutils/lint: log$`),
			stream.GrepNot(`cockroach/pkg/util/sysutil: syscall$`),
			stream.GrepNot(`cockroach/pkg/roachprod/install: syscall$`), // TODO: switch to sysutil
			stream.GrepNot(`cockroach/pkg/util/log: github\.com/pkg/errors$`),
			stream.GrepNot(`cockroach/pkg/(base|release|security|util/(log|randutil|stop)): log$`),
			stream.GrepNot(`cockroach/pkg/(server/serverpb|ts/tspb): github\.com/golang/protobuf/proto$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/rpc: github\.com/golang/protobuf/proto$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/sql/lexbase/allkeywords: log$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/util/timeutil/gen: log$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/roachpb/gen: log$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/util/log/gen: log$`),
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
				case strings.HasSuffix(s, "buildutil"):
				case strings.HasSuffix(s, settingsPkgPrefix):
				default:
					t.Errorf("%s <- please don't add CRDB dependencies to settings pkg", s)
				}
			}
		}); err != nil {
			t.Error(err)
		}
	})

	t.Run("TestForbiddenImportsSQLShell", func(t *testing.T) {
		t.Parallel()

		cmd, stderr, filter, err := dirCmd(crdb.Dir, "go", "list", "-deps",
			filepath.Join(cockroachDB, "./pkg/cmd/cockroach-sql"))
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		// forbiddenImportPkg
		forbiddenImports := map[string]struct{}{
			"github.com/cockroachdb/pebble":                     {},
			"github.com/cockroachdb/cockroach/pkg/cli":          {},
			"github.com/cockroachdb/cockroach/pkg/kv/kvserver":  {},
			"github.com/cockroachdb/cockroach/pkg/roachpb":      {},
			"github.com/cockroachdb/cockroach/pkg/server":       {},
			"github.com/cockroachdb/cockroach/pkg/sql":          {},
			"github.com/cockroachdb/cockroach/pkg/sql/catalog":  {},
			"github.com/cockroachdb/cockroach/pkg/sql/parser":   {},
			"github.com/cockroachdb/cockroach/pkg/sql/sem/tree": {},
			"github.com/cockroachdb/cockroach/pkg/storage":      {},
			"github.com/cockroachdb/cockroach/pkg/util/log":     {},
			"github.com/cockroachdb/cockroach/pkg/util/stop":    {},
			"github.com/cockroachdb/cockroach/pkg/util/tracing": {},
		}

		if err := stream.ForEach(
			stream.Sequence(
				filter,
				stream.Sort(),
				stream.Uniq()),
			func(s string) {
				if _, ok := forbiddenImports[s]; ok {
					t.Errorf("\ncockroach-sql depends on %s <- forbidden, this import makes the SQL shell too large", s)
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

	// TODO(tamird): replace this with errcheck.NewChecker() when
	// https://github.com/dominikh/go-tools/issues/57 is fixed.
	t.Run("TestErrCheck", func(t *testing.T) {
		skip.UnderShort(t)
		if bazel.BuiltWithBazel() {
			skip.IgnoreLint(t, "the errcheck tests are run during the bazel build")
		}
		excludesPath, err := filepath.Abs(testutils.TestDataPath(t, "errcheck_excludes.txt"))
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
		skip.UnderShort(t)
		skip.UnderBazelWithIssue(t, 73391, "Going to migrate to nogo")
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
				stream.GrepNot("migration/.*exported func TestingNewCluster returns unexported type"),
				stream.GrepNot("sql/.*exported func .* returns unexported type sql.planNode"),
				stream.GrepNot("pkg/sql/types/types.go.* var Uuid should be UUID"),
				stream.GrepNot("pkg/sql/oidext/oidext.go.*don't use underscores in Go names; const T_"),
				stream.GrepNot("server/api_v2.go.*package comment should be of the form"),
				stream.GrepNot("pkg/util/timeutil/time_zone_util.go.*error strings should not be capitalized or end with punctuation or a newline"),
				stream.GrepNot("pkg/sql/job_exec_context_test_util.go.*exported method ExtendedEvalContext returns unexported type"),
				stream.GrepNot("pkg/sql/job_exec_context_test_util.go.*exported method SessionDataMutatorIterator returns unexported type"),

				stream.GrepNot("type name will be used as row.RowLimit by other packages, and that stutters; consider calling this Limit"),
				stream.GrepNot("type name will be used as tracing.TracingMode by other packages, and that stutters; consider calling this Mode"),
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
		skip.UnderShort(t)
		if bazel.BuiltWithBazel() {
			skip.IgnoreLint(t, "the staticcheck tests are run during the bazel build")
		}

		cmd, stderr, filter, err := dirCmd(
			crdb.Dir,
			"staticcheck",
			pkgScope)
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
				// This file is a conditionally-compiled stub implementation that
				// will produce fake "func is unused" errors.
				stream.GrepNot(`pkg/build/bazel/non_bazel.go`),
				// NOTE(ricky): No idea what's wrong with mirror.go. See #72521
				stream.GrepNot(`pkg/cmd/mirror/mirror.go`),
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
				// Using deprecated method to COPY because the copyin driver does not
				// implement StmtExecContext as of 07/06/2020.
				stream.GrepNot(`pkg/cli/nodelocal.go:.* stmt.Exec is deprecated: .*`),
				stream.GrepNot(`pkg/cli/userfile.go:.* stmt.Exec is deprecated: .*`),
				// Cause is a method used by pkg/cockroachdb/errors (through an unnamed
				// interface).
				stream.GrepNot(`pkg/.*.go:.* func .*\.Cause is unused`),
				// Using deprecated WireLength call.
				stream.GrepNot(`pkg/rpc/stats_handler.go:.*v.WireLength is deprecated: This field is never set.*`),
				// roachpb/api.go needs v1 Protobuf reflection
				stream.GrepNot(`pkg/roachpb/api_test.go:.*package github.com/golang/protobuf/proto is deprecated: Use the "google.golang.org/protobuf/proto" package instead.`),
				// rpc/codec.go imports the same proto package that grpc-go imports (as of crdb@dd87d1145 and grpc-go@7b167fd6).
				stream.GrepNot(`pkg/rpc/codec.go:.*package github.com/golang/protobuf/proto is deprecated: Use the "google.golang.org/protobuf/proto" package instead.`),
				// goschedstats contains partial copies of go runtime structures, with
				// many fields that we're not using.
				stream.GrepNot(`pkg/util/goschedstats/runtime.*\.go:.*is unused`),
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
			`panic\(.*\)`,
			"--",
			// NOTE: if you're adding a new package to the list here because it
			// uses "panic-catch" error propagation mechanism of the vectorized
			// engine, don't forget to "register" the newly added package in
			// sql/colexecerror/error.go file.
			"sql/col*",
			":!sql/colexecerror/error.go",
			// This exception is because execgen itself uses panics during code
			// generation - not at execution time. The (glob,exclude) directive
			// (see git help gitglossary) makes * behave like a normal, single dir
			// glob, and exclude is the synonym of !.
			":(glob,exclude)sql/colexec/execgen/*.go",
			":!sql/colexec/execgen/testdata",
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
			// - coldata.NewMemBatchWithCapacity
			// - coldata.NewMemColumn
			// - coldata.Batch.AppendCol
			// TODO(yuzefovich): prohibit call to coldata.NewMemBatchNoCols.
			`(coldata\.NewMem(Batch|BatchWithCapacity|Column)|\.AppendCol)\(`,
			"--",
			// TODO(yuzefovich): prohibit calling coldata.* methods from other
			// sql/col* packages.
			"sql/colexec",
			"sql/colflow",
			":!sql/colexec/colexecbase/simple_project.go",
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

	t.Run("TestVectorizedDynamicBatches", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			// We prohibit usage of colmem.Allocator.NewMemBatchWithMaxCapacity
			// in order to remind us to think whether we want the dynamic batch
			// size behavior or not.
			`\.NewMemBatchWithMaxCapacity\(`,
			"--",
			"sql/col*",
			":!sql/col*_test.go",
			":!sql/colexec/colexectestutils/utils.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; either use ResetMaybeReallocate or NewMemBatchWithFixedCapacity", s)
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
			// vectorTypeEnforcer and BatchSchemaPrefixEnforcer.
			`(MaybeAppendColumn)\(`,
			"--",
			"sql/col*",
			":!sql/colexec/colexecutils/operator.go",
			":!sql/colmem/allocator.go",
			":!sql/colmem/allocator_test.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use colexecutils.vectorTypeEnforcer "+
				"or colexecutils.BatchSchemaPrefixEnforcer", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestVectorizedAppendToVector", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			// We prohibit usage of Vec.Append outside of the
			// colexecutils.AppendOnlyBufferedBatch.
			`\.(Append)\(`,
			"--",
			"sql/col*",
			":!sql/colexec/colexecutils/utils.go",
			":!sql/colmem/allocator_test.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use coldata.Vec.Copy or colexecutils.AppendOnlyBufferedGroup", s)
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
			`(yps|ypes) = append\(`,
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
		skip.UnderShort(t)
		skip.UnderBazelWithIssue(t, 65485, "Doesn't work in Bazel -- not really sure why yet")

		t.Parallel()
		var buf strings.Builder
		if err := gcassert.GCAssert(&buf,
			"../../col/coldata",
			"../../sql/colconv",
			"../../sql/colexec",
			"../../sql/colexec/colexecagg",
			"../../sql/colexec/colexecbase",
			"../../sql/colexec/colexechash",
			"../../sql/colexec/colexecjoin",
			"../../sql/colexec/colexecproj",
			"../../sql/colexec/colexecsel",
			"../../sql/colexec/colexecspan",
			"../../sql/colexec/colexecwindow",
			"../../sql/colfetcher",
			"../../sql/row",
			"../../kv/kvclient/rangecache",
		); err != nil {
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
			`\[\]types.T`,
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
		skip.UnderShort(t)
		if bazel.BuiltWithBazel() {
			skip.IgnoreLint(t, "the roachvet tests are run during the bazel build")
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

		nakedGoroutineExceptions := `(` + strings.Join([]string{
			`pkg/.*_test\.go`,
			`pkg/acceptance/.*\.go`,
			`pkg/cli/syncbench/.*\.go`,
			`pkg/cli/systembench/.*\.go`,
			`pkg/cmd/allocsim/.*\.go`,
			`pkg/cmd/cmp-protocol/.*\.go`,
			`pkg/cmd/cr2pg/.*\.go`,
			`pkg/cmd/reduce/.*\.go`,
			`pkg/cmd/roachprod-stress/.*\.go`,
			`pkg/cmd/roachprod/.*\.go`,
			`pkg/cmd/roachtest/.*\.go`,
			`pkg/cmd/smithtest/.*\.go`,
			`pkg/cmd/urlcheck/.*\.go`,
			`pkg/cmd/zerosum/.*\.go`,
			`pkg/testutils/.*\.go`,
			`pkg/workload/.*\.go`,
		}, "|") + `)`
		unkeyedLiteralExceptions := `pkg/.*_test\.go:.*(` + strings.Join([]string{
			`pkg/testutils/sstutil\.KV`,
		}, "|") + `)`
		filters := []stream.Filter{
			// Ignore generated files.
			stream.GrepNot(`pkg/.*\.pb\.go:`),
			stream.GrepNot(`pkg/.*\.pb\.gw\.go:`),
			stream.GrepNot(`pkg/.*\.[eo]g\.go:`),
			stream.GrepNot(`pkg/.*_generated\.go:`),

			// Ignore types that can change by system.
			stream.GrepNot(`pkg/util/sysutil/sysutil_unix.go:`),

			// Ignore jemalloc issues warnings.
			stream.GrepNot(`In file included from.*(start|runtime)_jemalloc\.go`),
			stream.GrepNot(`include/jemalloc/jemalloc\.h`),

			// Allow shadowing for variables named err, pErr (proto-errors in kv) and
			// ctx. For these variables, these names are very common and having too
			// look for new names to avoid shadowing is too onerous or even
			// counter-productive if it makes people use the wrong variable by
			// mistake.
			stream.GrepNot(`declaration of "?(pE|e)rr"? shadows`),
			stream.GrepNot(`declaration of "ctx" shadows`),

			// This exception is for hash.go, which re-implements runtime.noescape
			// for efficient hashing.
			stream.GrepNot(`pkg/sql/colexec/colexechash/hash.go:[0-9:]+: possible misuse of unsafe.Pointer`),
			stream.GrepNot(`^#`), // comment line
			// Roachpb's own error package takes ownership of error unwraps
			// (by enforcing that errors can never been wrapped under a
			// roachpb.Error, which is an inconvenient limitation but it is
			// what it is). Once this code is simplified to use generalized
			// error encode/decode, it can be dropped from the linter
			// exception as well.
			stream.GrepNot(`pkg/roachpb/errors\.go:.*invalid direct cast on error object`),
			// Cast in decode handler.
			stream.GrepNot(`pkg/sql/pgwire/pgerror/constraint_name\.go:.*invalid direct cast on error object`),
			// Cast in decode handler.
			stream.GrepNot(`pkg/kv/kvclient/kvcoord/lock_spans_over_budget_error\.go:.*invalid direct cast on error object`),
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
			// Cast in decode handler.
			stream.GrepNot(`pkg/util/contextutil/timeout_error\.go:.*invalid direct cast on error object`),
			// The logging package translates log.Fatal calls into errors.
			// We can't use the regular exception mechanism via functions.go
			// because addStructured takes its positional argument as []interface{},
			// instead of ...interface{}.
			stream.GrepNot(`pkg/util/log/channels\.go:\d+:\d+: logfDepth\(\): format argument is not a constant expression`),
			// roachprod/logger is not collecting redactable logs so we don't care
			// about printf hygiene there as much.
			stream.GrepNot(`pkg/roachprod/logger/log\.go:.*format argument is not a constant expression`),
			// We purposefully produce nil dereferences in this file to test crash conditions
			stream.GrepNot(`pkg/util/log/logcrash/crash_reporting_test\.go:.*nil dereference in type assertion`),
			// Spawning naked goroutines is ok when it's not as part of the main CRDB
			// binary. This is for now - if we use #58164 to introduce more aggressive
			// pooling, etc, then test code needs to adhere as well.
			stream.GrepNot(nakedGoroutineExceptions + `:.*Use of go keyword not allowed`),
			stream.GrepNot(nakedGoroutineExceptions + `:.*Illegal call to Group\.Go\(\)`),
			// We allow unkeyed struct literals for certain internal test types.
			// Ideally, go vet should not complain about this for types declared in
			// the same module: https://github.com/golang/go/issues/43864
			stream.GrepNot(unkeyedLiteralExceptions + `.*composite literal uses unkeyed fields`),
		}

		const vetTool = "roachvet"
		vetToolPath, err := exec.LookPath(vetTool)
		if err != nil {
			t.Fatalf("failed to find %s: %s", vetTool, err)
		}
		vetCmd(t, crdb.Dir, "go",
			[]string{"vet", "-vettool", vetToolPath, "-all", "-printf.funcs", printfuncs, pkgScope},
			filters)

	})

	t.Run("CODEOWNERS", func(t *testing.T) {
		skip.UnderBazel(t, "doesn't work under bazel")
		co, err := codeowners.DefaultLoadCodeOwners()
		require.NoError(t, err)
		const verbose = false
		repoRoot := filepath.Join("../../../")
		codeowners.LintEverythingIsOwned(t, verbose, co, repoRoot, "pkg")
	})
}
