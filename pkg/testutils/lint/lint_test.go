// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// +build lint

package lint

import (
	"bufio"
	"bytes"
	"fmt"
	"go/build"
	"go/parser"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/ghemawat/stream"
	"github.com/kisielk/gotool"
	"github.com/pkg/errors"
	"golang.org/x/tools/go/buildutil"
	"golang.org/x/tools/go/loader"
	"honnef.co/go/tools/lint"
	"honnef.co/go/tools/simple"
	"honnef.co/go/tools/staticcheck"
	"honnef.co/go/tools/unused"
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
			"git", "grep", "-nE", fmt.Sprintf(`[^a-zA-Z](%s)\(`, strings.Join(names, "|")),
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
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-LE", `^// (Copyright|Code generated by)`, "--", "*.go")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf(`%s <- missing license header`, s)
		}); err != nil {
			t.Error(err)
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
			":!util/grpcutil/grpc_util_test.go",
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

	t.Run("TestPGErrors", func(t *testing.T) {
		t.Parallel()
		// TODO(justin): we should expand the packages this applies to as possible.
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", `((fmt|errors).Errorf|errors.New)`, "--", "sql/parser/*.go", "util/ipaddr/*.go")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'pgerror.NewErrorf' instead", s)
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
			stream.GrepNot(`(json|jsonpb|yaml|xml|protoutil)\.Unmarshal\(`),
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

		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "ls-files", "*.go", ":!*/testdata/*")
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
		ignore := `\.(pb(\.gw)?)|(\.[eo]g)\.go|/testdata/|^sql/parser/sql\.go$`
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

	t.Run("TestUnused", func(t *testing.T) {
		if testing.Short() {
			t.Skip("short flag")
		}
		if pkgSpecified {
			t.Skip("PKG specified")
		}
		// This test uses 6GB of RAM (as of 2018-07-13), so it should not be parallelized.

		ctx := gotool.DefaultContext
		releaseTags := ctx.BuildContext.ReleaseTags
		lastTag := releaseTags[len(releaseTags)-1]
		dotIdx := strings.IndexByte(lastTag, '.')
		goVersion, err := strconv.Atoi(lastTag[dotIdx+1:])
		if err != nil {
			t.Fatal(err)
		}
		// Detecting unused exported fields/functions requires analyzing
		// the whole program (and all its tests) at once. Therefore, we
		// must load all packages instead of restricting the test to
		// pkgScope (that's why this is separate from Megacheck, even
		// though it is possible to combine them).
		paths := ctx.ImportPaths([]string{cockroachDB + "/pkg/..."})
		conf := loader.Config{
			Build:      &ctx.BuildContext,
			ParserMode: parser.ParseComments,
			ImportPkgs: make(map[string]bool, len(paths)),
		}
		for _, path := range paths {
			conf.ImportPkgs[path] = true
		}
		lprog, err := conf.Load()
		if err != nil {
			t.Fatal(err)
		}

		unusedChecker := unused.NewChecker(unused.CheckAll)
		unusedChecker.WholeProgram = true

		linter := lint.Linter{
			Checker:   unused.NewLintChecker(unusedChecker),
			GoVersion: goVersion,
			Ignores: []lint.Ignore{
				// sql/parser/yaccpar:14:6: type sqlParser is unused (U1000)
				// sql/parser/yaccpar:15:2: func sqlParser.Parse is unused (U1000)
				// sql/parser/yaccpar:16:2: func sqlParser.Lookahead is unused (U1000)
				// sql/parser/yaccpar:29:6: func sqlNewParser is unused (U1000)
				// sql/parser/yaccpar:152:6: func sqlParse is unused (U1000)
				&lint.GlobIgnore{Pattern: "github.com/cockroachdb/cockroach/pkg/sql/parser/sql.go", Checks: []string{"U1000"}},
				// Generated file containing many unused postgres error codes.
				&lint.GlobIgnore{Pattern: "github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror/codes.go", Checks: []string{"U1000"}},
			},
		}
		for _, p := range linter.Lint(lprog, &conf) {
			t.Errorf("%s: %s", p.Position, &p)
		}
	})

	// TestLogicTestsLint verifies that all the logic test files start with
	// a LogicTest directive.
	t.Run("TestLogicTestsLint", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir, "git", "ls-files",
			"sql/logictest/testdata/logic_test/",
			"sql/logictest/testdata/planner_test/",
			"sql/opt/exec/execbuilder/testdata/",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(filename string) {
			file, err := os.Open(filepath.Join(pkgDir, filename))
			if err != nil {
				t.Error(err)
				return
			}
			defer file.Close()
			firstLine, err := bufio.NewReader(file).ReadString('\n')
			if err != nil {
				t.Errorf("reading first line of %s: %s", filename, err)
				return
			}
			if !strings.HasPrefix(firstLine, "# LogicTest:") {
				t.Errorf("%s must start with a directive, e.g. `# LogicTest: default`", filename)
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

	// Things that are packaged scoped are below here.
	pkgScope := pkgVar
	if !pkgSpecified {
		pkgScope = "./pkg/..."
	}

	t.Run("TestForbiddenImports", func(t *testing.T) {
		t.Parallel()

		// forbiddenImportPkg -> permittedReplacementPkg
		forbiddenImports := map[string]string{
			"golang.org/x/net/context": "context",
			"log":  "util/log",
			"path": "path/filepath",
			"github.com/golang/protobuf/proto": "github.com/gogo/protobuf/proto",
			"github.com/satori/go.uuid":        "util/uuid",
			"golang.org/x/sync/singleflight":   "github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight",
			"syscall":                          "sysutil",
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
		// `go vet` is a special snowflake that emits all its output on
		// `stderr.
		cmd := exec.Command("go", "vet", "-all", "-shadow", "-printfuncs",
			strings.Join([]string{
				"Info:1",
				"Infof:1",
				"InfofDepth:2",
				"Warning:1",
				"Warningf:1",
				"WarningfDepth:2",
				"Error:1",
				"Errorf:1",
				"ErrorfDepth:2",
				"Fatal:1",
				"Fatalf:1",
				"FatalfDepth:2",
				"Event:1",
				"Eventf:1",
				"ErrEvent:1",
				"ErrEventf:1",
				"NewError:1",
				"NewErrorf:1",
				"VEvent:2",
				"VEventf:2",
				"UnimplementedWithIssueErrorf:1",
				"Wrapf:1",
			}, ","),
			pkgScope,
		)
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
			stream.GrepNot(`\.pb\.gw\.go:[0-9]+: declaration of "?ctx"? shadows`),
			stream.GrepNot(`\.[eo]g\.go:[0-9]+: declaration of ".*" shadows`),
			stream.GrepNot(`^#`), // comment line
			// Upstream compiler error. See: https://github.com/golang/go/issues/23701
			stream.GrepNot(`pkg/sql/pgwire/pgwire_test\.go.*internal compiler error`),
			stream.GrepNot(`^Please file a bug report|^https://golang.org/issue/new`),
		), func(s string) {
			t.Errorf("\n%s", s)
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
				// _fsm.go files are allowed to dot-import the util/fsm package.
				stream.GrepNot("_fsm.go.*should not use dot imports"),
				stream.GrepNot("sql/.*exported func .* returns unexported type sql.planNode"),
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

	t.Run("TestMegacheck", func(t *testing.T) {
		if testing.Short() {
			t.Skip("short flag")
		}
		// This test uses 9GB of RAM (as of 2018-07-13), so it should not be parallelized.

		ctx := gotool.DefaultContext
		releaseTags := ctx.BuildContext.ReleaseTags
		lastTag := releaseTags[len(releaseTags)-1]
		dotIdx := strings.IndexByte(lastTag, '.')
		goVersion, err := strconv.Atoi(lastTag[dotIdx+1:])
		if err != nil {
			t.Fatal(err)
		}
		paths := ctx.ImportPaths([]string{filepath.Join(cockroachDB, pkgScope)})
		conf := loader.Config{
			Build:      &ctx.BuildContext,
			ParserMode: parser.ParseComments,
			ImportPkgs: make(map[string]bool, len(paths)),
		}
		for _, path := range paths {
			conf.ImportPkgs[path] = true
		}
		lprog, err := conf.Load()
		if err != nil {
			t.Fatal(err)
		}

		for checker, ignores := range map[lint.Checker][]lint.Ignore{
			&miscChecker{}:  nil,
			&timerChecker{}: nil,
			&hashChecker{}:  nil,
			simple.NewChecker(): {
				&lint.GlobIgnore{Pattern: "github.com/cockroachdb/cockroach/pkg/security/securitytest/embedded.go", Checks: []string{"S1013"}},
				&lint.GlobIgnore{Pattern: "github.com/cockroachdb/cockroach/pkg/ui/embedded.go", Checks: []string{"S1013"}},
			},
			staticcheck.NewChecker(): {
				// The generated parser is full of `case` arms such as:
				//
				// case 1:
				// 	sqlDollar = sqlS[sqlpt-1 : sqlpt+1]
				// 	//line sql.y:781
				// 	{
				// 		sqllex.(*Scanner).stmts = sqlDollar[1].union.stmts()
				// 	}
				//
				// where the code in braces is generated from the grammar action; if
				// the action does not make use of the matched expression, sqlDollar
				// will be assigned but not used. This is expected and intentional.
				//
				// Concretely, the grammar:
				//
				// stmt:
				//   alter_table_stmt
				// | backup_stmt
				// | copy_from_stmt
				// | create_stmt
				// | delete_stmt
				// | drop_stmt
				// | explain_stmt
				// | help_stmt
				// | prepare_stmt
				// | execute_stmt
				// | deallocate_stmt
				// | grant_stmt
				// | insert_stmt
				// | rename_stmt
				// | revoke_stmt
				// | savepoint_stmt
				// | select_stmt
				//   {
				//     $$.val = $1.slct()
				//   }
				// | set_stmt
				// | show_stmt
				// | split_stmt
				// | transaction_stmt
				// | release_stmt
				// | truncate_stmt
				// | update_stmt
				// | /* EMPTY */
				//   {
				//     $$.val = Statement(nil)
				//   }
				//
				// is compiled into the `case` arm:
				//
				// case 28:
				// 	sqlDollar = sqlS[sqlpt-0 : sqlpt+1]
				// 	//line sql.y:830
				// 	{
				// 		sqlVAL.union.val = Statement(nil)
				// 	}
				//
				// which results in the unused warning:
				//
				// sql/parser/yaccpar:362:3: this value of sqlDollar is never used (SA4006)
				&lint.GlobIgnore{Pattern: "github.com/cockroachdb/cockroach/pkg/sql/parser/sql.go", Checks: []string{"SA4006"}},
				// Files generated by github.com/grpc-ecosystem/grpc-gateway use a
				// deprecated logging method (SA1019). Ignore such errors until they
				// fix it and we update to using a newer SHA.
				&lint.GlobIgnore{Pattern: "github.com/cockroachdb/cockroach/pkg/*/*/*.pb.gw.go", Checks: []string{"SA1019"}},
			},
		} {
			t.Run(checker.Name(), func(t *testing.T) {
				linter := lint.Linter{
					Checker:   checker,
					Ignores:   ignores,
					GoVersion: goVersion,
				}
				for _, p := range linter.Lint(lprog, &conf) {
					t.Errorf("%s: %s", p.Position, &p)
				}
			})
		}
	})

}
