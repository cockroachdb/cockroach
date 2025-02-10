// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build lint

package lint

import (
	"bufio"
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
	"github.com/ghemawat/stream"
	"github.com/jordanlewis/gcassert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/packages"
)

// Various copyright file headers we lint on.
var (
	cslHeader = regexp.MustCompile(`// Copyright 20\d\d The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
`)
	cslHeaderHash = regexp.MustCompile(`# Copyright 20\d\d The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.
`)
	// etcdApacheHeader is the header of pkg/raft at the time it was imported.
	etcdApacheHeader = regexp.MustCompile(`// Copyright 20\d\d The etcd Authors
//
// Licensed under the Apache License, Version 2.0 \(the "License"\);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
`)
	// cockroachModifiedCopyright is a header that's required to be added any
	// time a file with etcdApacheHeader is modified by authors from CRL.
	cockroachModifiedCopyright = regexp.MustCompile(
		`// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 20\d\d The Cockroach Authors.`)
)

const cockroachDB = "github.com/cockroachdb/cockroach"

//go:embed gcassert_paths.txt
var rawGcassertPaths string

func init() {
	if bazel.BuiltWithBazel() {
		goSdk := os.Getenv("GO_SDK")
		if goSdk == "" {
			panic("expected GO_SDK")
		}
		if err := os.Setenv("PATH", fmt.Sprintf("%s%c%s", filepath.Join(goSdk, "bin"), os.PathListSeparator, os.Getenv("PATH"))); err != nil {
			panic(err)
		}
		if err := os.Setenv("GOROOT", goSdk); err != nil {
			panic(err)
		}
	}
}

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
	err := cmd.Run()
	if err != nil && !errors.HasType(err, (*exec.ExitError)(nil)) {
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
	var crdbDir, pkgDir string
	if bazel.BuiltWithBazel() {
		var set bool
		crdbDir, set = envutil.EnvString("COCKROACH_WORKSPACE", 0)
		if !set || crdbDir == "" {
			t.Fatal("must supply COCKROACH_WORKSPACE variable (./dev lint does this for you automatically)")
		}
		pkgDir = filepath.Join(crdbDir, "pkg")
	} else {
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		for filepath.Base(cwd) != "pkg" {
			cwd = filepath.Dir(cwd)
		}
		pkgDir = cwd
		crdbDir = filepath.Dir(pkgDir)
	}

	pkgVar, pkgSpecified := os.LookupEnv("PKG")

	var nogoConfig map[string]any
	var nogoJSON []byte
	if bazel.BuiltWithBazel() {
		nogoJSONPath, err := bazel.Runfile("build/bazelutil/nogo_config.json")
		if err != nil {
			t.Fatal(err)
		}
		nogoJSON, err = os.ReadFile(nogoJSONPath)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		var err error
		nogoJSON, err = os.ReadFile(filepath.Join(crdbDir, "build", "bazelutil", "nogo_config.json"))
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := json.Unmarshal(nogoJSON, &nogoConfig); err != nil {
		t.Error(err)
	}

	// Things that are package scoped are below here.
	pkgScope := pkgVar
	if !pkgSpecified {
		pkgScope = "./pkg/..."
	}

	// Load packages for top-level forbidden import tests.
	pkgPath := filepath.Join(cockroachDB, pkgScope)
	pkgs, err := packages.Load(
		&packages.Config{
			Mode: packages.NeedImports | packages.NeedName,
			Dir:  crdbDir,
		},
		pkgPath,
	)
	if err != nil {
		t.Fatal(errors.Wrapf(err, "error loading package %s", pkgPath))
	}
	// NB: if no packages were found, this API confusingly
	// returns no error, so we need to explicitly check that
	// something was returned.
	if len(pkgs) == 0 {
		t.Fatalf("could not list packages under %s", pkgPath)
	}

	t.Run("TestLowercaseFunctionNames", func(t *testing.T) {
		skip.UnderShort(t)
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
		for _, name := range builtins.AllBuiltinNames() {
			if _, ok := builtins.CastBuiltinNames[name]; ok {
				continue
			}
			switch name {
			case "extract", "trim", "overlay", "position", "substring", "st_x", "st_y":
				// Exempt special forms: EXTRACT(... FROM ...), etc.
			default:
				names = append(names, strings.ToUpper(name))
			}
		}

		cmd, stderr, filter, err := dirCmd(crdbDir,
			"git", "grep", "-nE", fmt.Sprintf(`[^_a-zA-Z](%s)\(`, strings.Join(names, "|")),
			"--", "pkg", ":!pkg/cmd/roachtest/testdata/regression.diffs")
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

	t.Run("TestCopyrightHeadersWithSlash", func(t *testing.T) {
		t.Parallel()

		// These extensions identify source files that should have copyright headers.
		extensions := []string{
			"*.go", "*.cc", "*.h", "*.js", "*.ts", "*.tsx", "*.s", "*.S", "*.scss", "*.styl", "*.proto", "*.rl",
		}
		fullExtensions := make([]string, len(extensions)*2)
		for i, extension := range extensions {
			fullExtensions[i*2] = "build/**/" + extension
			fullExtensions[i*2+1] = "pkg/**/" + extension
		}

		cmd, stderr, filter, err := dirCmd(crdbDir, "git", append([]string{"ls-files"}, fullExtensions...)...)
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
			// These files are copied from bazel upstream with its own license.
			stream.GrepNot(`build/bazel/bes/.*.proto$`),
			// These files are copied from raft upstream with its own license.
			stream.GrepNot(`^pkg/raft/.*`),
			// Generated files for plpgsql.
			stream.GrepNot(`sql/plpgsql/parser/plpgsqllexbase/.*.go`),
		), func(filename string) {
			file, err := os.Open(filepath.Join(crdbDir, filename))
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

			if cslHeader.Find(data) == nil {
				t.Errorf("did not find expected CSL license header in %s", filename)
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

	t.Run("TestCopyrightHeadersWithHash", func(t *testing.T) {
		t.Parallel()

		// These extensions identify source files that should have copyright headers.
		extensions := []string{"GNUmakefile", "Makefile", "*.py", "*.sh"}

		cmd, stderr, filter, err := dirCmd(crdbDir, "git", append([]string{"ls-files"}, extensions...)...)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(filter,
			stream.GrepNot(`^c-deps/.*`),
			// These files are copied from raft upstream with its own license.
			stream.GrepNot(`^raft/.*`),
		), func(filename string) {
			file, err := os.Open(filepath.Join(crdbDir, filename))
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

			if cslHeaderHash.Find(data) == nil {
				t.Errorf("did not find expected CSL license header in %s", filename)
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

	// TestRaftCopyrightHeaders checks that all the source files in pkg/raft have
	// the original copyright headers from etcd-io/raft, and the modified files
	// have Cockroach attribution.
	t.Run("TestRaftCopyrightHeaders", func(t *testing.T) {
		t.Parallel()
		if pkgSpecified {
			skip.IgnoreLint(t, "PKG specified")
		}

		raftDir := filepath.Join(pkgDir, "raft")
		// These extensions identify source files that should have copyright headers.
		// TODO(pav-kv): add "*.proto". Currently raft.proto has no header.
		extensions := []string{"*.go"}

		// The commit that imported etcd-io/raft into pkg/raft.
		const baseSHA = "cd6f4f263bd42688096064825dfa668bde2d3720"
		const modifiedFlag = "M"
		const addedFlag = "A"
		gitDiff := func(flag string) map[string]struct{} {
			// List the source files that have been modified.
			cmd, stderr, filter, err := dirCmd(raftDir, "git", append([]string{
				"diff", baseSHA, "--name-status", fmt.Sprintf("--diff-filter=%s", flag), "--"},
				extensions...)...)
			require.NoError(t, err)
			require.NoError(t, cmd.Start())
			// The command outputs lines of the form "M\t<filename>".
			paths := make(map[string]struct{})
			require.NoError(t, stream.ForEach(stream.Sequence(filter), func(row string) {
				parts := strings.Split(row, "\t")
				require.Len(t, parts, 2)
				paths[parts[1]] = struct{}{}
			}))
			if err := cmd.Wait(); err != nil {
				require.Empty(t, stderr.String(), "err=%s", err)
			}
			return paths
		}
		// modified will contain the set of all files in pkg/raft that were
		// modified since importing etcd-io/raft into it.
		modified := gitDiff(modifiedFlag)
		// added will contain the set of all files in pkg/raft that were added
		// since importing etcd-io/raft into it.
		added := gitDiff(addedFlag)

		cmd, stderr, filter, err := dirCmd(raftDir, "git",
			append([]string{"ls-files", "--full-name"}, extensions...)...)
		require.NoError(t, err)
		require.NoError(t, cmd.Start())
		require.NoError(t, stream.ForEach(stream.Sequence(filter,
			stream.GrepNot(`\.pb\.go`),
			stream.GrepNot(`_string\.go`),
		), func(filename string) {
			file, err := os.Open(filepath.Join(crdbDir, filename))
			require.NoError(t, err)
			defer file.Close()
			data := make([]byte, 1024)
			n, err := file.Read(data)
			require.NoError(t, err)
			data = data[0:n]
			if _, ok := added[filename]; ok {
				// Typically, any new file that is added will include a
				// CockroachDB Software Licens header. However, if most of it isn't
				// new code, and is moved from an existing etcd forked file, the
				// author may consider it as modified; the linter is liberal enough
				// to allow either of these.
				assert.True(t, (cslHeader.Find(data) != nil) ||
					(etcdApacheHeader.Find(data) != nil && cockroachModifiedCopyright.Find(data) != nil),
					"did not find expected a) CockroachDB Software License header or b) "+
						"Apache license header and Cockroach copyright header in %s",
					filename)
			} else {
				assert.NotNilf(t, etcdApacheHeader.Find(data),
					"did not find expected Apache license header in %s", filename)
			}
			if _, ok := modified[filename]; ok {
				assert.NotNilf(t, cockroachModifiedCopyright.Find(data),
					"did not find expected Cockroach copyright header in %s", filename)
			}
		}))
		if err := cmd.Wait(); err != nil {
			require.Empty(t, stderr.String(), "err=%s", err)
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
			":!raft/*.go",
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

		// If run outside of Bazel, we assume this binary must be in the PATH.
		optfmt := "optfmt"
		if bazel.BuiltWithBazel() {
			var err error
			optfmt, err = bazel.Runfile("pkg/sql/opt/optgen/cmd/optfmt/optfmt_/optfmt")
			if err != nil {
				t.Fatal(err)
			}
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
				stream.Xargs(optfmt, "-l"),
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
			{re: `\bos\.(Getenv|LookupEnv)\("COCKROACH`,
				excludes: []string{
					":!cmd/bazci/githubpost",
					":!acceptance/test_acceptance.go",           // For COCKROACH_RUN_ACCEPTANCE
					":!compose/compare/compare/compare_test.go", // For COCKROACH_RUN_COMPOSE_COMPARE
					":!compose/compose_test.go",                 // For COCKROACH_RUN_COMPOSE
				},
			},
			{
				re: `\bos\.(Getenv|LookupEnv)\(`,
				excludes: []string{
					":!acceptance",
					":!build/bazel",
					":!ccl/acceptanceccl/backup_test.go",
					":!backup/backup_cloud_test.go",
					// KMS requires AWS credentials from environment variables.
					":!backup/backup_test.go",
					":!ccl/changefeedccl/helpers_test.go",
					":!ccl/cloudccl",
					":!cloud",
					":!ccl/workloadccl/fixture_test.go",
					":!internal/reporoot/reporoot.go",
					":!cmd",
					":!util/cgroups/cgroups.go",
					":!nightly",
					":!testutils/lint",
					":!util/envutil/env.go",
					":!testutils/data_path.go",
					":!testutils/bazelcodecover/code_cover_on.go", // For BAZEL_COVER_DIR.
					":!util/log/tracebacks.go",
					":!util/sdnotify/sdnotify_unix.go",
					":!util/grpcutil",                           // GRPC_GO_* variables
					":!roachprod",                               // roachprod requires AWS environment variables
					":!cli/env.go",                              // The CLI needs the PGHOST variable.
					":!cli/start.go",                            // The CLI needs the GOMEMLIMIT and GOGC variables.
					":!internal/codeowners/codeowners.go",       // For BAZEL_TEST.
					":!internal/team/team.go",                   // For BAZEL_TEST.
					":!util/log/test_log_scope.go",              // For TEST_UNDECLARED_OUTPUT_DIR, REMOTE_EXEC
					":!testutils/datapathutils/data_path.go",    // For TEST_UNDECLARED_OUTPUT_DIR, REMOTE_EXEC
					":!testutils/backup.go",                     // For BACKUP_TESTING_BUCKET
					":!compose/compose_test.go",                 // For PATH.
					":!testutils/skip/skip.go",                  // For REMOTE_EXEC.
					":!build/engflow/engflow.go",                // For GITHUB_ACTIONS_BRANCH, etc.
					":!acceptance/test_acceptance.go",           // For COCKROACH_RUN_ACCEPTANCE
					":!compose/compare/compare/compare_test.go", // For COCKROACH_RUN_COMPOSE_COMPARE
					":!compose/compose_test.go",                 // For COCKROACH_RUN_COMPOSE
					":!testutils/sideeye/sideeye.go",            // For SIDE_EYE_API_TOKEN
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
			`\bsync\.((RW)?Mutex|Map)`,
			"--",
			"*.go",
			":!*/doc.go",
			":!raft/*.go",
			":!util/syncutil/mutex_sync.go",
			":!util/syncutil/mutex_sync_race.go",
			":!testutils/lint/lint_test.go",
			":!testutils/lint/passes/deferunlockcheck/testdata/src/github.com/cockroachdb/cockroach/pkg/util/syncutil/mutex_sync.go",
			// Exception needed for goroutineStalledStates.
			":!kv/kvserver/concurrency/concurrency_manager_test.go",
			// See comment in memLock class.
			":!sql/vecindex/cspann/memstore/memstore_lock.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			var fix string
			switch {
			case strings.Contains(s, "sync.Mutex"):
				fix = "syncutil.Mutex"
			case strings.Contains(s, "sync.RWMutex"):
				fix = "syncutil.RWMutex"
			case strings.Contains(s, "sync.Map"):
				fix = "syncutil.Map"
			default:
				t.Fatalf("unexpected sync reference: %s", s)
			}
			t.Errorf("\n%s <- forbidden; use '%s' instead", s, fix)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestStartServer", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`, _, _ :?= serverutils\.StartServer\(`,
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
			t.Errorf("\n%s <- forbidden; use 'serverutils.StartServerOnly' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestServerCast", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\*(testServer|testTenant)`,
			"--",
			"server/*_test.go",
			":!server/server_special_test.go",
			":!server/server_controller_test.go",
			":!server/settings_cache_test.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use Go interfaces instead (see testutils/serverutils/api.go)", s)
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
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", `\sTODO\([^)]+\)[^:]`, "--",
			"*.go",
			":!raft/*.go",
		)
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

	t.Run("TestCollateSupported", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\bcollate\.Supported\(`,
			"--",
			"*.go",
			":!util/collatedstring/collatedstring.go",
			":!ccl/changefeedccl/avro_test.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'collatedstring.Supported()' instead", s)
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
			":!*_test.go",
			":!**/embedded.go",
			":!util/syncutil/mutex_tracing.go",
			":!util/timeutil/time.go",
			":!util/timeutil/zoneinfo.go",
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
			":!util/timeutil/context.go",
			":!server/testserver_sqlconn.go",
			// TODO(jordan): ban these too?
			":!server/debug/**",
			":!workload/**",
			":!*_test.go",
			":!cli/debug_synctest.go",
			":!cmd/**",
			":!roachprod", // TODO: switch to timeutil
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
			t.Errorf("\n%s <- forbidden; use 'timeutil.RunWithTimeout' instead", s)
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
			":!server/server_obs_service.go",
			":!server/testserver.go",
			":!util/tracing/*_test.go",
			":!ccl/sqlproxyccl/tenantdirsvr/test_directory_svr.go",
			":!ccl/sqlproxyccl/tenantdirsvr/test_simple_directory_svr.go",
			":!ccl/sqlproxyccl/tenantdirsvr/test_static_directory_svr.go",
			":!cmd/bazci/*.go",
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
			":!raft/*.go",
			":!rpc/codec.go",
			":!rpc/codec_test.go",
			":!settings/settings_test.go",
			":!kv/kvpb/api_requestheader.go",
			":!storage/mvcc_value.go",
			":!storage/enginepb/mvcc3_valueheader.go",
			":!sql/types/types_jsonpb.go",
			":!sql/schemachanger/scplan/scviz/maps.go",
			":!workload/schemachange/tracing.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`(json|jsonpb|yaml|protoutil|xml|\.Field|ewkb|wkb|wkt|asn1)\.Marshal\(`),
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
			":!clusterversion/setting.go",
			":!raft/*.go",
			":!util/protoutil/marshal.go",
			":!util/protoutil/marshaler.go",
			":!util/encoding/encoding.go",
			":!util/hlc/timestamp.go",
			":!kv/kvserver/raftlog/encoding.go",
			":!rpc/codec.go",
			":!rpc/codec_test.go",
			":!storage/mvcc_value.go",
			":!roachpb/data.go",
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
			stream.GrepNot(`(json|jsonpb|yaml|xml|protoutil|toml|Codec|ewkb|wkb|wkt|asn1)\.Unmarshal\(`),
			stream.GrepNot(`nolint:protounmarshal`),
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
			":!raft/*.go",
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

	t.Run("", func(t *testing.T) {
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
			":!ccl/changefeedccl/changefeedbase/errors.go",
			":!kv/kvclient/kvcoord/lock_spans_over_budget_error.go",
			":!spanconfig/errors.go",
			":!kv/kvpb/replica_unavailable_error.go",
			":!kv/kvpb/ambiguous_result_error.go",
			":!kv/kvpb/errors.go",
			":!sql/flowinfra/flow_registry.go",
			":!sql/pgwire/pgerror/constraint_name.go",
			":!sql/pgwire/pgerror/severity.go",
			":!sql/pgwire/pgerror/with_candidate_code.go",
			":!sql/pgwire/pgwirebase/too_big_error.go",
			":!sql/plpgsql/plpgsql_error.go",
			":!sql/protoreflect/redact.go",
			":!sql/colexecerror/error.go",
			":!util/timeutil/timeout_error.go",
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
			":!raft/*.go",
			":!sql/opt/optgen",
			":!sql/colexec/execgen",
			":!kv/kvpb/gen/main.go",
			":!testutils/serverutils/fwgen/gen.go",
			":!gen/genbzl/main.go",
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

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`nolint:yaml`),
		), func(s string) {
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
		cmd, stderr, filter, err := dirCmd(
			pkgDir, "git", "grep", "-nE",
			`^(import|\s+)(\w+ )?"database/sql"$`, "--",
			"*.go", ":!*_generated_test.go", ":!*_generated.go",
		)
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
		// If run outside of Bazel, we assume this binary must be in the PATH.
		crlfmt := "crlfmt"
		if bazel.BuiltWithBazel() {
			var err error
			crlfmt, err = bazel.Runfile("external/com_github_cockroachdb_crlfmt/crlfmt_/crlfmt")
			if err != nil {
				t.Fatal(err)
			}
		}

		ignore := `zcgo*|\.(pb(\.gw)?)|(\.[eo]g)\.go|/testdata/|^sql/parser/sql\.go$|(_)?generated(_test)?\.go$|^sql/pgrepl/pgreplparser/pgrepl\.go$|^sql/plpgsql/parser/plpgsql\.go$|^util/jsonpath/parser/jsonpath\.go$`
		cmd, stderr, filter, err := dirCmd(pkgDir, crlfmt, "-fast", "-ignore", ignore, "-tab", "2", ".")
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
			"go.uber.org/atomic":                          "sync/atomic",
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
			for _, pkg := range pkgs {
				for _, s := range pkg.Imports {
					arg.Out <- pkg.PkgPath + ": " + s.PkgPath
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
			stream.GrepNot(`cockroachdb/cockroach/pkg/build/bazel/util/tinystringer: errors$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/build/engflow: github\.com/golang/protobuf/proto$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/build/engflow: log$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/util/grpcutil: github\.com/cockroachdb\/errors\/errbase$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/util/future: github\.com/cockroachdb\/errors\/errbase$`),
			stream.GrepNot(`cockroach/pkg/roachprod/install: syscall$`), // TODO: switch to sysutil
			stream.GrepNot(`cockroach/pkg/util/log: github\.com/pkg/errors$`),
			stream.GrepNot(`cockroach/pkg/(base|release|security|util/(log|randutil|stop)): log$`),
			stream.GrepNot(`cockroach/pkg/(server/serverpb|ts/tspb): github\.com/golang/protobuf/proto$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/rpc: github\.com/golang/protobuf/proto$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/sql/lexbase/allkeywords: log$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/sql/plpgsql/parser/plpgsqllexbase/allkeywords: log$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/util/timeutil/gen: log$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/kv/kvpb/gen: log$`),
			stream.GrepNot(`cockroachdb/cockroach/pkg/util/log/gen: log$`),
			stream.GrepNot(`cockroach/pkg/util/uuid: github\.com/satori/go\.uuid$`),
			// See #132262.
			stream.GrepNot(`github.com/cockroachdb/cockroach/pkg/raft/raftlogger: log$`),
			stream.GrepNot(`github.com/cockroachdb/cockroach/pkg/raft/rafttest: log$`),
			stream.GrepNot(`github.com/cockroachdb/cockroach/pkg/workload/debug: log$`),
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
				case strings.HasSuffix(s, "envutil"):
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

		cmd, stderr, filter, err := dirCmd(crdbDir, "go", "list", "-deps",
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
			"github.com/cockroachdb/cockroach/pkg/kv/kvpb":      {},
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
		excludesPath, err := filepath.Abs(datapathutils.TestDataPath(t, "errcheck_excludes.txt"))
		if err != nil {
			t.Fatal(err)
		}
		// errcheck uses 2GB of ram (as of 2017-07-13), so don't parallelize it.
		cmd, stderr, filter, err := dirCmd(
			crdbDir,
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
		if bazel.BuiltWithBazel() {
			skip.IgnoreLint(t, "the returncheck tests are run during the bazel build")
		}
		// returncheck uses 2GB of ram (as of 2017-07-13), so don't parallelize it.
		cmd, stderr, filter, err := dirCmd(crdbDir, "returncheck", pkgScope)
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

	t.Run("TestStaticCheck", func(t *testing.T) {
		// staticcheck uses 2.4GB of ram (as of 2019-05-10), so don't parallelize it.
		skip.UnderShort(t)
		// If run outside of Bazel, we assume this binary must be in the PATH.
		staticcheck := "staticcheck"
		if bazel.BuiltWithBazel() {
			var err error
			staticcheck, err = bazel.Runfile("external/co_honnef_go_tools/cmd/staticcheck/staticcheck_/staticcheck")
			if err != nil {
				t.Fatal(err)
			}
		}

		// Determine the list of files to exclude."

		cmd, stderr, filter, err := dirCmd(crdbDir, staticcheck, pkgScope)
		if err != nil {
			t.Fatal(err)
		}

		staticcheckCheckNameRe := regexp.MustCompile(`^(S|SA|ST|U)[0-9][0-9][0-9][0-9]$`)
		filters := []stream.Filter{
			filter,
			stream.GrepNot(`\.pb\.go`),
			stream.GrepNot(`\.pb\.gw\.go`),
			// NB: we define a data structure here that mirrors the shape of a stdlib
			// data structure. This causes staticcheck to think fields in the structure
			// are unused, when in fact the runtime uses them.
			stream.GrepNot(`pkg/util/goschedstats/runtime_go1\.19\.go:.*\(U1000\)`),
			// NB: Looks like false positives in this file. Maybe due to the bazel build tag.
			// If this situation gets much worse, we can look at running staticcheck multiple
			// times and merging the results: https://staticcheck.io/docs/running-staticcheck/cli/build-tags/
			// This is more trouble than it's worth right now.
			stream.GrepNot(`pkg/cmd/mirror/go/mirror.go`),
			// As above, the bazel build tag has an impact here.
			stream.GrepNot(`pkg/testutils/docker/single_node_docker_test.go`),
		}
		for analyzerName, config := range nogoConfig {
			if !staticcheckCheckNameRe.MatchString(analyzerName) {
				continue
			}
			// NB: We're not loading only_files because right now we don't need it.
			// This could lead to disagreements between `nogo` and `lint` if things change.
			excludeFiles := config.(map[string]any)["exclude_files"]
			if excludeFiles == nil {
				continue
			}
			for excludeRegexp := range excludeFiles.(map[string]any) {
				excludeRegexp = strings.TrimPrefix(excludeRegexp, "cockroach/")
				excludeRegexp = strings.TrimSuffix(excludeRegexp, "$")
				filters = append(filters, stream.GrepNot(excludeRegexp+`:.*\(`+analyzerName+`\)$`))
			}
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(
			stream.Sequence(filters...), func(s string) {
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
			":!sql/colexecerror/error*.go",
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
			// - coldata.NewVec
			// - coldata.Batch.AppendCol
			// TODO(yuzefovich): prohibit call to coldata.NewMemBatchNoCols.
			`(coldata\.New(MemBatch|MemBatchWithCapacity|Vec)|\.AppendCol)\(`,
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
			t.Errorf("\n%s <- forbidden; use coldata.Vec.Copy or colexecutils.AppendOnlyBufferedBatch", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestColbuilderSimpleProject", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			// We prohibit usage of colexecbase.NewSimpleProjectOp outside of
			// addProjection helper in colbuilder package.
			`colexecbase\.NewSimpleProjectOp`,
			"--",
			"sql/colexec/colbuilder*",
			":!sql/colexec/colbuilder/execplan_util.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use addProjection to prevent type schema corruption", s)
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

		var gcassertPaths []string
		for _, path := range strings.Split(rawGcassertPaths, "\n") {
			path = strings.TrimSpace(path)
			if path == "" {
				continue
			}
			gcassertPaths = append(gcassertPaths, fmt.Sprintf("./pkg/%s", path))
		}

		// Ensure that all packages that have '//gcassert' or '// gcassert'
		// assertions are included into gcassertPaths.
		t.Run("Coverage", func(t *testing.T) {
			t.Parallel()

			cmd, stderr, filter, err := dirCmd(
				pkgDir,
				"git",
				"grep",
				"-nE",
				`// ?gcassert`,
			)
			if err != nil {
				t.Fatal(err)
			}

			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}

			if err := stream.ForEach(stream.Sequence(
				filter,
				stream.GrepNot("sql/colexec/execgen/cmd/execgen/*"),
				stream.GrepNot("sql/colexec/execgen/testdata/*"),
				stream.GrepNot("testutils/lint/lint_test.go"),
			), func(s string) {
				// s here is of the form
				//   util/hlc/timestamp.go:203:// gcassert:inline
				// and we want to extract the package path.
				filePath := s[:strings.Index(s, ":")]                  // up to the line number
				pkgPath := filePath[:strings.LastIndex(filePath, "/")] // up to the file name
				gcassertPath := fmt.Sprintf("./pkg/%s", pkgPath)
				for i := range gcassertPaths {
					if gcassertPath == gcassertPaths[i] {
						return
					}
				}
				t.Errorf("\n%s <- is not enforced, include %q into gcassertPaths", s, gcassertPath)
			}); err != nil {
				t.Error(err)
			}

			if err := cmd.Wait(); err != nil {
				if out := stderr.String(); len(out) > 0 {
					t.Fatalf("err=%s, stderr=%s", err, out)
				}
			}
		})

		var buf strings.Builder
		if err := gcassert.GCAssertCwd(&buf, crdbDir, gcassertPaths...); err != nil {
			t.Fatalf("failed gcassert (%+v):\n%s", err, buf.String())
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

	// TestMapToBool asserts that map[...]bool is not used. In most cases, such
	// a map can be replaced with map[...]struct{} that is more efficient, and
	// this linter nudges folks to do so. This linter can be disabled by
	// '//nolint:maptobool' comment.
	// TODO(yuzefovich): expand the scope where the linter is applied.
	t.Run("TestMapToBool", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`map\[.*\]bool`,
			"--",
			"sql/opt/norm*.go",
			":!*_test.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`nolint:maptobool`),
		), func(s string) {
			t.Errorf("\n%s <- forbidden; use map[...]struct{} instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	// TODO(yuzefovich): remove this linter when #76378 is resolved.
	t.Run("TestTODOTestTenantDisabled", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`base\.TODOTestTenantDisabled`,
			"--",
			"*",
			":!backup/backup_test.go",
			":!backup/backuprand/backup_rand_test.go",
			":!backup/backuptestutils/testutils.go",
			":!backup/create_scheduled_backup_test.go",
			":!backup/datadriven_test.go",
			":!backup/full_cluster_backup_restore_test.go",
			":!backup/restore_old_versions_test.go",
			":!backup/utils_test.go",
			":!ccl/changefeedccl/alter_changefeed_test.go",
			":!ccl/changefeedccl/changefeed_test.go",
			":!ccl/changefeedccl/helpers_test.go",
			":!ccl/changefeedccl/parquet_test.go",
			":!ccl/changefeedccl/scheduled_changefeed_test.go",
			":!ccl/importerccl/ccl_test.go",
			":!ccl/kvccl/kvfollowerreadsccl/boundedstaleness_test.go",
			":!ccl/kvccl/kvfollowerreadsccl/followerreads_test.go",
			":!ccl/kvccl/kvtenantccl/upgradeccl/tenant_upgrade_test.go",
			":!ccl/multiregionccl/cold_start_latency_test.go",
			":!ccl/multiregionccl/datadriven_test.go",
			":!ccl/multiregionccl/multiregionccltestutils/testutils.go",
			":!ccl/multiregionccl/regional_by_row_test.go",
			":!ccl/multiregionccl/unique_test.go",
			":!ccl/partitionccl/drop_test.go",
			":!ccl/partitionccl/partition_test.go",
			":!ccl/partitionccl/zone_test.go",
			":!ccl/serverccl/admin_test.go",
			":!crosscluster/replicationtestutils/testutils.go",
			":!crosscluster/streamclient/partitioned_stream_client_test.go",
			":!crosscluster/physical/replication_random_client_test.go",
			":!crosscluster/physical/stream_ingestion_job_test.go",
			":!crosscluster/physical/stream_ingestion_processor_test.go",
			":!crosscluster/producer/producer_job_test.go",
			":!crosscluster/producer/replication_stream_test.go",
			":!ccl/workloadccl/allccl/all_test.go",
			":!cli/democluster/demo_cluster.go",
			":!cli/democluster/demo_cluster_test.go",
			":!server/application_api/config_test.go",
			":!server/application_api/dbconsole_test.go",
			":!server/application_api/events_test.go",
			":!server/application_api/insights_test.go",
			":!server/application_api/jobs_test.go",
			":!server/application_api/query_plan_test.go",
			":!server/application_api/security_test.go",
			":!server/application_api/zcfg_test.go",
			":!server/grpc_gateway_test.go",
			":!server/multi_store_test.go",
			":!server/storage_api/decommission_test.go",
			":!server/storage_api/health_test.go",
			":!server/storage_api/rangelog_test.go",
			":!server/testserver.go",
			":!sql/importer/import_processor_test.go",
			":!sql/importer/import_stmt_test.go",
			":!sql/importer/read_import_mysql_test.go",
			":!sql/schemachanger/sctest/test_server_factory.go",
			":!sql/server_params_test.go",
			":!sql/ttl/ttljob/ttljob_test.go",
			":!testutils/lint/lint_test.go",
			":!ts/server_test.go",
			":!upgrade/upgrademanager/manager_external_test.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- new usages of base.TODOTestTenantDisabled are forbidden", s)
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
		// Note we retrieve the list of printfuncs from nogo_config.json.
		analyzerFlags := nogoConfig["printf"].(map[string]any)
		printfuncs := analyzerFlags["analyzer_flags"].(map[string]any)["funcs"].(string)
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
			// kvpb's own error package takes ownership of error unwraps
			// (by enforcing that errors can never been wrapped under a
			// kvpb.Error, which is an inconvenient limitation but it is
			// what it is). Once this code is simplified to use generalized
			// error encode/decode, it can be dropped from the linter
			// exception as well.
			stream.GrepNot(`pkg/kv/kvpb/errors\.go:.*invalid direct cast on error object`),
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
			stream.GrepNot(`pkg/util/timeutil/timeout_error\.go:.*invalid direct cast on error object`),
			// Direct error cast OK in this case for a low-dependency helper binary.
			stream.GrepNot(`pkg/cmd/github-pull-request-make/main\.go:.*invalid direct cast on error object`),
			// The logging package translates log.Fatal calls into errors.
			// We can't use the regular exception mechanism via functions.go
			// because addStructured takes its positional argument as []interface{},
			// instead of ...interface{}.
			stream.GrepNot(`pkg/util/log/channels\.go:\d+:\d+: logfDepth\(\): format argument is not a constant expression`),
			stream.GrepNot(`pkg/util/log/channels\.go:\d+:\d+: logfDepthInternal\(\): format argument is not a constant expression`),
			stream.GrepNot(`pkg/util/log/channels\.go:\d+:\d+: untypedVEventfDepth\(\): format argument is not a constant expression`),
			// roachprod/logger is not collecting redactable logs so we don't care
			// about printf hygiene there as much.
			stream.GrepNot(`pkg/roachprod/logger/log\.go:.*format argument is not a constant expression`),
			// We purposefully produce nil dereferences in this file to test crash conditions
			stream.GrepNot(`pkg/util/log/logcrash/crash_reporting_test\.go:.*nil dereference in type assertion`),
			// Temporarily copied code from google-cloud-go's retry predicate.
			stream.GrepNot(`pkg/cloud/gcp/gcs_retry\.go:.*invalid direct cast on error object`),
			// Spawning naked goroutines is ok when it's not as part of the main CRDB
			// binary. This is for now - if we use #58164 to introduce more aggressive
			// pooling, etc, then test code needs to adhere as well.
			stream.GrepNot(nakedGoroutineExceptions + `:.*Use of go keyword not allowed`),
			stream.GrepNot(nakedGoroutineExceptions + `:.*Illegal call to Group\.Go\(\)`),
			// We purposefully dereference nil in this file to test panic handling
			stream.GrepNot(`pkg/cmd/roachtest/roachtestutil/mixedversion/runner_test\.go:.*nil dereference`),
		}

		const vetTool = "roachvet"
		vetToolPath, err := exec.LookPath(vetTool)
		if err != nil {
			t.Fatalf("failed to find %s: %s", vetTool, err)
		}
		vetCmd(t, crdbDir, "go",
			[]string{"vet", "-vettool", vetToolPath, "-all", "-printf.funcs", printfuncs, pkgScope},
			filters)

	})

	t.Run("CODEOWNERS", func(t *testing.T) {
		co, err := codeowners.DefaultLoadCodeOwners()
		require.NoError(t, err)
		const verbose = false
		codeowners.LintEverythingIsOwned(t, verbose, co, crdbDir, "pkg")
	})

	t.Run("cookie construction is forbidden", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\.Cookie\{`,
			"--",
			":!*_test.go",
			":!*authserver/cookie.go",
			":!*roachtest*",
			":!*testserver*",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use constructors in `authserver/cookie.go` instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	// This is a weird restriction we impose so that lints don't
	// accidentally get skipped on some packages due to #124154.
	// Namely, we use the string `external/` to filter out third-party
	// code from lint analysis, and that check would exclude first-party
	// packages if any ended in the word "external".
	t.Run("TestExternalPackageName", func(t *testing.T) {
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "ls-files", "*external*")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		filters := []stream.Filter{
			filter,
			stream.Grep(`\.go$`),
			stream.Map(filepath.Dir),
			stream.Uniq(),
		}
		if err := stream.ForEach(
			stream.Sequence(filters...), func(pkgName string) {
				pkgBase := filepath.Base(pkgName)
				if strings.HasSuffix(pkgBase, "external") {
					t.Errorf("package name cannot end in the string 'external': found package pkg/%s\n", pkgName)
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

	t.Run("TestNoEnumeratingAllTables", func(t *testing.T) {
		t.Parallel()
		const (
			// sysTableExample and virtTableExample are the names of a system and
			// virtual tables respectively, that have been chosen to serve as
			// indicators, if they are detected in a test, that that test may be
			// enumerating *all* system or virtual tables which is generally
			// undesirable outside of a few specific allow-listed cases. There is
			// nothing special about these two tables other than that they are not
			// directly referenced in tests other than those deliberately enumerating
			// all tables, so they're well-suited for this purpose. We could add
			// others here as well if needed, and add exemptions if one of these is
			// intentionally used in a test.
			sysTableExample  = "span_stats_buckets"
			virtTableExample = "logical_replication_node_processors"
		)
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			"-e", sysTableExample,
			"-e", virtTableExample,
			"--",
			"**testdata**",
			"**/*_test.go",
			":!testutils/lint/lint_test.go",     // false-positive: the lint itself.
			":!sql/tests/testdata/initial_keys", // exempt: deliberate test of bootstrap catalog
			":!sql/catalog/systemschema_test/testdata/bootstrap*",  // exempt: deliberate test of bootstrap catalog.
			":!sql/catalog/internal/catkv/testdata/",               // TODO(foundations): #137029.
			":!cli/testdata/doctor/",                               // TODO(foundations): #137030.
			":!cmd/roachtest/testdata/regression.diffs",            // TODO(queries): #137026.
			":!cli/testdata/zip/file-filters/testzip_file_filters", // exempt: deliberate test to fetch all tables in debug zip.
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- is this test enumerating all system or internal tables? see https://go.crdb.dev/p/overly-broad-test", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	// Test forbidden roachtest imports.
	t.Run("TestRoachtestForbiddenImports", func(t *testing.T) {
		t.Parallel()

		roachprodLoggerPkg := "github.com/cockroachdb/cockroach/pkg/roachprod/logger"
		roachtestTaskPkg := "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
		// forbiddenImportPkg -> permittedReplacementPkg
		forbiddenImports := map[string]string{
			"github.com/cockroachdb/cockroach/pkg/util/log": roachprodLoggerPkg,
			"log": roachprodLoggerPkg,
			"github.com/cockroachdb/cockroach/pkg/util/ctxgroup": roachtestTaskPkg,
			"golang.org/x/sync/errgroup":                         roachtestTaskPkg,
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
			for _, pkg := range pkgs {
				for _, s := range pkg.Imports {
					arg.Out <- pkg.PkgPath + ": " + s.PkgPath
				}
			}
			return nil
		})
		numAnalyzed := 0
		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.Sort(),
			stream.Uniq(),
			stream.Grep(`cockroach/pkg/cmd/roachtest/(tests|operations): `),
		), func(s string) {
			pkgStr := strings.Split(s, ": ")
			_, importedPkg := pkgStr[0], pkgStr[1]
			numAnalyzed++

			// Test that a disallowed package is not imported.
			if replPkg, ok := forbiddenImports[importedPkg]; ok {
				t.Errorf("\n%s <- please use %q instead of %q", s, replPkg, importedPkg)
			}
		}); err != nil {
			t.Error(err)
		}
		if numAnalyzed == 0 {
			t.Errorf("Empty input! Please check the linter.")
		}
	})

	t.Run("TestRedactUnsafe", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\redact\.Unsafe\(`,
			"--",
			"*.go",
			":!util/encoding/encoding.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'encoding.Unsafe()' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestDebugStack", func(t *testing.T) {
		t.Parallel()

		excludeFiles := []string{
			":!util/debugutil/debugutil.go",
			":!server/debug/goroutineui/dump_test.go",
		}

		cmd, stderr, filter, err := dirCmd(pkgDir, "git", append([]string{
			"grep", "-nE", `debug\.Stack\(`, "--", "*.go",
		}, excludeFiles...)...)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'debugutil.Stack()' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	// This test verifies that all version-specific tests in pkg/upgrade/upgrades
	// contain a clusterversion.SkipWhenMinSupportedVersionIsAtLeast() check. This
	// check makes it easier to bump the minimum supported version (specifically
	// it allows cleaning up the deprecated upgrades in a separate PR).
	t.Run("TestUpgradesTestsCheckVersion", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-oEh",
			`^func Test[^(]*|clusterversion.SkipWhenMinSupportedVersionIsAtLeast`,
			"--",
			"upgrade/upgrades/v[0-9]*_test.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		testExpectingSkip := ""
		if err := stream.ForEach(filter, func(s string) {
			if strings.HasPrefix(s, "func Test") {
				if testExpectingSkip != "" {
					t.Errorf("\n%s is missing a clusterversion.SkipWhenMinSupportedVersionIsAtLeast() check", testExpectingSkip)
				}
				testExpectingSkip = strings.TrimPrefix(s, "func ")
			} else {
				if !strings.Contains(s, "clusterversion.SkipWhenMinSupportedVersionIsAtLeast") {
					panic("unexpected line: " + s)
				}
				testExpectingSkip = ""
			}
		}); err != nil {
			t.Error(err)
		}
		if testExpectingSkip != "" {
			t.Errorf("\n%s is missing a clusterversion.SkipWhenMinSupportedVersionIsAtLeast() check", testExpectingSkip)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})
}
