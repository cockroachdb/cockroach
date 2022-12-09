// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/google/shlex"
	"github.com/spf13/cobra"
)

const (
	stressTarget = "@com_github_cockroachdb_stress//:stress"

	// General testing flags.
	changedFlag      = "changed"
	countFlag        = "count"
	vFlag            = "verbose"
	showLogsFlag     = "show-logs"
	stressFlag       = "stress"
	stressArgsFlag   = "stress-args"
	raceFlag         = "race"
	ignoreCacheFlag  = "ignore-cache"
	rewriteFlag      = "rewrite"
	streamOutputFlag = "stream-output"
	testArgsFlag     = "test-args"
	vModuleFlag      = "vmodule"
	showDiffFlag     = "show-diff"
)

func makeTestCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	// testCmd runs the specified cockroachdb tests.
	testCmd := &cobra.Command{
		Use:   "test [pkg..]",
		Short: `Run the specified tests`,
		Long: `Run the specified tests.

This command takes a list of packages and tests all of them. It's also
permissive enough to accept bazel build targets (like
pkg/kv/kvserver:kvserver_test) instead.`,
		Example: `
    dev test pkg/kv/kvserver --filter=TestReplicaGC*
        Run the tests that match with 'TestReplicaGC*' in the kvserver package.

    dev test pkg/... -v
        Increase test verbosity. Shows bazel and go test process output.

    dev test pkg/spanconfig/... pkg/ccl/spanconfigccl/...
        Test multiple packages recursively

    dev test --race --stress ...
        Run a test under race and stress

    dev test pkg/spanconfig/... --test-args '-test.trace=trace.out'
        Pass arguments to go test (see 'go help testflag'; prefix args with '-test.{arg}')

    dev test pkg/spanconfig --stress --stress-args '-maxruns 1000 -p 4'
        Pass arguments to github.com/cockroachdb/stress

    dev test --stress --timeout=1m --test-args='-test.timeout 5s'
        Stress for 1m until a test runs longer than 5s

    dev test pkg/server -f=TestSpanStatsResponse -v --count=5 --vmodule='raft=1'
`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
	// Attach flags for the test sub-command.
	addCommonBuildFlags(testCmd)
	addCommonTestFlags(testCmd)

	// Go's test runner runs tests in sub-processes; the stderr/stdout data from
	// the test process is first swallowed by go test and then only
	// conditionally released to the invoking user depending on flags passed to
	// `go test`. The `-v` switch controls whether `go test` shows the test
	// process' output (which test is being run, how long it took, etc.) always,
	// or only on failures. `--show-logs` by contrast is a flag for the process
	// under test, controlling whether the process-internal logs are made
	// visible.
	testCmd.Flags().BoolP(vFlag, "v", false, "show testing process output")
	testCmd.Flags().Bool(changedFlag, false, "automatically determine tests to run. This is done on a best-effort basis by asking git which files have changed. Only .go files and files in testdata/ directories are factored into this analysis.")
	testCmd.Flags().Int(countFlag, 1, "run test the given number of times")
	testCmd.Flags().BoolP(showLogsFlag, "", false, "show crdb logs in-line")
	testCmd.Flags().Bool(stressFlag, false, "run tests under stress")
	testCmd.Flags().String(stressArgsFlag, "", "additional arguments to pass to stress")
	testCmd.Flags().Bool(raceFlag, false, "run tests using race builds")
	testCmd.Flags().Bool(ignoreCacheFlag, false, "ignore cached test runs")
	testCmd.Flags().Bool(rewriteFlag, false, "rewrite test files using results from test run (only applicable to certain tests)")
	testCmd.Flags().Bool(streamOutputFlag, false, "stream test output during run")
	testCmd.Flags().String(testArgsFlag, "", "additional arguments to pass to the go test binary")
	testCmd.Flags().String(vModuleFlag, "", "comma-separated list of pattern=N settings for file-filtered logging")
	testCmd.Flags().Bool(showDiffFlag, false, "generate a diff for expectation mismatches when possible")
	return testCmd
}

func (d *dev) test(cmd *cobra.Command, commandLine []string) error {
	var tmpDir string
	if !buildutil.CrdbTestBuild {
		// tmpDir will contain the build event binary file if produced.
		var err error
		tmpDir, err = os.MkdirTemp("", "")
		if err != nil {
			return err
		}
	}
	defer func() {
		if err := sendBepDataToBeaverHubIfNeeded(filepath.Join(tmpDir, bepFileBasename)); err != nil {
			// Retry.
			if err := sendBepDataToBeaverHubIfNeeded(filepath.Join(tmpDir, bepFileBasename)); err != nil {
				log.Printf("Interal Error: Sending BEP file to beaver hub failed - %v", err)
			}
		}
		if !buildutil.CrdbTestBuild {
			_ = os.RemoveAll(tmpDir)
		}
	}()
	pkgs, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	var (
		filter        = mustGetFlagString(cmd, filterFlag)
		ignoreCache   = mustGetFlagBool(cmd, ignoreCacheFlag)
		race          = mustGetFlagBool(cmd, raceFlag)
		rewrite       = mustGetFlagBool(cmd, rewriteFlag)
		streamOutput  = mustGetFlagBool(cmd, streamOutputFlag)
		testArgs      = mustGetFlagString(cmd, testArgsFlag)
		short         = mustGetFlagBool(cmd, shortFlag)
		stress        = mustGetFlagBool(cmd, stressFlag)
		stressCmdArgs = mustGetFlagString(cmd, stressArgsFlag)
		timeout       = mustGetFlagDuration(cmd, timeoutFlag)
		verbose       = mustGetFlagBool(cmd, vFlag)
		changed       = mustGetFlagBool(cmd, changedFlag)
		showLogs      = mustGetFlagBool(cmd, showLogsFlag)
		count         = mustGetFlagInt(cmd, countFlag)
		vModule       = mustGetFlagString(cmd, vModuleFlag)
		showDiff      = mustGetFlagBool(cmd, showDiffFlag)

		// These are tests that require access to another directory for
		// --rewrite. These can either be single directories or
		// recursive directories ending in /...
		extraRewritablePaths = []struct{ pkg, path string }{
			{"pkg/ccl/logictestccl", "pkg/sql/logictest"},
			{"pkg/ccl/logictestccl", "pkg/sql/opt/exec/execbuilder"},
			{"pkg/sql/opt/memo", "pkg/sql/opt/testutils/opttester/testfixtures"},
			{"pkg/sql/opt/norm", "pkg/sql/opt/testutils/opttester/testfixtures"},
			{"pkg/sql/opt/xform", "pkg/sql/opt/testutils/opttester/testfixtures"},
			{"pkg/sql/opt/exec/explain", "pkg/sql/opt/testutils/opttester/testfixtures"},
		}

		logicTestPaths = []string{
			"pkg/sql/logictest/tests",
			"pkg/ccl/logictestccl/tests",
			"pkg/sql/opt/exec/execbuilder/tests",
			"pkg/sql/sqlitelogictest/tests",
			"pkg/ccl/sqlitelogictestccl/tests",
		}
	)

	var disableTestSharding bool
	if changed {
		if rewrite {
			return fmt.Errorf("cannot combine --%s and --%s", changedFlag, rewriteFlag)
		}
		targets, err := d.determineAffectedTargets(ctx)
		if err != nil {
			return err
		}
		pkgs = append(pkgs, targets...)
		if len(pkgs) == 0 {
			log.Printf("WARNING: no affected tests found")
			return nil
		}
	}

	if rewrite {
		ignoreCache = true
		disableTestSharding = true
	}

	// Enumerate all tests to run.
	if len(pkgs) == 0 {
		// Empty `dev test` does the same thing as `dev test pkg/...`
		pkgs = append(pkgs, "pkg/...")
	}

	var args []string
	args = append(args, "test")
	if numCPUs != 0 {
		args = append(args, fmt.Sprintf("--local_cpu_resources=%d", numCPUs))
	}
	if race {
		args = append(args, "--config=race")
	} else if stress {
		disableTestSharding = true
	}

	var testTargets []string
	for _, pkg := range pkgs {
		if pkg == "pkg/..." {
			// Special-cased target to skip known broken-under-bazel and
			// integration tests.
			testTargets = append(testTargets, "//pkg:all_tests")
			continue
		}

		pkg = strings.TrimPrefix(pkg, "//")
		pkg = strings.TrimPrefix(pkg, "./")
		pkg = strings.TrimRight(pkg, "/")

		if !strings.HasPrefix(pkg, "pkg/") {
			return fmt.Errorf("malformed package %q, expecting %q", pkg, "pkg/{...}")
		}

		var target string
		if strings.Contains(pkg, ":") || strings.HasSuffix(pkg, "/...") {
			// For parity with bazel, we allow specifying named build targets.
			target = pkg
		} else {
			target = fmt.Sprintf("%s:all", pkg)
		}
		testTargets = append(testTargets, target)
	}

	// Stressing is specifically for go tests. Take a second here to filter
	// only the go_test targets so we don't end up stressing e.g. a
	// disallowed_imports_test.
	if stress {
		query := fmt.Sprintf("kind('go_test', %s)", strings.Join(testTargets, " + "))
		goTestLines, err := d.exec.CommandContextSilent(ctx, "bazel", "query", "--output=label", query)
		if err != nil {
			return err
		}
		testTargets = strings.Split(strings.TrimSpace(string(goTestLines)), "\n")
		if len(testTargets) == 0 {
			log.Printf("WARNING: no tests found")
			return nil
		}
	}

	args = append(args, testTargets...)
	if ignoreCache {
		args = append(args, "--nocache_test_results")
	}
	args = append(args, "--test_env=GOTRACEBACK=all")

	if rewrite {
		if stress {
			return fmt.Errorf("cannot combine --%s and --%s", stressFlag, rewriteFlag)
		}
		workspace, err := d.getWorkspace(ctx)
		if err != nil {
			return err
		}
		args = append(args, fmt.Sprintf("--test_env=COCKROACH_WORKSPACE=%s", workspace))
		args = append(args, "--test_arg", "-rewrite")
		for _, testTarget := range testTargets {
			dir := getDirectoryFromTarget(testTarget)
			var logicTestParentDir string
			for _, logicTestPath := range logicTestPaths {
				if strings.Contains(testTarget, logicTestPath) {
					logicTestParentDir = logicTestPath
					break
				}
			}
			if logicTestParentDir != "" {
				args = append(args, fmt.Sprintf("--sandbox_writable_path=%s", filepath.Join(workspace, filepath.Join(filepath.Dir(logicTestParentDir), "testdata"))))
			} else {
				args = append(args, fmt.Sprintf("--sandbox_writable_path=%s", filepath.Join(workspace, dir)))
			}
			for _, extraRewritablePath := range extraRewritablePaths {
				if strings.Contains(testTarget, extraRewritablePath.pkg) {
					// Some targets need special handling if they rewrite outside of
					// their own testdata directory.
					args = append(args, fmt.Sprintf("--sandbox_writable_path=%s",
						filepath.Join(workspace, extraRewritablePath.path)))
				}
			}
		}
	}
	if showDiff {
		args = append(args, "--test_arg", "-show-diff")
	}
	if timeout > 0 && !stress {
		// If stress is specified, we'll pad the timeout differently below.

		// The bazel timeout should be higher than the timeout passed to the
		// test binary (giving it ample time to clean up, 5 seconds is probably
		// enough).
		args = append(args, fmt.Sprintf("--test_timeout=%d", 5+int(timeout.Seconds())))
		args = append(args, "--test_arg", fmt.Sprintf("-test.timeout=%s", timeout.String()))

		// If --test-args '-test.timeout=X' is specified as well, or
		// -- --test_arg '-test.timeout=X', that'll take precedence further
		// below.
	}

	if stress {
		args = append(args, d.getStressArgs(stressCmdArgs, timeout)...)
	}

	if filter != "" {
		args = append(args, fmt.Sprintf("--test_filter=%s", filter))
		// For sharded test packages, it doesn't make much sense to spawn multiple
		// test processes that don't end up running anything. Default to running
		// things in a single process if a filter is specified.
		disableTestSharding = true
	}
	if short {
		args = append(args, "--test_arg", "-test.short")
	}
	if verbose {
		args = append(args, "--test_arg", "-test.v")
	}
	if showLogs {
		args = append(args, "--test_arg", "-show-logs")
	}
	if count != 1 {
		args = append(args, "--test_arg", fmt.Sprintf("-test.count=%d", count))
	}
	if vModule != "" {
		args = append(args, "--test_arg", fmt.Sprintf("-vmodule=%s", vModule))
	}
	if testArgs != "" {
		goTestArgs, err := d.getGoTestArgs(ctx, testArgs)
		if err != nil {
			return err
		}
		args = append(args, goTestArgs...)
	}
	if disableTestSharding {
		args = append(args, "--test_sharding_strategy=disabled")
	}

	args = append(args, d.getTestOutputArgs(stress, verbose, showLogs, streamOutput)...)
	args = append(args, additionalBazelArgs...)
	logCommand("bazel", args...)

	// Do not log --build_event_binary_file=... because it is not relevant to the actual call
	// from the user perspective.
	if buildutil.CrdbTestBuild {
		args = append(args, "--build_event_binary_file=/tmp/path")
	} else {
		args = append(args, fmt.Sprintf("--build_event_binary_file=%s", filepath.Join(tmpDir, bepFileBasename)))
	}

	err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
	if err != nil {
		var cmderr *exec.ExitError
		if errors.As(err, &cmderr) && cmderr.ProcessState.ExitCode() == 4 {
			// If the exit code is 4, the build was successful but no tests were found.
			// ref: https://docs.bazel.build/versions/0.21.0/guide.html#what-exit-code-will-i-get
			log.Printf("WARNING: the build succeeded but no tests were found.")
			return nil
		}
	}
	return err

	// TODO(irfansharif): Both here and in `dev bench`, if the command is
	// unsuccessful we could explicitly check for "missing package" errors. The
	// situation is not so bad currently however:
	//
	//   [...] while parsing 'pkg/f:all': no such package 'pkg/f'
}

func (d *dev) getGoTestArgs(ctx context.Context, testArgs string) ([]string, error) {
	var goTestArgs []string
	if testArgs != "" {
		// The test output directory defaults to wherever "go test" is running,
		// which for us is somewhere in the sandbox. When passing arguments for
		// go test (-{mem,cpu}profile, -trace for e.g.), we may be interested in
		// output files. For that reason, configure it to write (if anything) to the
		// workspace by default.
		//
		// TODO(irfansharif): We could also elevate the go test flags that do write
		// output into top-level dev flags (like --rewrite) and only selectively
		// configure -test.outputdir and the sandbox writable path.
		workspace, err := d.getWorkspace(ctx)
		if err != nil {
			return nil, err
		}
		goTestArgs = append(goTestArgs,
			"--test_arg", fmt.Sprintf("-test.outputdir=%s", workspace),
			fmt.Sprintf("--sandbox_writable_path=%s", filepath.Join(workspace)),
		)

		parts, err := shlex.Split(testArgs)
		if err != nil {
			return nil, err
		}
		for _, part := range parts {
			goTestArgs = append(goTestArgs, "--test_arg", part)
		}
	}
	return goTestArgs, nil
}

func (d *dev) getTestOutputArgs(stress, verbose, showLogs, streamOutput bool) []string {
	testOutputArgs := []string{"--test_output", "errors"}
	if stress || streamOutput {
		// Stream the output to continually observe the number of successful
		// test iterations.
		testOutputArgs = []string{"--test_output", "streamed"}
	} else if verbose || showLogs {
		testOutputArgs = []string{"--test_output", "all"}
	}
	return testOutputArgs
}

func (d *dev) getStressArgs(stressCmdArg string, timeout time.Duration) []string {
	var stressArgs, stressCmdArgs []string
	if timeout > 0 {
		stressCmdArgs = append(stressCmdArgs, fmt.Sprintf("-maxtime=%s", timeout))
		// The bazel timeout should be higher than the stress duration, lets
		// generously give it an extra minute.
		stressArgs = append(stressArgs, fmt.Sprintf("--test_timeout=%d", int((timeout+time.Minute).Seconds())))
	} else {
		// We're running under stress and no timeout is specified. We want
		// to respect the timeout passed down to stress[1]. Similar to above
		// we want the bazel timeout to be longer, so lets just set it to
		// 24h.
		//
		// [1]: Through --stress-arg=-maxtime or if nothing is specified,
		//      -maxtime=0 is taken as "run forever".
		stressArgs = append(stressArgs, fmt.Sprintf("--test_timeout=%.0f", 24*time.Hour.Seconds()))
	}
	if numCPUs > 0 {
		stressCmdArgs = append(stressCmdArgs, fmt.Sprintf("-p=%d", numCPUs))
	}
	if stressCmdArg != "" {
		stressCmdArgs = append(stressCmdArgs, stressCmdArg)
	}
	stressArgs = append(stressArgs, "--test_env=COCKROACH_STRESS=true")
	stressArgs = append(stressArgs, "--run_under",
		// NB: Run with -bazel, which propagates `TEST_TMPDIR` to `TMPDIR`,
		// and -shardable-artifacts set such that we can merge the XML output
		// files.
		fmt.Sprintf("%s -bazel -shardable-artifacts 'XML_OUTPUT_FILE=%s merge-test-xmls' %s", stressTarget, d.getDevBin(), strings.Join(stressCmdArgs, " ")))
	return stressArgs
}

func getDirectoryFromTarget(target string) string {
	target = strings.TrimPrefix(target, "//")
	if strings.HasSuffix(target, "/...") {
		return strings.TrimSuffix(target, "/...")
	}
	colon := strings.LastIndex(target, ":")
	if colon < 0 {
		return target
	}
	return target[:colon]
}

func (d *dev) determineAffectedTargets(ctx context.Context) ([]string, error) {
	// List files changed against `master`.
	remotes, err := d.exec.CommandContextSilent(ctx, "git", "remote", "-v")
	if err != nil {
		return nil, err
	}
	var upstream string
	for _, remote := range strings.Split(strings.TrimSpace(string(remotes)), "\n") {
		if (strings.Contains(remote, "github.com/cockroachdb/cockroach") || strings.Contains(remote, "github.com:cockroachdb/cockroach")) && strings.HasSuffix(remote, "(fetch)") {
			upstream = strings.Fields(remote)[0]
			break
		}
	}
	if upstream == "" {
		return nil, fmt.Errorf("could not find git upstream")
	}

	baseBytes, err := d.exec.CommandContextSilent(ctx, "git", "merge-base", fmt.Sprintf("%s/master", upstream), "HEAD")
	if err != nil {
		return nil, err
	}
	base := strings.TrimSpace(string(baseBytes))

	changedFiles, err := d.exec.CommandContextSilent(ctx, "git", "diff", "--no-ext-diff", "--name-only", base, "--", "*.go", "**/testdata/**")
	if err != nil {
		return nil, err
	}
	trimmedOutput := strings.TrimSpace(string(changedFiles))
	if trimmedOutput == "" {
		return nil, nil
	}
	changedFilesList := strings.Split(trimmedOutput, "\n")
	// Each file in this list needs to be munged somewhat to match up to the
	// Bazel target syntax.
	for idx, file := range changedFilesList {
		if strings.HasSuffix(file, ".go") {
			changedFilesList[idx] = "//" + filepath.Dir(file) + ":" + filepath.Base(file)
		} else {
			// Otherwise this is a testdata file.
			testdataIdx := strings.LastIndex(file, "/testdata/")
			if testdataIdx < 0 {
				return nil, fmt.Errorf("cannot parse testdata file %s; this is a bug", file)
			}
			pkg := file[:testdataIdx]
			testdataFile := file[testdataIdx+len("/testdata/"):]
			changedFilesList[idx] = "//" + pkg + ":testdata/" + testdataFile
		}
	}

	// List the targets affected by these changed files.
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return nil, err
	}
	subdirs, err := d.os.ListSubdirectories(filepath.Join(workspace, "pkg"))
	if err != nil {
		return nil, err
	}
	var dirsToQuery []string
	for _, subdir := range subdirs {
		if subdir == "ui" {
			continue
		}
		dirsToQuery = append(dirsToQuery, fmt.Sprintf("//pkg/%s/...", subdir))
	}
	// The repetition of `kind('go_test')` here is kind of odd, but I've
	// noticed the produced list of targets includes non-go_test targets if
	// we don't include the outer call.
	query := fmt.Sprintf("kind('go_test', rdeps(kind('go_test', %s), %s))", strings.Join(dirsToQuery, " + "), strings.Join(changedFilesList, " + "))
	tests, err := d.exec.CommandContextSilent(ctx, "bazel", "query", query, "--output=label")
	if err != nil {
		return nil, err
	}
	return strings.Split(strings.TrimSpace(string(tests)), "\n"), nil
}
