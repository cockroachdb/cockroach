// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/google/shlex"
	"github.com/spf13/cobra"
)

const (
	// General testing flags.
	changedFlag      = "changed"
	countFlag        = "count"
	vFlag            = "verbose"
	showLogsFlag     = "show-logs"
	stressFlag       = "stress"
	raceFlag         = "race"
	deadlockFlag     = "deadlock"
	ignoreCacheFlag  = "ignore-cache"
	rewriteFlag      = "rewrite"
	streamOutputFlag = "stream-output"
	testArgsFlag     = "test-args"
	vModuleFlag      = "vmodule"
	showDiffFlag     = "show-diff"
)

// List of bazel integration tests that will fail when running `dev test pkg/...`
var integrationTests = map[string]struct{ testName, commandToRun string }{
	"pkg/acceptance":              {"acceptance_test", "dev acceptance"},
	"pkg/compose":                 {"compose_test", "dev compose"},
	"pkg/compose/compare/compare": {"compare_test", "dev compose"},
	"pkg/testutils/docker":        {"docker_test", ""},
	"pkg/testutils/lint":          {"lint_test", "dev lint"},
}

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

    dev test --race --count 250 ...
        Run a test under race, 250 times in parallel

    dev test pkg/spanconfig/... --test-args '-test.trace=trace.out'
        Pass arguments to go test (see 'go help testflag'; prefix args with '-test.{arg}')

    dev test pkg/spanconfig --stress
        Run pkg/spanconfig test repeatedly in parallel up to 1000 times until it fails

    dev test pkg/spanconfig --stress --count 2000
        Same as above, but run 2000 times instead of 1000

    timeout 60 dev test pkg/spanconfig --stress --count 2000
        Same as above, but time out after 60 seconds if no test has failed
          (Note: the timeout command is called "gtimeout" on macOS and can be installed with "brew install coreutils")

    dev test pkg/cmd/dev:dev_test --stress --timeout 5s
        Run a test repeatedly until it runs longer than 5s

    end=$((SECONDS+N))
    while [ $SECONDS -lt $end ]; do
        dev test pkg/cmd/dev:dev_test --stress
    done
        Run a test repeatedly until at least N seconds have passed (useful if "dev test --stress" ends too quickly and you want to keep the test running for a while)

    dev test pkg/server -f=TestSpanStatsResponse -v --count=5 --vmodule='raft=1'
        Run a specific test, multiple times, with increased logging verbosity

    dev test pkg/server -- --test_env=COCKROACH_RANDOM_SEED=1234
        Run a test with a specified seed by passing the --test_env flag directly to bazel
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
	testCmd.Flags().Int(countFlag, 0, "run test the given number of times")
	testCmd.Flags().BoolP(showLogsFlag, "", false, "show crdb logs in-line")
	testCmd.Flags().Bool(stressFlag, false, "run tests under stress")
	testCmd.Flags().Bool(raceFlag, false, "run tests using race builds")
	testCmd.Flags().Bool(deadlockFlag, false, "run tests using the deadlock detector")
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
			if err := sendBepDataToBeaverHubIfNeeded(filepath.Join(tmpDir, bepFileBasename)); err != nil && d.debug {
				log.Printf("Internal Error: Sending BEP file to beaver hub failed - %v", err)
			}
		}
		if !buildutil.CrdbTestBuild {
			_ = os.RemoveAll(tmpDir)
		}
	}()
	pkgs, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	var (
		filter       = mustGetFlagString(cmd, filterFlag)
		ignoreCache  = mustGetFlagBool(cmd, ignoreCacheFlag)
		race         = mustGetFlagBool(cmd, raceFlag)
		deadlock     = mustGetFlagBool(cmd, deadlockFlag)
		rewrite      = mustGetFlagBool(cmd, rewriteFlag)
		streamOutput = mustGetFlagBool(cmd, streamOutputFlag)
		testArgs     = mustGetFlagString(cmd, testArgsFlag)
		short        = mustGetFlagBool(cmd, shortFlag)
		stress       = mustGetFlagBool(cmd, stressFlag)
		timeout      = mustGetFlagDuration(cmd, timeoutFlag)
		verbose      = mustGetFlagBool(cmd, vFlag)
		changed      = mustGetFlagBool(cmd, changedFlag)
		showLogs     = mustGetFlagBool(cmd, showLogsFlag)
		count        = mustGetFlagInt(cmd, countFlag)
		vModule      = mustGetFlagString(cmd, vModuleFlag)
		showDiff     = mustGetFlagBool(cmd, showDiffFlag)

		// These are tests that require access to another directory for
		// --rewrite. These can either be single directories or
		// recursive directories ending in /...
		extraRewritablePaths = []struct{ pkg, path string }{
			{"pkg/ccl/logictestccl", "pkg/sql/logictest"},
			{"pkg/ccl/logictestccl", "pkg/sql/opt/exec/execbuilder"},
			{"pkg/ccl/schemachangerccl", "pkg/sql/schemachanger/testdata"},
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

	if stress && streamOutput {
		return fmt.Errorf("cannot combine --%s and --%s", stressFlag, streamOutputFlag)
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
	var goTags []string
	args = append(args, "test")
	addCommonBazelArguments(&args)
	if race {
		args = append(args, "--config=race", "--test_sharding_strategy=disabled")
	}
	if deadlock {
		goTags = append(goTags, "deadlock")
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

		if !strings.HasPrefix(pkg, "pkg/") && !strings.HasPrefix(pkg, "pkg:") {
			return fmt.Errorf("malformed package %q, expecting %q", pkg, "pkg/{...}")
		}

		if !strings.Contains(pkg, ":") && !strings.HasSuffix(pkg, "/...") {
			pkg = fmt.Sprintf("%s:all", pkg)
		}
		// Filter out only test targets.
		queryArgs := []string{"query", fmt.Sprintf("kind(.*_test, %s)", pkg)}
		labelsBytes, err := d.exec.CommandContextSilent(ctx, "bazel", queryArgs...)
		if err != nil {
			return fmt.Errorf("could not query for tests within %s: got error %w", pkg, err)
		}
		labels := strings.TrimSpace(string(labelsBytes))
		if labels == "" {
			log.Printf("WARNING: no test targets were found matching %s", pkg)
			continue
		}
		testTargets = append(testTargets, strings.Split(labels, "\n")...)
	}

	for _, target := range testTargets {
		testTarget := strings.Split(target, ":")
		integrationTest, ok := integrationTests[testTarget[0]]
		if ok {
			// If the test targets all tests in the package or the individual test, warn the user
			if testTarget[1] == "all" || testTarget[1] == integrationTest.testName {
				if integrationTest.commandToRun == "" {
					return fmt.Errorf("%s:%s will fail since it is an integration test", testTarget[0], integrationTest.testName)
				}
				return fmt.Errorf("%s:%s will fail since it is an integration test. To run this test, run `%s`", testTarget[0], integrationTest.testName, integrationTest.commandToRun)
			}
		}
	}

	if len(testTargets) == 0 {
		log.Printf("WARNING: no matching test targets were found and no tests will be run")
		return nil
	}

	args = append(args, testTargets...)
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
	if timeout > 0 {
		args = append(args, fmt.Sprintf("--test_timeout=%d", int(timeout.Seconds())))
	}

	if stress {
		if count == 0 {
			// Default to 1000 unless a different count was provided.
			count = 1000
		}
		args = append(args,
			"--test_env=COCKROACH_STRESS=true",
			"--notest_keep_going",
		)
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
	if count == 1 {
		ignoreCache = true
	} else if count != 0 {
		args = append(args, fmt.Sprintf("--runs_per_test=%d", count), "--runs_per_test=.*disallowed_imports_test@1")
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

	if ignoreCache {
		args = append(args, "--nocache_test_results")
	}

	if len(goTags) > 0 {
		args = append(args, "--define", "gotags=bazel,gss,"+strings.Join(goTags, ","))
	}
	args = append(args, d.getTestOutputArgs(verbose, showLogs, streamOutput)...)
	args = append(args, additionalBazelArgs...)
	logCommand("bazel", args...)
	if stress {
		d.warnAboutChangeInStressBehavior(timeout)
	}

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
			err = nil
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

func (d *dev) getTestOutputArgs(verbose, showLogs, streamOutput bool) []string {
	testOutputArgs := []string{"--test_output", "errors"}
	if streamOutput {
		// Stream the output to continually observe the number of successful
		// test iterations.
		testOutputArgs = []string{"--test_output", "streamed"}
	} else if verbose || showLogs {
		testOutputArgs = []string{"--test_output", "all"}
	}
	return testOutputArgs
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
	base, err := d.getMergeBaseHash(ctx)
	if err != nil {
		return nil, err
	}
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
