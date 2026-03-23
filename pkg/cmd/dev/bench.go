// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"go/parser"
	"go/token"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/benchdoc"
	"github.com/spf13/cobra"
)

const (
	benchTimeFlag           = "bench-time"
	benchMemFlag            = "bench-mem"
	runSepProcessTenantFlag = "run-sep-process-tenant"
)

// makeBenchCmd constructs the subcommand used to run the specified benchmarks.
func makeBenchCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	benchCmd := &cobra.Command{
		Use:   "bench [pkg...]",
		Short: `Run the specified benchmarks`,
		Long: `Run the specified benchmarks.

Note that by default we explicitly restrict the benchmark to running on a single core (i.e., GOMAXPROCS=1).
This behavior can be overridden with --test-args='-test.cpu N'`,
		Example: `
	dev bench pkg/sql/parser --filter=BenchmarkParse
	dev bench pkg/bench -f='BenchmarkTracing/1node/scan/trace=off' --count=2 --bench-time=10x
	dev bench pkg/bench -f='BenchmarkTracing/1node/scan/trace=off' --ignore-cache --test-args='-test.cpuprofile=cpu.out -test.memprofile=mem.out' --bench-mem=false`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
	addCommonBuildFlags(benchCmd)
	addCommonTestFlags(benchCmd)

	benchCmd.Flags().BoolP(vFlag, "v", false, "show benchmark process output")
	benchCmd.Flags().BoolP(showLogsFlag, "", false, "show crdb logs in-line")
	benchCmd.Flags().Int(countFlag, 1, "run benchmark n times")
	benchCmd.Flags().Bool(ignoreCacheFlag, true, "ignore cached benchmark runs")
	// We use a string flag for benchtime instead of a duration; the go test
	// runner accepts input of the form "Nx" to run the benchmark N times (see
	// `go help testflag`).
	benchCmd.Flags().String(benchTimeFlag, "", "duration to run each benchmark for")
	benchCmd.Flags().Bool(benchMemFlag, true, "print memory allocations for benchmarks")
	benchCmd.Flags().Bool(streamOutputFlag, true, "stream bench output during run")
	benchCmd.Flags().String(testArgsFlag, "", "additional arguments to pass to go test binary")
	benchCmd.Flags().Bool(runSepProcessTenantFlag, false, "run separate process tenant benchmarks (these may freeze due to tenant limits)")

	return benchCmd
}

// benchRun groups benchmarks that share the same RunArgs so they can be
// executed in a single bazel invocation.
type benchRun struct {
	filter  string
	names   []string // benchmark names in this group, for display purposes
	runArgs benchdoc.RunArgs
}

func (d *dev) bench(cmd *cobra.Command, commandLine []string) error {
	pkgs, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	var (
		filter              = mustGetFlagString(cmd, filterFlag)
		ignoreCache         = mustGetFlagBool(cmd, ignoreCacheFlag)
		timeout             = mustGetFlagDuration(cmd, timeoutFlag)
		short               = mustGetFlagBool(cmd, shortFlag)
		showLogs            = mustGetFlagBool(cmd, showLogsFlag)
		verbose             = mustGetFlagBool(cmd, vFlag)
		count               = mustGetFlagInt(cmd, countFlag)
		benchTime           = mustGetFlagString(cmd, benchTimeFlag)
		benchMem            = mustGetFlagBool(cmd, benchMemFlag)
		streamOutput        = mustGetFlagBool(cmd, streamOutputFlag)
		testArgs            = mustGetFlagString(cmd, testArgsFlag)
		runSepProcessTenant = mustGetFlagBool(cmd, runSepProcessTenantFlag)
	)

	// Enumerate all benches to run.
	if len(pkgs) == 0 {
		// Empty `dev bench` does the same thing as `dev bench pkg/...`
		// TODO: pkg/... is a ton of stuff, most of which is not
		// benchmarks. Maybe we should be searching for all occurrences
		// of `func Bench` in the repo to enumerate the correct
		// packages.
		pkgs = append(pkgs, "pkg/...")
	}

	var testTargets []string
	for _, pkg := range pkgs {
		labels, err := d.getTestTargets(ctx, pkg)
		if err != nil {
			return err
		}
		if len(labels) == 0 {
			log.Printf("WARNING: no test targets were found matching %s", pkg)
			continue
		}
		testTargets = append(testTargets, labels...)
	}

	// When a filter is provided, look up benchdoc annotations in the source
	// files for the specified packages. Benchmarks are grouped by their
	// effective RunArgs so that benchmarks needing different parameters get
	// separate bazel invocations.
	var runs []benchRun
	if filter != "" {
		workspace, err := d.getWorkspace(ctx)
		if err != nil {
			return err
		}
		runs, err = d.groupBenchmarksByRunArgs(workspace, pkgs, filter)
		if err != nil {
			// Non-fatal: fall back to running with CLI args only.
			log.Printf("WARNING: could not read benchdoc annotations: %v", err)
			runs = nil
		}
	}

	// Apply CLI flag overrides to benchdoc RunArgs. CLI flags take
	// precedence when explicitly set by the user.
	countChanged := cmd.Flags().Changed(countFlag)
	timeoutChanged := cmd.Flags().Changed(timeoutFlag)
	for i := range runs {
		if countChanged {
			runs[i].runArgs.Count = count
		}
		if benchTime != "" {
			runs[i].runArgs.BenchTime = benchTime
		}
		if timeoutChanged {
			runs[i].runArgs.Timeout = timeout
		}
	}

	// If we have no benchdoc-driven groups (no filter, or parsing failed),
	// fall back to a single run with CLI flags.
	if len(runs) == 0 {
		ra := benchdoc.NewRunArgs()
		ra.Count = count
		ra.BenchTime = benchTime
		ra.Timeout = timeout
		runs = []benchRun{{filter: filter, runArgs: ra}}
	}

	// Deduplicate runs that ended up with identical RunArgs after overrides.
	runs = deduplicateRuns(runs)

	for i, run := range runs {
		if len(runs) > 1 {
			log.Printf("=== benchmark invocation %d/%d: %s ===",
				i+1, len(runs), strings.Join(run.names, ", "))
		}
		if err := d.runBench(ctx, runBenchOpts{
			run:                 run,
			testTargets:         testTargets,
			ignoreCache:         ignoreCache,
			short:               short,
			showLogs:            showLogs,
			verbose:             verbose,
			benchMem:            benchMem,
			streamOutput:        streamOutput,
			testArgs:            testArgs,
			runSepProcessTenant: runSepProcessTenant,
			additionalBazelArgs: additionalBazelArgs,
		}); err != nil {
			return err
		}
	}
	return nil
}

type runBenchOpts struct {
	run                 benchRun
	testTargets         []string
	ignoreCache         bool
	short               bool
	showLogs            bool
	verbose             bool
	benchMem            bool
	streamOutput        bool
	testArgs            string
	runSepProcessTenant bool
	additionalBazelArgs []string
}

// runBench executes a single bazel test invocation for a group of benchmarks
// that share the same RunArgs.
func (d *dev) runBench(ctx context.Context, opts runBenchOpts) error {
	run := opts.run
	var args []string
	args = append(args, "test")
	addCommonBazelArguments(&args)
	if run.runArgs.Timeout > 0 {
		args = append(args, fmt.Sprintf("--test_timeout=%d",
			int(run.runArgs.Timeout.Seconds())))
	}

	args = append(args, opts.testTargets...)
	if opts.ignoreCache {
		args = append(args, "--nocache_test_results")
	}

	args = append(args, "--test_arg", "-test.run=-")
	if run.filter == "" {
		args = append(args, "--test_arg", "-test.bench=.")
	} else {
		args = append(args, "--test_arg",
			fmt.Sprintf("-test.bench=%s", run.filter))
	}
	args = append(args, "--test_sharding_strategy=disabled")
	args = append(args, "--test_arg", "-test.cpu", "--test_arg", "1")
	if opts.short {
		args = append(args, "--test_arg", "-test.short")
	}
	if opts.verbose {
		args = append(args, "--test_arg", "-test.v")
	}
	if opts.showLogs {
		args = append(args, "--test_arg", "-show-logs")
	}
	if run.runArgs.Count > 1 {
		args = append(args, "--test_arg",
			fmt.Sprintf("-test.count=%d", run.runArgs.Count))
	}
	if run.runArgs.BenchTime != "" {
		args = append(args, "--test_arg",
			fmt.Sprintf("-test.benchtime=%s", run.runArgs.BenchTime))
	}
	if opts.benchMem {
		args = append(args, "--test_arg", "-test.benchmem")
	}
	if opts.runSepProcessTenant {
		args = append(args, "--test_arg", "-run-sep-process-tenant")
	}
	// The `crdb_test` flag enables metamorphic variables and various "extra"
	// debug checking code paths that can interfere with performance testing,
	// hence it should be disabled for benchmarks.
	args = append(args, "--crdb_test_off", "--crdb_bench")
	if opts.testArgs != "" {
		goTestArgs, err := d.getGoTestArgs(ctx, opts.testArgs)
		if err != nil {
			return err
		}
		args = append(args, goTestArgs...)
	}
	args = append(args, d.getGoTestEnvArgs()...)
	args = append(args, d.getTestOutputArgs(
		opts.verbose, opts.showLogs, opts.streamOutput)...)
	args = append(args, opts.additionalBazelArgs...)
	logCommand("bazel", args...)
	return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
}

// groupBenchmarksByRunArgs scans _test.go files in the given packages for
// benchdoc annotations, filters benchmarks by the provided regex, and groups
// them by their RunArgs. Benchmarks with no benchdoc annotation use defaults.
func (d *dev) groupBenchmarksByRunArgs(
	workspace string, pkgs []string, filter string,
) ([]benchRun, error) {
	filterRe, err := regexp.Compile(filter)
	if err != nil {
		return nil, fmt.Errorf("invalid filter regex: %w", err)
	}

	// Collect all benchmark infos from the specified packages.
	var allInfos []benchdoc.BenchmarkInfo
	for _, pkg := range pkgs {
		// Skip wildcard packages — we can't enumerate source files for
		// pkg/... without walking the entire tree.
		if strings.HasSuffix(pkg, "/...") {
			continue
		}
		pkg = normalizePackage(pkg)
		// Strip any Bazel target suffix (e.g., ":foo_test").
		if idx := strings.IndexByte(pkg, ':'); idx >= 0 {
			pkg = pkg[:idx]
		}

		pkgDir := filepath.Join(workspace, pkg)
		infos, parseErr := parseBenchmarkInfos(pkgDir, pkg)
		if parseErr != nil {
			return nil, parseErr
		}
		allInfos = append(allInfos, infos...)
	}

	if len(allInfos) == 0 {
		return nil, nil
	}

	// Filter benchmarks by the regex and group by RunArgs.
	type runArgsKey struct {
		Count     int
		BenchTime string
		Timeout   time.Duration
	}
	groups := make(map[runArgsKey][]string) // key → list of benchmark names
	argsMap := make(map[runArgsKey]benchdoc.RunArgs)

	for _, info := range allInfos {
		if !filterRe.MatchString(info.Name) {
			continue
		}
		key := runArgsKey{
			Count:     info.RunArgs.Count,
			BenchTime: info.RunArgs.BenchTime,
			Timeout:   info.RunArgs.Timeout,
		}
		groups[key] = append(groups[key], info.Name)
		argsMap[key] = info.RunArgs
	}

	if len(groups) == 0 {
		return nil, nil
	}

	// If all matched benchmarks have the same RunArgs, keep the original
	// filter to allow sub-benchmark matching (e.g., "BenchmarkFoo/case1").
	if len(groups) == 1 {
		for key, names := range groups {
			ra := argsMap[key]
			if !hasCustomRunArgs(ra) {
				// All defaults — nothing to override.
				return nil, nil
			}
			logBenchdocArgs(names, ra)
			return []benchRun{{filter: filter, names: names, runArgs: ra}}, nil
		}
	}

	// Multiple groups: build a precise filter for each group.
	var runs []benchRun
	for key, names := range groups {
		ra := argsMap[key]
		// Build an exact-match filter: ^(BenchmarkA|BenchmarkB)$
		benchFilter := "^(" + strings.Join(names, "|") + ")$"
		if hasCustomRunArgs(ra) {
			logBenchdocArgs(names, ra)
		}
		runs = append(runs, benchRun{
			filter: benchFilter, names: names, runArgs: ra,
		})
	}
	return runs, nil
}

// parseBenchmarkInfos parses all _test.go files in pkgDir and returns
// benchmark info for each benchmark function found.
func parseBenchmarkInfos(pkgDir string, pkg string) ([]benchdoc.BenchmarkInfo, error) {
	entries, err := os.ReadDir(pkgDir)
	if err != nil {
		return nil, fmt.Errorf("reading package directory %s: %w", pkgDir, err)
	}

	var infos []benchdoc.BenchmarkInfo
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(pkgDir, entry.Name())
		fset := token.NewFileSet()
		f, parseErr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if parseErr != nil {
			// Skip files that fail to parse.
			continue
		}
		packageResolver := func() (string, error) { return pkg, nil }
		teamResolver := func() (string, error) { return "", nil }
		bi, analyzeErr := benchdoc.AnalyzeBenchmarkDocs(
			f, packageResolver, teamResolver,
			true /* lenient */, nil /* reportFailure */)
		if analyzeErr != nil {
			return nil, analyzeErr
		}
		infos = append(infos, bi...)
	}
	return infos, nil
}

// deduplicateRuns merges runs that have identical RunArgs into a single run
// with a combined filter.
func deduplicateRuns(runs []benchRun) []benchRun {
	if len(runs) <= 1 {
		return runs
	}
	type key struct {
		Count     int
		BenchTime string
		Timeout   time.Duration
	}
	seen := make(map[key]int) // key → index in result
	var result []benchRun
	for _, r := range runs {
		k := key{
			Count:     r.runArgs.Count,
			BenchTime: r.runArgs.BenchTime,
			Timeout:   r.runArgs.Timeout,
		}
		if idx, ok := seen[k]; ok {
			// Merge filters and names.
			existing := result[idx].filter
			if existing != "" && r.filter != "" {
				result[idx].filter = existing + "|" + r.filter
			}
			result[idx].names = append(result[idx].names, r.names...)
		} else {
			seen[k] = len(result)
			result = append(result, r)
		}
	}
	return result
}

func hasCustomRunArgs(ra benchdoc.RunArgs) bool {
	return ra.Count > 0 || ra.BenchTime != "" || ra.Timeout > 0
}

func logBenchdocArgs(names []string, ra benchdoc.RunArgs) {
	log.Printf("benchdoc: %s: %s",
		strings.Join(names, ", "), formatRunArgs(ra))
}

func formatRunArgs(ra benchdoc.RunArgs) string {
	var parts []string
	if ra.Count > 0 {
		parts = append(parts, fmt.Sprintf("count=%d", ra.Count))
	}
	if ra.BenchTime != "" {
		parts = append(parts, fmt.Sprintf("benchtime=%s", ra.BenchTime))
	}
	if ra.Timeout > 0 {
		parts = append(parts, fmt.Sprintf("timeout=%s", ra.Timeout))
	}
	return strings.Join(parts, ", ")
}

func (d *dev) getGoTestEnvArgs() []string {
	var goTestEnv []string
	// Make the `$HOME/.cache/crdb-test-fixtures` directory available for reusable
	// test fixtures, if available. See testfixtures.ReuseOrGenerate().
	if cacheDir, err := d.os.UserCacheDir(); err == nil {
		dir := filepath.Join(cacheDir, "crdb-test-fixtures")
		if err := os.MkdirAll(dir, 0755); err == nil {
			goTestEnv = append(goTestEnv, "--test_env", fmt.Sprintf("COCKROACH_TEST_FIXTURES_DIR=%s", dir))
			goTestEnv = append(goTestEnv, fmt.Sprintf("--sandbox_writable_path=%s", dir))
		}
	}
	return goTestEnv
}
