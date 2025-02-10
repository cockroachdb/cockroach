// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/parser"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	roachprodConfig "github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
)

type executorConfig struct {
	cluster           string
	binaries          map[string]string
	excludeList       []string
	ignorePackageList []string
	outputDir         string
	timeout           string
	shellCommand      string
	testArgs          []string
	iterations        int
	lenient           bool
	affinity          bool
	quiet             bool
	recoverable       bool
}

type executor struct {
	executorConfig
	excludeBenchmarksRegex [][]*regexp.Regexp
	ignorePackages         map[string]struct{}
	runOptions             install.RunOptions
	log                    *logger.Logger
}

type benchmark struct {
	pkg  string
	name string
}

type benchmarkKey struct {
	benchmark
	key string
}

func newExecutor(config executorConfig) (*executor, error) {
	// Exclude packages that should not to be probed. This is useful for excluding
	// packages that have known issues and unable to list its benchmarks, or are
	// not relevant to the current benchmarking effort.
	ignorePackages := make(map[string]struct{})
	for _, pkg := range config.ignorePackageList {
		ignorePackages[pkg] = struct{}{}
	}

	config.outputDir = strings.TrimRight(config.outputDir, "/")
	err := os.MkdirAll(config.outputDir, os.ModePerm)
	if err != nil {
		return nil, err
	}
	err = util.VerifyPathFlag("output-dir", config.outputDir, true)
	if err != nil {
		return nil, err
	}

	runOptions := install.DefaultRunOptions().WithRetryDisabled()
	if config.recoverable {
		// For VMs that have started failing with transient errors we'll want to
		// introduce a longer retry, since the `recoverable` flag indicates these
		// may still be recoverable (usually due to preemption, but running on a
		// managed instance group that will try and recover the VM). But since it's
		// possible a node may never come back up (due to resources exhaustion),
		// we'll want to limit the number of retries.
		runOptions = runOptions.WithRetryOpts(retry.Options{
			InitialBackoff: 1 * time.Minute,
			MaxBackoff:     5 * time.Minute,
			Multiplier:     2,
			MaxRetries:     10,
		})
	}

	if config.iterations < 1 {
		return nil, errors.New("iterations must be greater than 0")
	}

	roachprodConfig.Quiet = config.quiet
	timestamp := timeutil.Now()
	l := InitLogger(filepath.Join(config.outputDir, fmt.Sprintf("roachprod-microbench-%s.log", timestamp.Format(util.TimeFormat))))

	excludeBenchmarks := util.GetRegexExclusionPairs(config.excludeList)
	return &executor{
		executorConfig:         config,
		excludeBenchmarksRegex: excludeBenchmarks,
		ignorePackages:         ignorePackages,
		runOptions:             runOptions,
		log:                    l,
	}, nil
}

func defaultExecutorConfig() executorConfig {
	return executorConfig{
		binaries:     map[string]string{"experiment": "experiment"},
		outputDir:    "artifacts/roachprod-microbench",
		timeout:      "10m",
		shellCommand: "COCKROACH_RANDOM_SEED=1",
		iterations:   1,
		lenient:      true,
		recoverable:  true,
	}
}

// listBenchmarks distributes a listing command to test package binaries across
// the cluster. It will filter out any packages that do not contain benchmarks
// or that matches the exclusion regex. A list of valid packages and benchmarks
// are returned.
func (e *executor) listBenchmarks(
	roachprodLog *logger.Logger, packages []string, numNodes int,
) ([]string, []benchmark, error) {
	// Generate commands for listing benchmarks.
	commands := make([][]cluster.RemoteCommand, 0)
	for _, pkg := range packages {
		for _, bin := range e.binaries {
			remoteBinDir := fmt.Sprintf("%s/%s/bin", bin, pkg)
			// The command will not fail if the directory does not exist.
			command := cluster.RemoteCommand{
				Args: []string{"sh", "-c",
					fmt.Sprintf(`"[ ! -d %s ] || (cd %s && ./run.sh -test.list=^Benchmark*)"`,
						remoteBinDir, remoteBinDir)},
				Metadata: pkg,
			}
			commands = append(commands, []cluster.RemoteCommand{command})
		}
	}

	// Execute commands for listing benchmarks.
	isValidBenchmarkName := regexp.MustCompile(`^Benchmark[a-zA-Z0-9_]+$`).MatchString
	errorCount := 0
	benchmarkCounts := make(map[benchmark]int)
	callback := func(response cluster.RemoteResponse) {
		if !e.quiet {
			fmt.Print(".")
		}
		if response.Err == nil {
			pkg := response.Metadata.(string)
		outer:
			for _, benchmarkName := range strings.Split(response.Stdout, "\n") {
				benchmarkName = strings.TrimSpace(benchmarkName)
				if benchmarkName == "" {
					continue
				}
				if !isValidBenchmarkName(benchmarkName) {
					if !e.quiet {
						fmt.Println()
					}
					e.log.Printf("Ignoring invalid benchmark name: %s", benchmarkName)
					continue
				}
				for _, exclusionPair := range e.excludeBenchmarksRegex {
					if exclusionPair[0].MatchString(pkg) && exclusionPair[1].MatchString(benchmarkName) {
						continue outer
					}
				}

				benchmarkEntry := benchmark{pkg, benchmarkName}
				benchmarkCounts[benchmarkEntry]++
			}
		} else {
			if !e.quiet {
				fmt.Println()
			}
			e.log.Errorf("Remote command = {%s}, error = {%v}, stderr output = {%s}",
				strings.Join(response.Args, " "), response.Err, response.Stderr)
			errorCount++
		}
	}
	e.log.Printf("Distributing and running benchmark listings across cluster %s", e.cluster)
	_ = cluster.ExecuteRemoteCommands(
		roachprodLog, roachprod.RunWithDetails, e.cluster, commands, numNodes, true, e.runOptions, callback,
	)
	if !e.quiet {
		fmt.Println()
	}

	if errorCount > 0 {
		return nil, nil, errors.New("Failed to list benchmarks")
	}

	benchmarks := make([]benchmark, 0, len(benchmarkCounts))
	packageCounts := make(map[string]int)
	for k, v := range benchmarkCounts {
		// Some packages override TestMain to run the tests twice or more thus
		// appearing more than once in the listing logic.
		if v >= len(e.binaries) {
			packageCounts[k.pkg]++
			benchmarks = append(benchmarks, k)
		} else {
			e.log.Printf("Ignoring benchmark %s/%s, missing from %d binaries", k.pkg, k.name, len(e.binaries)-v)
		}
	}

	validPackages := make([]string, 0, len(packageCounts))
	for pkg, count := range packageCounts {
		e.log.Printf("Found %d benchmarks in %s", count, pkg)
		validPackages = append(validPackages, pkg)
	}
	return validPackages, benchmarks, nil
}

// listRemotePackages returns a list of packages containing benchmarks on the remote
// cluster by inspecting the given remote directory.
func (e *executor) listRemotePackages(log *logger.Logger, dir string) ([]string, error) {
	ctx := context.Background()
	results, err := roachprod.RunWithDetails(ctx, log, e.cluster, "", "", false,
		[]string{"find", dir, "-name", "run.sh"}, install.WithNodes(install.Nodes{1}))
	if err != nil {
		return nil, err
	}
	result := results[0]
	if result.Err != nil {
		return nil, result.Err
	}
	packages := make([]string, 0)
	binRunSuffix := "/bin/run.sh"
	for _, line := range strings.Split(result.Stdout, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || !strings.HasSuffix(line, binRunSuffix) {
			continue
		}
		line = strings.TrimPrefix(line, dir)
		line = strings.TrimPrefix(line, "/")
		line = strings.TrimSuffix(line, binRunSuffix)
		packages = append(packages, line)
	}
	return packages, nil
}

// executeBenchmarks executes the microbenchmarks on the remote cluster. Reports
// containing the microbenchmark results for each package are stored in the log
// output directory. Microbenchmark failures are recorded in separate log files,
// that are stored alongside the reports, and are named the same as the
// corresponding microbenchmark. When running in lenient mode errors will not
// fail the execution, and will still be logged to the aforementioned logs.
func (e *executor) executeBenchmarks() error {

	// Remote execution Logging is captured and saved to appropriate log files and
	// the main logger is used for orchestration logging only. Therefore, we use a
	// muted logger for remote execution.
	muteLogger, err := (&logger.Config{Stdout: io.Discard, Stderr: io.Discard}).NewLogger("")
	if err != nil {
		return err
	}

	// Init `roachprod` and get the number of nodes in the cluster.
	InitRoachprod()
	statuses, err := roachprod.Status(context.Background(), e.log, e.cluster, "")
	if err != nil {
		return err
	}
	numNodes := len(statuses)
	e.log.Printf("number of nodes to execute benchmarks on: %d", numNodes)

	// List packages containing benchmarks on the remote cluster for the first
	// remote binaries' directory.
	remotePackageDir := maps.Values(e.binaries)[0]
	remotePackages, err := e.listRemotePackages(muteLogger, remotePackageDir)
	if err != nil {
		return err
	}
	if len(remotePackages) == 0 {
		return errors.Newf("No benchmarks found in the remote working directory %s", remotePackageDir)
	}

	packages := make([]string, 0, len(remotePackages))
	for _, pkg := range remotePackages {
		if _, ok := e.ignorePackages[pkg]; !ok {
			packages = append(packages, pkg)
		}
	}

	validPackages, benchmarks, err := e.listBenchmarks(muteLogger, packages, numNodes)
	if err != nil {
		return err
	}
	if len(validPackages) == 0 {
		return errors.New("no packages containing benchmarks found")
	}

	// Create reports for each key, binary combination.
	reporters := make(map[string]*report)
	defer func() {
		for _, report := range reporters {
			report.closeReports()
		}
	}()
	for key := range e.binaries {
		report := &report{}
		// Use the binary key as the report output directory name.
		dir := path.Join(e.outputDir, key)
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return err
		}
		err = report.createReports(e.log, dir, validPackages)
		reporters[key] = report
		if err != nil {
			return err
		}
	}

	// Generate commands for running benchmarks.
	commands := make([][]cluster.RemoteCommand, 0)
	for _, bench := range benchmarks {
		runCommand := fmt.Sprintf("./run.sh %s -test.benchmem -test.bench=^%s$ -test.run=^$ -test.v",
			strings.Join(e.testArgs, " "), bench.name)
		if e.timeout != "" {
			runCommand = fmt.Sprintf("timeout -k 30s %s %s", e.timeout, runCommand)
		}
		if e.shellCommand != "" {
			runCommand = fmt.Sprintf("%s && %s", e.shellCommand, runCommand)
		}
		commandGroup := make([]cluster.RemoteCommand, 0)
		// Weave the commands between binaries and iterations.
		for i := 0; i < e.iterations; i++ {
			for key, bin := range e.binaries {
				shellCommand := fmt.Sprintf(`"cd %s/%s/bin && %s"`, bin, bench.pkg, runCommand)
				command := cluster.RemoteCommand{
					Args:     []string{"sh", "-c", shellCommand},
					Metadata: benchmarkKey{bench, key},
				}
				commandGroup = append(commandGroup, command)
			}
		}
		if e.affinity {
			commands = append(commands, commandGroup)
		} else {
			// When affinity is disabled, commands & single iterations can run on any
			// node. This has the benefit of not having stragglers, but the downside
			// of possibly introducing noise.
			for _, command := range commandGroup {
				commands = append(commands, []cluster.RemoteCommand{command})
			}
		}
	}

	// Execute commands.
	errorCount := 0
	logIndex := 0
	missingBenchmarks := make(map[benchmark]int, 0)
	failedBenchmarks := make(map[benchmark]int, 0)
	skippedBenchmarks := make(map[benchmark]int, 0)
	callback := func(response cluster.RemoteResponse) {
		if !e.quiet {
			fmt.Print(".")
		}
		extractResults := parser.ExtractBenchmarkResults(response.Stdout)
		benchmarkResponse := response.Metadata.(benchmarkKey)
		report := reporters[benchmarkResponse.key]
		for _, benchmarkResult := range extractResults.Results {
			if _, writeErr := report.benchmarkOutput[benchmarkResponse.pkg].WriteString(
				fmt.Sprintf("%s\n", strings.Join(benchmarkResult, " "))); writeErr != nil {
				e.log.Errorf("Failed to write benchmark result to file - %v", writeErr)
			}
		}
		if extractResults.Errors || response.Err != nil {
			if !e.quiet {
				fmt.Println()
			}
			tag := fmt.Sprintf("%d", logIndex)
			if response.ExitStatus == 124 || response.ExitStatus == 137 {
				tag = fmt.Sprintf("%d-timeout", logIndex)
			}
			err = report.writeBenchmarkErrorLogs(response, tag)
			if err != nil {
				e.log.Errorf("Failed to write error logs - %v", err)
			}
			errorCount++
			logIndex++
		}
		if _, writeErr := report.analyticsOutput[benchmarkResponse.pkg].WriteString(
			fmt.Sprintf("%s %d ms\n", benchmarkResponse.name,
				response.Duration.Milliseconds())); writeErr != nil {
			e.log.Errorf("Failed to write analytics to file - %v", writeErr)
		}

		// If we didn't find any results, increment the appropriate counter.
		if len(extractResults.Results) == 0 {
			switch {
			case extractResults.Errors || response.Err != nil || response.ExitStatus != 0:
				failedBenchmarks[benchmarkResponse.benchmark]++
			case extractResults.Skipped:
				skippedBenchmarks[benchmarkResponse.benchmark]++
			default:
				missingBenchmarks[benchmarkResponse.benchmark]++
			}
		}
	}
	e.log.Printf("Found %d benchmarks, distributing and running benchmarks for %d iteration(s) across cluster %s",
		len(benchmarks), e.iterations, e.cluster)
	_ = cluster.ExecuteRemoteCommands(
		muteLogger, roachprod.RunWithDetails, e.cluster, commands, numNodes, !e.lenient, e.runOptions, callback,
	)

	if !e.quiet {
		fmt.Println()
	}
	for res, count := range failedBenchmarks {
		e.log.Errorf("Failed benchmark: %s/%s in %d iterations", res.pkg, res.name, count)
	}
	for res, count := range skippedBenchmarks {
		e.log.Errorf("Skipped benchmark: %s/%s in %d iterations", res.pkg, res.name, count)
	}
	for res, count := range missingBenchmarks {
		e.log.Errorf("Missing benchmark: %s/%s in %d iterations", res.pkg, res.name, count)
	}

	e.log.Printf("Completed benchmarks, results located at %s", e.outputDir)
	return nil
}
