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
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/cluster"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	roachprodConfig "github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type executorConfig struct {
	cluster           string
	binaries          string
	compareBinaries   string
	excludeList       []string
	ignorePackageList []string
	outputDir         string
	libDir            string
	remoteDir         string
	timeout           string
	shellCommand      string
	testArgs          []string
	iterations        int
	copyBinaries      bool
	lenient           bool
	affinity          bool
	quiet             bool
}

type executor struct {
	executorConfig
	binariesList           []string
	excludeBenchmarksRegex [][]*regexp.Regexp
	packages               []string
	log                    *logger.Logger
}

type benchmark struct {
	pkg  string
	name string
}

type benchmarkIndexed struct {
	benchmark
	index int
}

type benchmarkExtractionResult struct {
	results [][]string
	errors  bool
	skipped bool
}

func newExecutor(config executorConfig) (*executor, error) {
	// Gather package info from the primary binary.
	archivePackages, err := readArchivePackages(config.binaries)
	if err != nil {
		return nil, err
	}

	// Exclude packages that should not to be probed. This is useful for excluding
	// packages that have known issues and unable to list its benchmarks, or are
	// not relevant to the current benchmarking effort.
	ignorePackages := make(map[string]struct{})
	for _, pkg := range config.ignorePackageList {
		ignorePackages[pkg] = struct{}{}
	}
	packages := make([]string, 0, len(archivePackages))
	for _, pkg := range archivePackages {
		if _, ok := ignorePackages[pkg]; !ok {
			packages = append(packages, pkg)
		}
	}

	config.outputDir = strings.TrimRight(config.outputDir, "/")
	err = os.MkdirAll(config.outputDir, os.ModePerm)
	if err != nil {
		return nil, err
	}
	err = verifyPathFlag("outputdir", config.outputDir, true)
	if err != nil {
		return nil, err
	}

	binariesList := []string{config.binaries}
	if config.compareBinaries != "" {
		binariesList = append(binariesList, config.compareBinaries)
	}

	for _, binariesName := range binariesList {
		if err = verifyPathFlag("binaries", binariesName, false); err != nil {
			return nil, err
		}
		if !strings.HasSuffix(binariesName, ".tar") && !strings.HasSuffix(binariesName, ".tar.gz") {
			return nil, fmt.Errorf("the binaries archive %s must have the extension .tar or .tar.gz", binariesName)
		}
	}

	if config.iterations < 1 {
		return nil, errors.New("iterations must be greater than 0")
	}

	roachprodConfig.Quiet = config.quiet
	timestamp := timeutil.Now()
	l := initLogger(filepath.Join(config.outputDir, fmt.Sprintf("roachprod-microbench-%s.log", timestamp.Format(timeFormat))))

	excludeBenchmarks := getRegexExclusionPairs(config.excludeList)
	return &executor{
		executorConfig:         config,
		excludeBenchmarksRegex: excludeBenchmarks,
		binariesList:           binariesList,
		packages:               packages,
		log:                    l,
	}, nil
}

func defaultExecutorConfig() executorConfig {
	return executorConfig{
		binaries:     "bin/test_binaries.tar.gz",
		outputDir:    "artifacts/roachprod-microbench",
		libDir:       "bin/lib",
		remoteDir:    "/mnt/data1/microbench",
		timeout:      "10m",
		shellCommand: "COCKROACH_RANDOM_SEED=1",
		iterations:   1,
		copyBinaries: true,
		lenient:      true,
		affinity:     false,
		quiet:        false,
	}
}

// prepareCluster prepares the cluster for executing microbenchmarks. It copies
// the binaries to the remote cluster and extracts it. It also copies the lib
// directory to the remote cluster. The number of nodes in the cluster is
// returned, or -1 if the node count could not be determined.
func (e *executor) prepareCluster() (int, error) {
	e.log.Printf("Setting up roachprod")
	if err := initRoachprod(e.log); err != nil {
		return -1, err
	}

	statuses, err := roachprod.Status(context.Background(), e.log, e.cluster, "")
	if err != nil {
		return -1, err
	}
	numNodes := len(statuses)
	e.log.Printf("Number of nodes: %d", numNodes)

	// Clear old artifacts, copy and extract new artifacts on to the cluster.
	if e.copyBinaries {
		if fi, cmdErr := os.Stat(e.libDir); cmdErr == nil && fi.IsDir() {
			if putErr := roachprod.Put(context.Background(), e.log, e.cluster, e.libDir, "lib", true); putErr != nil {
				return numNodes, putErr
			}
		}

		for binIndex, bin := range e.binariesList {
			// Specify the binaries' tarball remote location.
			remoteBinSrc := fmt.Sprintf("%d_%s", binIndex, filepath.Base(bin))
			remoteBinDest := filepath.Join(e.remoteDir, strconv.Itoa(binIndex))
			if rpErr := roachprod.Put(context.Background(), e.log, e.cluster, bin, remoteBinSrc, true); rpErr != nil {
				return numNodes, rpErr
			}
			if rpErr := roachprodRun(e.cluster, e.log, []string{"rm", "-rf", remoteBinDest}); rpErr != nil {
				return numNodes, rpErr
			}
			if rpErr := roachprodRun(e.cluster, e.log, []string{"mkdir", "-p", remoteBinDest}); rpErr != nil {
				return numNodes, rpErr
			}
			extractFlags := "-xf"
			if filepath.Ext(bin) == ".gz" {
				extractFlags = "-xzf"
			}
			if rpErr := roachprodRun(e.cluster, e.log, []string{"tar", extractFlags, remoteBinSrc, "-C", remoteBinDest}); rpErr != nil {
				return numNodes, rpErr
			}
			if rpErr := roachprodRun(e.cluster, e.log, []string{"rm", "-rf", remoteBinSrc}); rpErr != nil {
				return numNodes, rpErr
			}
		}
	}
	return numNodes, nil
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
		for binIndex := range e.binariesList {
			remoteBinDir := fmt.Sprintf("%s/%d/%s/bin", e.remoteDir, binIndex, pkg)
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
		roachprodLog, roachprod.RunWithDetails, e.cluster, commands, numNodes, true, callback,
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
		if v >= len(e.binariesList) {
			packageCounts[k.pkg]++
			benchmarks = append(benchmarks, k)
		} else {
			e.log.Printf("Ignoring benchmark %s/%s, missing from %d binaries", k.pkg, k.name, len(e.binariesList)-v)
		}
	}

	validPackages := make([]string, 0, len(packageCounts))
	for pkg, count := range packageCounts {
		e.log.Printf("Found %d benchmarks in %s", count, pkg)
		validPackages = append(validPackages, pkg)
	}
	return validPackages, benchmarks, nil
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

	numNodes, err := e.prepareCluster()
	if err != nil {
		return err
	}

	validPackages, benchmarks, err := e.listBenchmarks(muteLogger, e.packages, numNodes)
	if err != nil {
		return err
	}
	if len(validPackages) == 0 {
		return errors.New("no packages containing benchmarks found")
	}

	// Create reports for each binary.
	// Currently, we only support comparing two binaries at most.
	reporters := make([]*report, 0)
	defer func() {
		for _, report := range reporters {
			report.closeReports()
		}
	}()
	for index := range e.binariesList {
		report := &report{}
		dir := path.Join(e.outputDir, strconv.Itoa(index))
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return err
		}
		err = report.createReports(e.log, dir, validPackages)
		reporters = append(reporters, report)
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
			for binIndex := range e.binariesList {
				shellCommand := fmt.Sprintf(`"cd %s/%d/%s/bin && %s"`, e.remoteDir, binIndex, bench.pkg, runCommand)
				command := cluster.RemoteCommand{
					Args:     []string{"sh", "-c", shellCommand},
					Metadata: benchmarkIndexed{bench, binIndex},
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
		extractResults := extractBenchmarkResults(response.Stdout)
		benchmarkResponse := response.Metadata.(benchmarkIndexed)
		report := reporters[benchmarkResponse.index]
		for _, benchmarkResult := range extractResults.results {
			if _, writeErr := report.benchmarkOutput[benchmarkResponse.pkg].WriteString(
				fmt.Sprintf("%s\n", strings.Join(benchmarkResult, " "))); writeErr != nil {
				e.log.Errorf("Failed to write benchmark result to file - %v", writeErr)
			}
		}
		if extractResults.errors || response.Err != nil {
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
		if len(extractResults.results) == 0 {
			switch {
			case extractResults.errors || response.Err != nil || response.ExitStatus != 0:
				failedBenchmarks[benchmarkResponse.benchmark]++
			case extractResults.skipped:
				skippedBenchmarks[benchmarkResponse.benchmark]++
			default:
				missingBenchmarks[benchmarkResponse.benchmark]++
			}
		}
	}
	e.log.Printf("Found %d benchmarks, distributing and running benchmarks for %d iteration(s) across cluster %s",
		len(benchmarks), e.iterations, e.cluster)
	_ = cluster.ExecuteRemoteCommands(
		muteLogger, roachprod.RunWithDetails, e.cluster, commands, numNodes, !e.lenient, callback,
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
	if errorCount != 0 {
		return errors.Newf("Found %d errors during remote execution", errorCount)
	}
	return nil
}

// extractBenchmarkResults extracts the microbenchmark results generated by a
// test binary and reports if any failures or skips were found in the output.
// This method makes specific assumptions regarding the format of the output,
// and attempts to ignore any spurious output that the test binary may have
// logged. The returned list of string arrays each represent a row of metrics as
// outputted by the test binary.
func extractBenchmarkResults(benchmarkOutput string) benchmarkExtractionResult {
	keywords := map[string]struct{}{
		"ns/op":     {},
		"B/op":      {},
		"allocs/op": {},
	}
	results := make([][]string, 0)
	buf := make([]string, 0)
	containsErrors := false
	skipped := false
	var benchName string
	for _, line := range strings.Split(benchmarkOutput, "\n") {
		elems := strings.Fields(line)
		for _, s := range elems {
			if !containsErrors {
				containsErrors = strings.Contains(s, "FAIL") || strings.Contains(s, "panic:")
			}
			if !skipped {
				skipped = strings.Contains(s, "SKIP")
			}
			if strings.HasPrefix(s, "Benchmark") && len(s) > 9 {
				benchName = s
			}
			if _, ok := keywords[s]; ok {
				row := elems
				if elems[0] == benchName {
					row = elems[1:]
				}

				buf = append(buf, row...)
				if benchName != "" {
					buf = append([]string{benchName}, buf...)
					results = append(results, buf)
				}
				buf = make([]string, 0)
				benchName = ""
			}
		}
	}
	return benchmarkExtractionResult{results, containsErrors, skipped}
}
