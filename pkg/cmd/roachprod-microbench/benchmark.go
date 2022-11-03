// Copyright 2022 The Cockroach Authors.
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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/cluster"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

type benchmark struct {
	pkg  string
	name string
}

type benchmarkIndexed struct {
	benchmark
	index int
}

// prepareCluster prepares the cluster for executing microbenchmarks. It copies
// the binaries to the remote cluster and extracts it. It also copies the lib
// directory to the remote cluster. The number of nodes in the cluster is
// returned, or -1 if the node count could not be determined.
func prepareCluster(binaries []string, remoteDir string) (int, error) {
	l.Printf("Setting up roachprod")
	if err := initRoachprod(); err != nil {
		return -1, err
	}

	statuses, err := roachprod.Status(context.Background(), l, *flagCluster, "")
	if err != nil {
		return -1, err
	}
	numNodes := len(statuses)
	l.Printf("Number of nodes: %d", numNodes)

	// Clear old artifacts, copy and extract new artifacts on to the cluster.
	if *flagCopy {
		if fi, cmdErr := os.Stat(*flagLibDir); cmdErr == nil && fi.IsDir() {
			if putErr := roachprod.Put(context.Background(), l, *flagCluster, *flagLibDir, "lib", true); putErr != nil {
				return numNodes, putErr
			}
		}

		for binIndex, bin := range binaries {
			// Specify the binaries' tarball remote location.
			remoteBinSrc := fmt.Sprintf("%d_%s", binIndex, filepath.Base(bin))
			remoteBinDest := filepath.Join(remoteDir, strconv.Itoa(binIndex))
			if rpErr := roachprod.Put(context.Background(), l, *flagCluster, bin, remoteBinSrc, true); rpErr != nil {
				return numNodes, rpErr
			}
			if rpErr := roachprodRun(*flagCluster, []string{"rm", "-rf", remoteBinDest}); rpErr != nil {
				return numNodes, rpErr
			}
			if rpErr := roachprodRun(*flagCluster, []string{"mkdir", "-p", remoteBinDest}); rpErr != nil {
				return numNodes, rpErr
			}
			extractFlags := "-xf"
			if filepath.Ext(bin) == ".gz" {
				extractFlags = "-xzf"
			}
			if rpErr := roachprodRun(*flagCluster, []string{"tar", extractFlags, remoteBinSrc, "-C", remoteBinDest}); rpErr != nil {
				return numNodes, rpErr
			}
			if rpErr := roachprodRun(*flagCluster, []string{"rm", "-rf", remoteBinSrc}); rpErr != nil {
				return numNodes, rpErr
			}
		}
	}
	return numNodes, nil
}

func initRoachprod() error {
	_ = roachprod.InitProviders()
	_, err := roachprod.Sync(l, vm.ListOptions{})
	return err
}

func roachprodRun(clusterName string, cmdArray []string) error {
	return roachprod.Run(context.Background(), l, clusterName, "", "", false, os.Stdout, os.Stderr, cmdArray)
}

// listBenchmarks distributes a listing command to test package binaries across
// the cluster. It will filter out any packages that do not contain benchmarks
// or that matches the exclusion regex. A list of valid packages and benchmarks
// are returned.
func listBenchmarks(
	log *logger.Logger, remoteDir string, binaries, packages []string, numNodes int,
) ([]string, []benchmark, error) {
	exclusionPairs := getRegexExclusionPairs()

	// Generate commands for listing benchmarks.
	commands := make([][]cluster.RemoteCommand, 0)
	for _, pkg := range packages {
		for binIndex := range binaries {
			remoteBinDir := fmt.Sprintf("%s/%d/%s/bin", remoteDir, binIndex, pkg)
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
	l.Printf("Distributing and running benchmark listings across cluster %s", *flagCluster)
	isValidBenchmarkName := regexp.MustCompile(`^Benchmark[a-zA-Z0-9_]+$`).MatchString
	errorCount := 0
	benchmarkCounts := make(map[benchmark]int)
	cluster.ExecuteRemoteCommands(log, *flagCluster, commands, numNodes, true, func(response cluster.RemoteResponse) {
		fmt.Print(".")
		if response.Err == nil {
			pkg := response.Metadata.(string)
		outer:
			for _, benchmarkName := range strings.Split(response.Stdout, "\n") {
				benchmarkName = strings.TrimSpace(benchmarkName)
				if benchmarkName == "" {
					continue
				}
				if !isValidBenchmarkName(benchmarkName) {
					fmt.Println()
					l.Printf("Ignoring invalid benchmark name: %s", benchmarkName)
					continue
				}
				for _, exclusionPair := range exclusionPairs {
					if exclusionPair[0].MatchString(pkg) && exclusionPair[1].MatchString(benchmarkName) {
						continue outer
					}
				}

				benchmarkEntry := benchmark{pkg, benchmarkName}
				benchmarkCounts[benchmarkEntry]++
			}
		} else {
			fmt.Println()
			l.Errorf("Remote command = {%s}, error = {%v}, stderr output = {%s}",
				strings.Join(response.Args, " "), response.Err, response.Stderr)
			errorCount++
		}
	})
	fmt.Println()

	if errorCount > 0 {
		return nil, nil, errors.New("Failed to list benchmarks")
	}

	benchmarks := make([]benchmark, 0, len(benchmarkCounts))
	packageCounts := make(map[string]int)
	for k, v := range benchmarkCounts {
		// Some packages override TestMain to run the tests twice or more thus
		// appearing more than once in the listing logic.
		if v >= len(binaries) {
			packageCounts[k.pkg]++
			benchmarks = append(benchmarks, k)
		} else {
			l.Printf("Ignoring benchmark %s/%s, missing from %d binaries", k.pkg, k.name, len(binaries)-v)
		}
	}

	validPackages := make([]string, 0, len(packageCounts))
	for pkg, count := range packageCounts {
		l.Printf("Found %d benchmarks in %s", count, pkg)
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
func executeBenchmarks(binaries, packages []string) error {
	remoteDir := fmt.Sprintf("%s/microbench", *flagRemoteDir)

	// Remote execution Logging is captured and saved to appropriate log files and
	// the main logger is used for orchestration logging only. Therefore, we use a
	// muted logger for remote execution.
	muteLogger, err := (&logger.Config{Stdout: io.Discard, Stderr: io.Discard}).NewLogger("")
	if err != nil {
		return err
	}

	numNodes, err := prepareCluster(binaries, remoteDir)
	if err != nil {
		return err
	}

	validPackages, benchmarks, err := listBenchmarks(muteLogger, remoteDir, binaries, packages, numNodes)
	if err != nil {
		return err
	}
	if len(validPackages) == 0 {
		return errors.New("no packages containing benchmarks found")
	}

	// Create reports for each binary.
	// Currently, we only support comparing two binaries.
	reporters := make([]*Report, 0)
	defer func() {
		for _, report := range reporters {
			report.closeReports()
		}
	}()
	for binIndex := range binaries {
		report := &Report{}
		dir := workingDir
		if binIndex > 0 {
			dir = *flagCompareDir
		}
		err = report.createReports(dir, validPackages)
		reporters = append(reporters, report)
		if err != nil {
			return err
		}
	}

	// Generate commands for running benchmarks.
	commands := make([][]cluster.RemoteCommand, 0)
	for _, bench := range benchmarks {
		runCommand := fmt.Sprintf("./run.sh %s -test.benchmem -test.bench=^%s$ -test.run=^$",
			strings.Join(testArgs, " "), bench.name)
		if *flagTimeout != "" {
			runCommand = fmt.Sprintf("timeout %s %s", *flagTimeout, runCommand)
		}
		if *flagShell != "" {
			runCommand = fmt.Sprintf("%s && %s", *flagShell, runCommand)
		}
		commandGroup := make([]cluster.RemoteCommand, 0)
		// Weave the commands between binaries and iterations.
		for i := 0; i < *flagIterations; i++ {
			for binIndex := range binaries {
				shellCommand := fmt.Sprintf(`"cd %s/%d/%s/bin && %s"`, remoteDir, binIndex, bench.pkg, runCommand)
				command := cluster.RemoteCommand{
					Args:     []string{"sh", "-c", shellCommand},
					Metadata: benchmarkIndexed{bench, binIndex},
				}
				commandGroup = append(commandGroup, command)
			}
		}
		if *flagAffinity {
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
	l.Printf("Found %d benchmarks, distributing and running benchmarks for %d iteration(s) across cluster %s",
		len(benchmarks), *flagIterations, *flagCluster)
	cluster.ExecuteRemoteCommands(muteLogger, *flagCluster, commands, numNodes, !*flagLenient, func(response cluster.RemoteResponse) {
		fmt.Print(".")
		benchmarkResults, containsErrors := extractBenchmarkResults(response.Stdout)
		benchmarkResponse := response.Metadata.(benchmarkIndexed)
		report := reporters[benchmarkResponse.index]
		for _, benchmarkResult := range benchmarkResults {
			if _, writeErr := report.benchmarkOutput[benchmarkResponse.pkg].WriteString(
				fmt.Sprintf("%s\n", strings.Join(benchmarkResult, " "))); writeErr != nil {
				l.Errorf("Failed to write benchmark result to file - %v", writeErr)
			}
		}
		if containsErrors || response.Err != nil {
			fmt.Println()
			err = report.writeBenchmarkErrorLogs(response, logIndex)
			if err != nil {
				l.Errorf("Failed to write error logs - %v", err)
			}
			errorCount++
			logIndex++
		}
		if _, writeErr := report.analyticsOutput[benchmarkResponse.pkg].WriteString(
			fmt.Sprintf("%s %d ms\n", benchmarkResponse.name,
				response.Duration.Milliseconds())); writeErr != nil {
			l.Errorf("Failed to write analytics to file - %v", writeErr)
		}
		if len(benchmarkResults) == 0 {
			missingBenchmarks[benchmarkResponse.benchmark]++
		}
	})

	fmt.Println()
	l.Printf("Completed benchmarks, results located at %s for time %s", workingDir, timestamp.Format(timeFormat))
	if len(missingBenchmarks) > 0 {
		l.Errorf("Failed to find results for %d benchmarks", len(missingBenchmarks))
		l.Errorf("Missing benchmarks %v", missingBenchmarks)
	}
	if errorCount != 0 {
		if *flagLenient {
			l.Printf("Ignoring errors in benchmark results (lenient flag was set)")
		} else {
			return errors.Newf("Found %d errors during remote execution", errorCount)
		}
	}
	return nil
}

// getRegexExclusionPairs returns a list of regex exclusion pairs, separated by
// comma, derived from the command flags. The first element of the pair is the
// package regex and the second is the microbenchmark regex.
func getRegexExclusionPairs() [][]*regexp.Regexp {
	if *flagExclude == "" {
		return nil
	}
	excludeRegexes := make([][]*regexp.Regexp, 0)
	excludeList := strings.Split(*flagExclude, ",")
	for _, pair := range excludeList {
		pairSplit := strings.Split(pair, ":")
		var pkgRegex, benchRegex *regexp.Regexp
		if len(pairSplit) != 2 {
			pkgRegex = regexp.MustCompile(".*")
			benchRegex = regexp.MustCompile(pairSplit[0])
		} else {
			pkgRegex = regexp.MustCompile(pairSplit[0])
			benchRegex = regexp.MustCompile(pairSplit[1])
		}
		excludeRegexes = append(excludeRegexes, []*regexp.Regexp{pkgRegex, benchRegex})
	}
	return excludeRegexes
}

// extractBenchmarkResults extracts the microbenchmark results generated by a
// test binary and reports if any failures were found in the output. This method
// makes specific assumptions regarding the format of the output, and attempts
// to ignore any spurious output that the test binary may have logged. The
// returned list of string arrays each represent a row of metrics as outputted
// by the test binary.
func extractBenchmarkResults(benchmarkOutput string) ([][]string, bool) {
	keywords := map[string]struct{}{
		"ns/op":     {},
		"b/op":      {},
		"allocs/op": {},
	}
	results := make([][]string, 0)
	buf := make([]string, 0)
	containsErrors := false
	var benchName string
	for _, line := range strings.Split(benchmarkOutput, "\n") {
		elems := strings.Fields(line)
		for _, s := range elems {
			if !containsErrors {
				containsErrors = strings.Contains(s, "FAIL") || strings.Contains(s, "panic")
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
	return results, containsErrors
}
