package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

const timeFormat = "2006-01-02T15_04_05Z07:00"

var (
	l             *logger.Logger
	flags         = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flagCopy      = flags.Bool("copy", true, "copy roachbench artifacts and libraries to the target cluster")
	flagLibDir    = flags.String("libdir", "lib.docker_amd64", "location of the directory containing the built geos directories")
	flagVerbose   = flags.Bool("verbose", false, "realtime verbose output from roachprod commands")
	flagLogStderr = flags.Bool("log-stderr", false, "log stderr output from roachprod commands")
	flagRemoteDir = flags.String("remotedir", "/mnt/data1", "roachbench artifacts directory on the target cluster")
	benchDir      string
	timestamp     time.Time
	testArgs      []string
	cluster       string
)

type Benchmark struct {
	pkg  string
	name string
}

func verifyArtifactsExist(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return errors.Wrapf(err, "the benchdir flag %q is not a directory relative to the current working directory", dir)
	}
	if !fi.Mode().IsDir() {
		return fmt.Errorf("the benchdir flag %q is not a directory relative to the current working directory", dir)
	}
	return nil
}

func initLogger() {
	loggerCfg := logger.Config{Stdout: os.Stdout, Stderr: os.Stderr}
	var loggerError error
	l, loggerError = loggerCfg.NewLogger(filepath.Join(benchDir,
		fmt.Sprintf("roachprod-bench-%s.log", timestamp.Format(timeFormat))))
	if loggerError != nil {
		_, _ = fmt.Fprintf(os.Stderr, "unable to configure logger: %s\n", loggerError)
		os.Exit(1)
	}
}

func initRoachprod() {
	_ = roachprod.InitProviders()
	if _, err := roachprod.Sync(l); err != nil {
		l.Printf("Failed to sync roachprod data - %v", err)
		os.Exit(1)
	}
}

func logResponseErrors(response RemoteResponse) {
	if response.err != nil || (*flagLogStderr && strings.TrimSpace(response.stderr) != "") {
		l.Errorf("Remote command = {%s}, stderr output = {%s}", strings.Join(response.command, " "), response.stderr)
	}
	if response.err != nil {
		l.Errorf("Remote command = {%s}, error = {%v}", strings.Join(response.command, " "), response.err)
	}
}

func setupVars() error {
	flags.Usage = func() {
		_, _ = fmt.Fprintf(flags.Output(), "usage: %s <benchdir> <cluster> [<flags>] -- [<args>]\n", flags.Name())
		flags.PrintDefaults()
	}

	if len(os.Args) < 3 {
		var b bytes.Buffer
		flags.SetOutput(&b)
		flags.Usage()
		return errors.Newf("%s", b.String())
	}

	testArgs = getTestArgs()
	if err := flags.Parse(os.Args[3:]); err != nil {
		return err
	}

	timestamp = timeutil.Now()
	benchDir = strings.TrimRight(os.Args[1], "/")
	cluster = os.Args[2]

	err := verifyArtifactsExist(benchDir)
	if err != nil {
		return err
	}

	return nil
}

func roachprodRun(clusterName string, cmdArray []string) error {
	return roachprod.Run(context.Background(), l, clusterName, "", "", false, os.Stdout, os.Stderr, cmdArray)
}

func run() error {
	if err := setupVars(); err != nil {
		return err
	}
	initLogger()
	initRoachprod()
	err := openReports()
	if err != nil {
		return err
	}
	defer closeReports()

	pkgs, err := readManifest(benchDir)
	if err != nil {
		return err
	}
	buildHash := filepath.Base(benchDir)

	// Locate binaries tarball.
	ext := "tar"
	if _, osErr := os.Stat(filepath.Join(benchDir, "bin.tar.gz")); osErr == nil {
		ext = "tar.gz"
	}
	if _, osErr := os.Stat(filepath.Join(benchDir, fmt.Sprintf("bin.%s", ext))); oserror.IsNotExist(osErr) {
		l.Errorf("bin.%s does not exist in %s", ext, benchDir)
		return osErr
	}
	binPath := fmt.Sprintf("%s/bin.%s", benchDir, ext)
	remoteBinName := fmt.Sprintf("roachbench-%s.%s", buildHash, ext)
	remoteDir := fmt.Sprintf("%s/roachbench-%s", *flagRemoteDir, buildHash)

	if *flagCopy {
		if fi, cmdErr := os.Stat(*flagLibDir); cmdErr == nil && fi.IsDir() {
			if putErr := roachprod.Put(context.Background(), l, cluster, *flagLibDir, "lib", true); putErr != nil {
				return putErr
			}
		}
		if err = roachprod.Put(context.Background(), l, cluster, binPath, remoteBinName, true); err != nil {
			return err
		}

		// Clear old build artifacts.
		if err = roachprodRun(cluster, []string{"rm", "-rf", remoteDir}); err != nil {
			return errors.Wrap(err, "failed to remove old build artifacts")
		}

		// Copy and extract new build artifacts.
		if err = roachprodRun(cluster, []string{"mkdir", "-p", remoteDir}); err != nil {
			return err
		}
		extractFlags := "-xf"
		if ext == "tar.gz" {
			extractFlags = "-xzf"
		}
		if err = roachprodRun(cluster, []string{"tar", extractFlags, remoteBinName, "-C", remoteDir}); err != nil {
			return err
		}
	}

	// Extract binaries to target remote directory.
	// flagRemoteDir

	statuses, err := roachprod.Status(context.Background(), l, cluster, "")
	if err != nil {
		return err
	}
	numNodes := len(statuses)

	commands := make([]RemoteCommand, 0)
	for _, pkg := range pkgs {
		command := RemoteCommand{
			[]string{"sh", "-c", fmt.Sprintf("\"cd %s/%s/bin && ./run.sh -test.list=Benchmark*\"", remoteDir, pkg)},
			pkg,
		}
		commands = append(commands, command)
	}

	l.Printf("Distributing and running benchmark listings across cluster %s\n", cluster)
	isValidBenchmarkName := regexp.MustCompile(`^Benchmark[a-zA-Z0-9_]+$`).MatchString
	errorCount := 0
	benchmarks := make([]Benchmark, 0)
	counts := make(map[string]int)
	executeRemoteCommands(cluster, commands, numNodes, false, func(response RemoteResponse) {
		fmt.Print(".")
		logResponseErrors(response)
		if response.err == nil {
			pkg := response.metadata.(string)
			for _, benchmark := range strings.Split(response.stdout, "\n") {
				benchmark = strings.TrimSpace(benchmark)
				if isValidBenchmarkName(benchmark) {
					benchmarks = append(benchmarks, Benchmark{pkg, benchmark})
					counts[pkg]++
				} else if benchmark != "" {
					l.Printf("Ignoring invalid benchmark name: %s", benchmark)
				}
			}
		}
		if response.err != nil {
			errorCount++
		}
	})

	fmt.Println()
	for pkg, count := range counts {
		l.Printf("Found %d benchmarks in %s\n", count, pkg)
	}

	commands = make([]RemoteCommand, 0)
	for _, benchmark := range benchmarks {
		command := RemoteCommand{
			[]string{"sh", "-c", fmt.Sprintf("\"cd %s/%s/bin && ./run.sh %s -test.benchmem -test.bench=^%s$ -test.run=^$\"",
				remoteDir, benchmark.pkg, strings.Join(testArgs, " "), benchmark.name)},
			benchmark,
		}
		commands = append(commands, command)
	}

	missingBenchmarks := make([]Benchmark, 0)
	l.Printf("Found %d benchmarks, distributing and running benchmarks across cluster %s\n", len(benchmarks), cluster)
	executeRemoteCommands(cluster, commands, numNodes, false, func(response RemoteResponse) {
		fmt.Print(".")
		logResponseErrors(response)
		if response.err != nil {
			errorCount++
		}
		benchmarkResults := extractBenchmarkResults(response.stdout)
		benchmark := response.metadata.(Benchmark)
		for _, benchmarkResult := range benchmarkResults {
			if _, writeErr := benchmarkOutput.WriteString(
				fmt.Sprintf("%s/%s\n", benchmark.pkg, strings.Join(benchmarkResult, " "))); writeErr != nil {
				l.Errorf("Failed to write benchmark result to file - %v", writeErr)
			}
		}
		if _, writeErr := analyticsOutput.WriteString(
			fmt.Sprintf("%s/%s %d ms\n", benchmark.pkg, benchmark.name, response.duration.Milliseconds())); writeErr != nil {
			l.Errorf("Failed to write analytics to file - %v", writeErr)
		}
		if len(benchmarkResults) == 0 {
			missingBenchmarks = append(missingBenchmarks, benchmark)
		}
	})

	fmt.Println()
	if len(missingBenchmarks) > 0 {
		l.Errorf("Failed to find results for %d benchmarks", len(missingBenchmarks))
		l.Errorf("Missing benchmarks %v", missingBenchmarks)
	}
	if errorCount != 0 {
		l.Errorf("Found %d errors during remote execution", errorCount)
	}

	l.Printf("Completed benchmarks, results located at %s for time %s\n", benchDir, timestamp.Format(timeFormat))
	return nil
}

func main() {
	ssh.InsecureIgnoreHostKey = true
	if err := run(); err != nil {
		if l != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
		} else {
			l.Printf("Encountered error during run %v", err)
		}
		fmt.Println("FAIL")
		os.Exit(1)
	} else {
		fmt.Println("SUCCESS")
	}
}

func getTestArgs() (ret []string) {
	if len(os.Args) > 3 {
		flagsAndArgs := os.Args[3:]
		for i, arg := range flagsAndArgs {
			if arg == "--" {
				ret = flagsAndArgs[i+1:]
				break
			}
		}
	}
	return ret
}
