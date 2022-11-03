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
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-bench/google"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const timeFormat = "2006-01-02T15_04_05"

var (
	l                *logger.Logger
	flags            = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flagCluster      = flags.String("cluster", "", "cluster to run the benchmarks on")
	flagLibDir       = flags.String("libdir", "lib.docker_amd64", "location of libraries required by test binaries")
	flagRemoteDir    = flags.String("remotedir", "/mnt/data1", "roachbench working directory on the target cluster")
	flagCompareDir   = flags.String("comparedir", "", "directory with reports to compare the results of the benchmarks against")
	flagPublishDir   = flags.String("publishdir", "", "directory to publish the reports of the benchmarks to")
	flagPreviousTime = flags.String("previoustime", "", "timestamp of the previous run to compare against")
	flagExclude      = flags.String("exclude", "", "comma-separated regex of packages and benchmarks to exclude e.g. 'pkg/util/.*:BenchmarkIntPool,pkg/sql:.*'")
	flagCopy         = flags.Bool("copy", true, "copy and extract roachbench artifacts and libraries to the target cluster")
	flagLenient      = flags.Bool("lenient", true, "tolerate errors in the benchmark results")
	flagIterations   = flags.Int("iterations", 1, "number of iterations to run each benchmark")
	logOutputDir     string
	benchDir         string
	timestamp        time.Time
	testArgs         []string
)

type benchmark struct {
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

func setupVars() error {
	flags.Usage = func() {
		_, _ = fmt.Fprintf(flags.Output(), "usage: %s <benchdir> [<flags>] -- [<args>]\n", flags.Name())
		flags.PrintDefaults()
	}

	if len(os.Args) < 2 {
		var b bytes.Buffer
		flags.SetOutput(&b)
		flags.Usage()
		return errors.Newf("%s", b.String())
	}

	testArgs = getTestArgs()
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}

	if *flagPreviousTime == "" {
		timestamp = timeutil.Now()
		if *flagCluster == "" {
			return fmt.Errorf("the cluster flag is required if not comparing against a previous run")
		}
	} else {
		var err error
		timestamp, err = time.Parse(timeFormat, *flagPreviousTime)
		if err != nil {
			return err
		}
	}

	benchDir = strings.TrimRight(os.Args[1], "/")
	logOutputDir = filepath.Join(benchDir, fmt.Sprintf("logs-%s", timestamp.Format(timeFormat)))

	// On a new run the log should output to the timestamped log output directory.
	// On doing only a comparison from a previous run the log should output to
	// bench dir with a timestamp appended to the filename.
	if *flagPreviousTime == "" {
		initLogger(filepath.Join(logOutputDir, "roachprod-bench.log"))
	} else {
		initLogger(filepath.Join(benchDir, fmt.Sprintf("roachprod-bench-%s.log", timestamp.Format(timeFormat))))
	}

	err := verifyArtifactsExist(benchDir)
	if err != nil {
		return err
	}

	return nil
}

func run() error {
	if err := setupVars(); err != nil {
		return err
	}
	packages, err := readManifest(benchDir)
	if err != nil {
		return err
	}

	if *flagPreviousTime == "" {
		err = executeBenchmarks(packages)
		if err != nil {
			return err
		}
	}

	if *flagPublishDir != "" {
		err = publishDirectory(logOutputDir, *flagPublishDir)
		if err != nil {
			return err
		}
	}

	if *flagCompareDir != "" {
		ctx := context.Background()
		service, cErr := google.New(ctx)
		if cErr != nil {
			return cErr
		}
		tableResults, cErr := compareBenchmarks(packages, *flagCompareDir, logOutputDir)
		if cErr != nil {
			return cErr
		}
		for pkgGroup, tables := range tableResults {
			if pubErr := publishToGoogleSheets(ctx, service, pkgGroup+"/...", tables); pubErr != nil {
				return pubErr
			}
		}
	} else {
		l.Printf("No comparison directory specified, skipping comparison\n")
	}

	return nil
}

func main() {
	ssh.InsecureIgnoreHostKey = true
	if err := run(); err != nil {
		if l != nil {
			l.Errorf("Failed with error: %v", err)
		} else {
			_, _ = fmt.Fprintln(os.Stderr, err)
		}
		fmt.Println("FAIL")
		os.Exit(1)
	} else {
		fmt.Println("SUCCESS")
	}
}

func getTestArgs() (ret []string) {
	if len(os.Args) > 2 {
		flagsAndArgs := os.Args[2:]
		for i, arg := range flagsAndArgs {
			if arg == "--" {
				ret = flagsAndArgs[i+1:]
				break
			}
		}
	}
	return ret
}
