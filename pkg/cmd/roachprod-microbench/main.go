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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/google"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const timeFormat = "2006-01-02T15_04_05"

var (
	l                   *logger.Logger
	flags               = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flagCluster         = flags.String("cluster", "", "cluster to run the benchmarks on")
	flagLibDir          = flags.String("libdir", "bin/lib", "location of libraries required by test binaries")
	flagBinaries        = flags.String("binaries", "bin/test_binaries.tar.gz", "portable test binaries archive built with dev test-binaries")
	flagCompareBinaries = flags.String("compare-binaries", "", "run additional binaries from this archive and compare the results")
	flagRemoteDir       = flags.String("remotedir", "/mnt/data1", "working directory on the target cluster")
	flagCompareDir      = flags.String("comparedir", "", "directory with reports to compare the results of the benchmarks against (produces a comparison sheet)")
	flagPublishDir      = flags.String("publishdir", "", "directory to publish the reports of the benchmarks to")
	flagSheetDesc       = flags.String("sheet-desc", "", "append a description to the sheet title when doing a comparison")
	flagExclude         = flags.String("exclude", "", "comma-separated regex of packages and benchmarks to exclude e.g. 'pkg/util/.*:BenchmarkIntPool,pkg/sql:.*'")
	flagTimeout         = flags.String("timeout", "", "timeout for each benchmark e.g. 10m")
	flagShell           = flags.String("shell", "", "additional shell command to run on node before benchmark execution")
	flagCopy            = flags.Bool("copy", true, "copy and extract test binaries and libraries to the target cluster")
	flagLenient         = flags.Bool("lenient", true, "tolerate errors in the benchmark results")
	flagAffinity        = flags.Bool("affinity", true, "run benchmarks with iterations and binaries having affinity to the same node")
	flagIterations      = flags.Int("iterations", 1, "number of iterations to run each benchmark")
	workingDir          string
	testArgs            []string
	timestamp           time.Time
)

func verifyPathFlag(flagName, path string, expectDir bool) error {
	if fi, err := os.Stat(path); err != nil {
		return fmt.Errorf("the %s flag points to a path %s that does not exist", flagName, path)
	} else {
		switch isDir := fi.Mode().IsDir(); {
		case expectDir && !isDir:
			return fmt.Errorf("the %s flag must point to a directory not a file", flagName)
		case !expectDir && isDir:
			return fmt.Errorf("the %s flag must point to a file not a directory", flagName)
		}
	}
	return nil
}

func setupVars() error {
	flags.Usage = func() {
		_, _ = fmt.Fprintf(flags.Output(), "usage: %s <working dir> [<flags>] -- [<test args>]\n", flags.Name())
		flags.PrintDefaults()
	}

	if len(os.Args) < 2 {
		var b bytes.Buffer
		flags.SetOutput(&b)
		flags.Usage()
		return errors.Newf("%s", b.String())
	}

	workingDir = strings.TrimRight(os.Args[1], "/")
	err := os.MkdirAll(workingDir, os.ModePerm)
	if err != nil {
		return err
	}
	err = verifyPathFlag("working dir", workingDir, true)
	if err != nil {
		return err
	}

	testArgs = getTestArgs()
	if err = flags.Parse(os.Args[2:]); err != nil {
		return err
	}

	// Only require binaries if we are going to run microbenchmarks.
	if *flagCluster != "" {
		if err = verifyPathFlag("binaries archive", *flagBinaries, false); err != nil {
			return err
		}
		if !strings.HasSuffix(*flagBinaries, ".tar") && !strings.HasSuffix(*flagBinaries, ".tar.gz") {
			return fmt.Errorf("the binaries archive must have the extention .tar or .tar.gz")
		}
	}

	if *flagCompareBinaries != "" {
		if err = verifyPathFlag("binaries compare archive", *flagCompareBinaries, false); err != nil {
			return err
		}
		if *flagCompareDir != "" {
			return fmt.Errorf("cannot specify both --compare-binaries and --comparedir")
		}
		*flagCompareDir = filepath.Join(workingDir, "compare")
		err = os.MkdirAll(*flagCompareDir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	timestamp = timeutil.Now()
	initLogger(filepath.Join(workingDir, fmt.Sprintf("roachprod-microbench-%s.log", timestamp.Format(timeFormat))))
	l.Printf("roachprod-microbench %s", strings.Join(os.Args, " "))

	return nil
}

func run() error {
	err := setupVars()
	if err != nil {
		return err
	}

	var packages []string
	if *flagCluster != "" {
		binaries := []string{*flagBinaries}
		if *flagCompareBinaries != "" {
			binaries = append(binaries, *flagCompareBinaries)
		}
		packages, err = readArchivePackages(*flagBinaries)
		if err != nil {
			return err
		}
		err = executeBenchmarks(binaries, packages)
		if err != nil {
			return err
		}
	} else {
		packages, err = getPackagesFromLogs(workingDir)
		if err != nil {
			return err
		}
		l.Printf("No cluster specified, skipping microbenchmark execution")
	}

	if *flagPublishDir != "" {
		err = publishDirectory(workingDir, *flagPublishDir)
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
		tableResults, cErr := compareBenchmarks(packages, workingDir, *flagCompareDir)
		if cErr != nil {
			return cErr
		}
		for pkgGroup, tables := range tableResults {
			sheetName := pkgGroup + "/..."
			if *flagSheetDesc != "" {
				sheetName += " " + *flagSheetDesc
			}
			if pubErr := publishToGoogleSheets(ctx, service, sheetName, tables); pubErr != nil {
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
