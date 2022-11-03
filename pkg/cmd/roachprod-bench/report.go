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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-bench/cluster"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

const (
	reportLogName    = "report"
	analyticsLogName = "analytics"
)

var (
	benchmarkOutput map[string]*os.File
	analyticsOutput map[string]*os.File
)

func initLogger(path string) {
	loggerCfg := logger.Config{Stdout: os.Stdout, Stderr: os.Stderr}
	var loggerError error
	l, loggerError = loggerCfg.NewLogger(path)
	if loggerError != nil {
		_, _ = fmt.Fprintf(os.Stderr, "unable to configure logger: %s\n", loggerError)
		os.Exit(1)
	}
}

func getReportLogName(name string, pkg string) string {
	pkgFormatted := strings.ReplaceAll(pkg, "/", "-")
	return fmt.Sprintf("%s-%s.log", pkgFormatted, name)
}

func createReports(packages []string) error {
	benchmarkOutput = make(map[string]*os.File)
	analyticsOutput = make(map[string]*os.File)
	var err error
	for _, pkg := range packages {
		benchmarkOutput[pkg], err = os.Create(filepath.Join(logOutputDir, getReportLogName(reportLogName, pkg)))
		if err != nil {
			return err
		}
		analyticsOutput[pkg], err = os.Create(filepath.Join(logOutputDir, getReportLogName(analyticsLogName, pkg)))
		if err != nil {
			return err
		}
	}
	return nil
}

func closeReports() {
	for _, file := range benchmarkOutput {
		err := file.Close()
		if err != nil {
			l.Errorf("Error closing benchmark output file: %s", err)
		}
	}
	for _, file := range analyticsOutput {
		err := file.Close()
		if err != nil {
			l.Errorf("Error closing analytics output file: %s", err)
		}
	}
}

func writeBenchmarkErrorLogs(response cluster.RemoteResponse, index int) error {
	benchmarkResponse := response.Metadata.(benchmark)
	stdoutLogName := fmt.Sprintf("%s-%d-stdout.log", benchmarkResponse.name, index)
	stderrLogName := fmt.Sprintf("%s-%d-stderr.log", benchmarkResponse.name, index)
	l.Printf("Writing error logs for benchmark at %s, %s\n", stdoutLogName, stderrLogName)
	stdoutFile, err := os.Create(filepath.Join(logOutputDir, stdoutLogName))
	if err != nil {
		return err
	}
	stderrFile, err := os.Create(filepath.Join(logOutputDir, stderrLogName))
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := stdoutFile.Close(); closeErr != nil {
			l.Errorf("Error closing stdout file: %s", closeErr)
		}
		if closeErr := stderrFile.Close(); closeErr != nil {
			l.Errorf("Error closing stderr file: %s", closeErr)
		}
	}()

	_, err = stdoutFile.WriteString(response.Stdout)
	if err != nil {
		return err
	}

	var buffer strings.Builder
	buffer.WriteString(fmt.Sprintf("Remote command: %s\n", strings.Join(response.Args, " ")))
	if response.Err != nil {
		buffer.WriteString(fmt.Sprintf("Remote error: %s\n", response.Err))
	}
	buffer.WriteString(response.Stderr)
	_, err = stderrFile.WriteString(buffer.String())
	if err != nil {
		return err
	}

	return err
}
