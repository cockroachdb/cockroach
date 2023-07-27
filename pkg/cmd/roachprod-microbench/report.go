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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/cluster"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

const (
	reportLogName    = "report"
	analyticsLogName = "analytics"
)

type Report struct {
	benchmarkOutput map[string]*os.File
	analyticsOutput map[string]*os.File
	path            string
}

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
	pkgFormatted := strings.ReplaceAll(pkg, "/", "_")
	return fmt.Sprintf("%s-%s.log", pkgFormatted, name)
}

func getPackageFromReportLogName(name string) string {
	pkg := strings.ReplaceAll(name, "_", "/")
	pkg = strings.TrimSuffix(pkg, "-"+reportLogName+".log")
	return pkg
}

func isReportLog(name string) bool {
	return strings.HasSuffix(name, reportLogName+".log")
}

func (report *Report) createReports(path string, packages []string) error {
	report.benchmarkOutput = make(map[string]*os.File)
	report.analyticsOutput = make(map[string]*os.File)
	report.path = path

	var err error
	for _, pkg := range packages {
		report.benchmarkOutput[pkg], err = os.Create(filepath.Join(path, getReportLogName(reportLogName, pkg)))
		if err != nil {
			return err
		}
		report.analyticsOutput[pkg], err = os.Create(filepath.Join(path, getReportLogName(analyticsLogName, pkg)))
		if err != nil {
			return err
		}
	}
	return nil
}

func (report *Report) closeReports() {
	for _, file := range report.benchmarkOutput {
		err := file.Close()
		if err != nil {
			l.Errorf("Error closing benchmark output file: %s", err)
		}
	}
	for _, file := range report.analyticsOutput {
		err := file.Close()
		if err != nil {
			l.Errorf("Error closing analytics output file: %s", err)
		}
	}
}

func (report *Report) writeBenchmarkErrorLogs(response cluster.RemoteResponse, index int) error {
	benchmarkResponse := response.Metadata.(benchmarkIndexed)
	stdoutLogName := fmt.Sprintf("%s-%d-stdout.log", benchmarkResponse.name, index)
	stderrLogName := fmt.Sprintf("%s-%d-stderr.log", benchmarkResponse.name, index)
	l.Printf("Writing error logs for benchmark at %s, %s\n", stdoutLogName, stderrLogName)

	if err := os.WriteFile(filepath.Join(report.path, stdoutLogName), []byte(response.Stdout), 0644); err != nil {
		return err
	}

	var buffer strings.Builder
	buffer.WriteString(fmt.Sprintf("Remote command: %s\n", strings.Join(response.Args, " ")))
	if response.Err != nil {
		buffer.WriteString(fmt.Sprintf("Remote error: %s\n", response.Err))
	}
	buffer.WriteString(response.Stderr)

	if err := os.WriteFile(filepath.Join(report.path, stderrLogName), []byte(buffer.String()), 0644); err != nil {
		return err
	}

	return nil
}
