// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

type report struct {
	log             *logger.Logger
	benchmarkOutput map[string]*os.File
	analyticsOutput map[string]*os.File
	path            string
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

func (report *report) createReports(log *logger.Logger, path string, packages []string) error {
	report.log = log
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

func (report *report) closeReports() {
	for _, file := range report.benchmarkOutput {
		err := file.Close()
		if err != nil {
			report.log.Errorf("Error closing benchmark output file: %s", err)
		}
	}
	for _, file := range report.analyticsOutput {
		err := file.Close()
		if err != nil {
			report.log.Errorf("Error closing analytics output file: %s", err)
		}
	}
}

func (report *report) writeBenchmarkErrorLogs(response cluster.RemoteResponse, tag string) error {
	benchmarkResponse := response.Metadata.(benchmarkKey)
	stdoutLogName := fmt.Sprintf("%s-%s-stdout.log", benchmarkResponse.name, tag)
	stderrLogName := fmt.Sprintf("%s-%s-stderr.log", benchmarkResponse.name, tag)
	report.log.Printf("Writing error logs for benchmark at %s, %s\n", stdoutLogName, stderrLogName)

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
