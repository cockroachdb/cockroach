// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bufio"
	"os"
	"reflect"
	"strings"
)

type Metadata struct {
	ExperimentCommitTime string `field:"experiment-commit-time"`
	Repository           string `field:"repository"`
	BaselineCommit       string `field:"baseline-commit"`
	GoOS                 string `field:"goos"`
	ExperimentCommit     string `field:"experiment-commit"`
	RunTime              string `field:"run-time"`
	BenchmarksCommit     string `field:"benchmarks-commit"`
	Machine              string `field:"machine"`
	GoArch               string `field:"goarch"`
}

// getPackagesFromLogs scans a directory for benchmark report logs and
// creates a list of packages that were used to generate the logs.
func getPackagesFromLogs(dir string) ([]string, error) {
	packages := make([]string, 0)
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if isReportLog(file.Name()) {
			packages = append(packages, getPackageFromReportLogName(file.Name()))
		}
	}
	return packages, nil
}

// loadMetadata reads a Go benchmark metadata file and returns a Metadata
// struct.
func loadMetadata(logFile string) (Metadata, error) {
	metadata := Metadata{}
	file, err := os.Open(logFile)
	if err != nil {
		return metadata, err
	}
	defer file.Close()

	metadataMap := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		metadataMap[key] = value
	}

	if err = scanner.Err(); err != nil {
		return metadata, err
	}

	v := reflect.ValueOf(&metadata).Elem()
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		fieldName := field.Tag.Get("field")
		if value, ok := metadataMap[fieldName]; ok {
			v.Field(i).SetString(value)
		}
	}

	return metadata, nil
}
