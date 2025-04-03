// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/parser"
	"github.com/cockroachdb/errors"
)

type cleanConfig struct {
	inputFilePath  string
	outputFilePath string
}

type clean struct {
	cleanConfig
	inputFile *os.File
}

func newClean(config cleanConfig) (*clean, error) {
	file, err := os.Open(config.inputFilePath)
	if err != nil {
		return nil, err
	}

	return &clean{cleanConfig: config, inputFile: file}, nil
}

func (c *clean) writeCleanOutputToFile(
	cleanedBenchmarkOutputLog parser.BenchmarkParseResults,
) error {

	if err := os.MkdirAll(filepath.Dir(c.outputFilePath), os.ModePerm); err != nil {
		return err
	}

	outputFile, err := os.Create(c.outputFilePath)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	for _, benchmarkResult := range cleanedBenchmarkOutputLog.Results {
		if _, writeErr := outputFile.WriteString(
			fmt.Sprintf("%s\n", strings.Join(benchmarkResult, " "))); writeErr != nil {
			return errors.Wrap(writeErr, "failed to write benchmark result to file")
		}
	}

	return nil
}

func (c *clean) cleanBenchmarkOutputLog() error {
	defer c.inputFile.Close()

	rawBenchmarkLogs, err := io.ReadAll(c.inputFile)
	if err != nil {
		return err
	}

	cleanedBenchmarkOutputLog := parser.ExtractBenchmarkResults(string(rawBenchmarkLogs))
	if err = c.writeCleanOutputToFile(cleanedBenchmarkOutputLog); err != nil {
		return err
	}

	return nil
}
