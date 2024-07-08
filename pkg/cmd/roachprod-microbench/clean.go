// Copyright 2024 The Cockroach Authors.
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
	"strings"
)

type cleanConfig struct {
	inputFilePath  string
	outputFilePath string
}

type clean struct {
	cleanConfig
	inputFile *os.File

	ctx context.Context
}

func defaultCleanConfig() cleanConfig {
	return cleanConfig{
		inputFilePath:  "",
		outputFilePath: "",
	}
}

func newClean(config cleanConfig) (*clean, error) {
	file, err := os.Open(config.inputFilePath)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	return &clean{cleanConfig: config, inputFile: file, ctx: ctx}, nil
}

func (c *clean) writeCleanOutputToFile(cleanedBenchmarkOutputLog benchmarkExtractionResult) error {

	if err := os.MkdirAll(c.outputFilePath[:strings.LastIndex(c.outputFilePath, "/")], os.ModePerm); err != nil {
		return err
	}

	outputFile, err := os.Create(c.outputFilePath)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	for _, benchmarkResult := range cleanedBenchmarkOutputLog.results {
		if _, writeErr := outputFile.WriteString(
			fmt.Sprintf("%s\n", strings.Join(benchmarkResult, " "))); writeErr != nil {
			return fmt.Errorf("failed to write benchmark result to file - %v", writeErr)
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

	cleanedBenchmarkOutputLog := extractBenchmarkResults(string(rawBenchmarkLogs))
	if err = c.writeCleanOutputToFile(cleanedBenchmarkOutputLog); err != nil {
		return err
	}

	return nil
}
