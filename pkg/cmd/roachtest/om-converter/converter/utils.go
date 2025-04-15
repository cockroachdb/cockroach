// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package converter

import (
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
)

// FileOutputs defines the output file names
const (
	RawStatsFile        = "stats.om"
	AggregatedStatsFile = "aggregated_stats.om"
)

var SkippedPostProcessTests []string

var GlobalLabels []model.Label

type CommitMapping map[string]string

var pathPattern = regexp.MustCompile(`(\d+-+\d+)/(.*?)/(cpu_arch=\w+|run_\d+)`)

// Global variable to store the commit mapping
var commitMapping CommitMapping

// SetCommitMapping sets the global commit mapping
func SetCommitMapping(mapping CommitMapping) {
	commitMapping = mapping
}

func SetSkippedPostProcessTests(tests []string) {
	SkippedPostProcessTests = tests
}
func getLabelMap(
	file model.FileInfo,
	labels []model.Label,
	extraLabelsRequired bool,
	metricName string,
	metricLabels []model.Label,
) map[string]string {
	runDateAndId, test := getTestDateAndName(file)
	labelMap := make(map[string]string)
	if extraLabelsRequired {
		for _, label := range labels {
			labelMap[label.Name] = label.Value
		}

		if metricName != "" && metricLabels != nil {
			for _, label := range metricLabels {
				labelMap[label.Name] = label.Value
			}
		}
	}

	for _, label := range GlobalLabels {
		labelMap[label.Name] = label.Value
	}

	runId := strings.Split(runDateAndId, "-")[1]
	labelMap["test-run-id"] = fmt.Sprintf("teamcity-%s", runId)
	labelMap["test"] = test
	if commitMapping != nil {
		if commitSHA, ok := commitMapping[runId]; ok {
			labelMap["commit"] = commitSHA
		}
	}
	return labelMap
}

// getTestDateAndName extracts the run date/ID and test name from a file path.
// The first return value is the run date and ID (from the path pattern),
// and the second return value is the test path before run_N.
func getTestDateAndName(file model.FileInfo) (runDateAndID string, testName string) {
	matches := pathPattern.FindStringSubmatch(file.Path)
	if matches == nil {
		return "", file.Path
	}

	return matches[1], matches[2]
}

func LoadCommitMappingFromCSV(filePath string) (CommitMapping, error) {
	if filePath == "" {
		return nil, nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open commit mapping file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read commit mapping file: %w", err)
	}

	mapping := make(CommitMapping)
	for _, record := range records {
		if len(record) >= 2 {
			runID := record[0]
			commitSHA := record[1]
			mapping[runID] = commitSHA
		}
	}
	return mapping, nil
}

func SetGlobalLabels(labels []model.Label) {
	GlobalLabels = labels
}
