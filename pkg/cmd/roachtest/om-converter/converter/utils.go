// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package converter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
)

// FileOutputs defines the output file names
const (
	RawStatsFile        = "stats.om"
	AggregatedStatsFile = "aggregated_stats.om"
)

var pathPattern = regexp.MustCompile(`(\d+-+\d+)/(.*?)/run_\d+`)

func getLabelMap(file model.FileInfo, labels []model.Label) map[string]string {
	runDateAndId, test := getTestDateAndName(file)
	labelMap := make(map[string]string)
	for _, label := range labels {
		labelMap[label.Name] = label.Value
	}

	runId := strings.Split(runDateAndId, "-")[1]
	labelMap["test-run-id"] = fmt.Sprintf("teamcity-%s", runId)
	labelMap["test"] = test
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
