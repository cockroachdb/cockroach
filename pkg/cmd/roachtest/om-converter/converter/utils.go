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

var excludePathLabels = regexp.MustCompile(`run_(\d+)|(\d+).perf|stats.json`)

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

// getTestDateAndName removes the first part of the path up to and including the first '/'
func getTestDateAndName(file model.FileInfo) (string, string) {
	actualPath := strings.TrimPrefix(file.Path, file.DirectoryPrefix)

	index := strings.Index(actualPath, "/")
	if index == -1 {
		return "", actualPath
	}

	return actualPath[:index], strings.Trim(excludePathLabels.ReplaceAllString(actualPath[index:], ""), `/`)
}
