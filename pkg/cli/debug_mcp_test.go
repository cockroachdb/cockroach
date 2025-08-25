// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"encoding/json"
	"testing"
)

func TestQueryTSDump(t *testing.T) {
	testFile := "/Users/armahishi/tsdump/tsdump_oct_1_2"

	resultJSON := queryTSDumpTool(map[string]any{
		"file":   testFile,
		"metric": "cr.node.sys.cpu.host.combined.percent-normalized",
	})

	var resultMap map[string]any
	if err := json.Unmarshal([]byte(resultJSON), &resultMap); err != nil {
		t.Fatalf("failed to parse JSON result: %v", err)
	}

	if errMsg, hasErr := resultMap["error"].(string); hasErr {
		t.Skipf("query error (file may not exist or metric not found): %s", errMsg)
	}

	dataPoints, ok := resultMap["data_points"].([]any)
	if !ok || len(dataPoints) == 0 {
		t.Fatal("no data points returned")
	}

	stats, ok := resultMap["stats"].(map[string]any)
	if !ok {
		t.Fatal("stats field missing or wrong type")
	}
	if stats["count"] == nil || stats["mean"] == nil {
		t.Fatal("stats missing required fields")
	}

	anomalies, ok := resultMap["anomalies"].(map[string]any)
	if !ok {
		t.Fatal("anomalies field missing or wrong type")
	}
	if anomalies["outliers"] == nil || anomalies["spikes"] == nil {
		t.Fatal("anomalies missing required fields")
	}
}
