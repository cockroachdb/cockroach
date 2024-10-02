// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package engflow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConstructJSONReport(t *testing.T) {
	const (
		label1 = "//pkg/cmd/dev:dev_test"
		label2 = "//pkg/build:build_test"
		label3 = "//pkg/build/util:util_test"
	)
	i := &InvocationInfo{
		InvocationId:      "fake-invocation-id",
		StartedTimeMillis: 1705611047000,
		FinishTimeMillis:  1705611054000,
		TestResults: map[string][]*TestResultWithXml{
			label1: {
				&TestResultWithXml{
					Label: label1,
					TestXml: `<testsuites>
	<testsuite errors="0" failures="0" skipped="0" tests="16" time="0.028" name="github.com/cockroachdb/cockroach/pkg/cmd/dev">
		<testcase classname="dev" name="TestDataDriven" time="0.010">
			<failure message="Failed" type="">=== RUN   TestDataDriven&#xA;--- FAIL: TestDataDriven (0.01s)&#xA;</failure>
		</testcase>
		<testcase classname="dev" name="TestRecorderDriven" time="0.006"></testcase>
	</testsuite>
</testsuites>`,
				},
			},
			label2: {
				&TestResultWithXml{
					Label: label2,
					TestXml: `<testsuites>
	<testsuite errors="1" failures="0" skipped="0" tests="1" time="" name="github.com/cockroachdb/cockroach/pkg/build">
		<testcase classname="build" name="TestComputeBinaryVersion" time="">
			<error message="No pass/skip/fail event found for test" type="">=== RUN   TestComputeBinaryVersion&#xA;--- FAIL: TestComputeBinaryVersion (0.00s)&#xA;panic</error>
		</testcase>
	</testsuite>
</testsuites>`,
				},
			},
			label3: {
				&TestResultWithXml{
					Label: label3,
					TestXml: `<testsuites>
	<testsuite errors="0" failures="0" skipped="0" tests="3" time="0.010" name="github.com/cockroachdb/cockroach/pkg/build/util">
		<testcase classname="util" name="TestMergeXml" time="0.003"></testcase>
		<testcase classname="util" name="TestOutputOfBinaryRule" time="0.004"></testcase>
		<testcase classname="util" name="TestOutputsOfGenrule" time="0.002"></testcase>
	</testsuite>
</testsuites>`,
				},
			},
		},
	}

	jsonReport, errs := ConstructJSONReport(i, "tanzanite")
	require.Empty(t, errs)
	require.Equal(t, jsonReport, JsonReport{
		Server:       "tanzanite",
		InvocationId: "fake-invocation-id",
		StartedAt:    "2024-01-18T20:50:47Z",
		FinishedAt:   "2024-01-18T20:50:54Z",
		ResultsByLabel: map[string][]JsonTestResult{
			label1: {
				JsonTestResult{
					TestName:       "TestDataDriven",
					Status:         "FAILURE",
					DurationMillis: 10,
				},
				JsonTestResult{
					TestName:       "TestRecorderDriven",
					Status:         "SUCCESS",
					DurationMillis: 6,
				},
			},
			label2: {
				JsonTestResult{
					TestName: "TestComputeBinaryVersion",
					Status:   "ERROR",
				},
			},
			label3: {
				JsonTestResult{
					TestName:       "TestMergeXml",
					Status:         "SUCCESS",
					DurationMillis: 3,
				},
				JsonTestResult{
					TestName:       "TestOutputOfBinaryRule",
					Status:         "SUCCESS",
					DurationMillis: 4,
				},
				JsonTestResult{
					TestName:       "TestOutputsOfGenrule",
					Status:         "SUCCESS",
					DurationMillis: 2,
				},
			},
		},
	})
}
