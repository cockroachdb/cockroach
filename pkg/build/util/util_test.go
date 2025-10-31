// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"bytes"
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOutputOfBinaryRule(t *testing.T) {
	require.Equal(t, OutputOfBinaryRule("//pkg/cmd/cockroach-short", false),
		"pkg/cmd/cockroach-short/cockroach-short_/cockroach-short")
	require.Equal(t, OutputOfBinaryRule("//pkg/cmd/cockroach-short", true),
		"pkg/cmd/cockroach-short/cockroach-short_/cockroach-short.exe")
	require.Equal(t, OutputOfBinaryRule("//pkg/cmd/cockroach-short:cockroach-short", false),
		"pkg/cmd/cockroach-short/cockroach-short_/cockroach-short")
	require.Equal(t, OutputOfBinaryRule("//pkg/cmd/cockroach-short:cockroach-short", true),
		"pkg/cmd/cockroach-short/cockroach-short_/cockroach-short.exe")
	require.Equal(t, OutputOfBinaryRule("pkg/cmd/cockroach-short", false),
		"pkg/cmd/cockroach-short/cockroach-short_/cockroach-short")
	require.Equal(t, OutputOfBinaryRule("pkg/cmd/cockroach-short", true),
		"pkg/cmd/cockroach-short/cockroach-short_/cockroach-short.exe")
	require.Equal(t, OutputOfBinaryRule("@com_github_cockroachdb_stress//:stress", false),
		"external/com_github_cockroachdb_stress/stress_/stress")
	require.Equal(t, OutputOfBinaryRule("@com_github_cockroachdb_stress//:stress", true),
		"external/com_github_cockroachdb_stress/stress_/stress.exe")
	require.Equal(t, OutputOfBinaryRule("@com_github_bazelbuild_buildtools//buildifier:buildifier", false),
		"external/com_github_bazelbuild_buildtools/buildifier/buildifier_/buildifier")
	require.Equal(t, OutputOfBinaryRule("@com_github_bazelbuild_buildtools//buildifier:buildifier", true),
		"external/com_github_bazelbuild_buildtools/buildifier/buildifier_/buildifier.exe")
}

func TestOutputsOfGenrule(t *testing.T) {
	xmlQueryOutput := `<?xml version="1.1" encoding="UTF-8" standalone="no"?>
<query version="2">
    <rule class="genrule" location="/Users/ricky/go/src/github.com/cockroachdb/cockroach/docs/generated/sql/BUILD.bazel:1:8" name="//docs/generated/sql:sql">
        <string name="name" value="sql"/>
        <list name="tools">
            <label value="//pkg/cmd/docgen:docgen"/>
        </list>
        <list name="outs">
            <output value="//docs/generated/sql:aggregates.md"/>
            <output value="//docs/generated/sql:functions.md"/>
            <output value="//docs/generated/sql:operators.md"/>
            <output value="//docs/generated/sql:window_functions.md"/>
        </list>
        <string name="cmd" value="&#10;$(location //pkg/cmd/docgen) functions $(RULEDIR) --quiet&#10;"/>
        <rule-input name="//pkg/cmd/docgen:docgen"/>
        <rule-input name="@bazel_tools//tools/genrule:genrule-setup.sh"/>
        <rule-output name="//docs/generated/sql:aggregates.md"/>
        <rule-output name="//docs/generated/sql:functions.md"/>
        <rule-output name="//docs/generated/sql:operators.md"/>
        <rule-output name="//docs/generated/sql:window_functions.md"/>
    </rule>
</query>`
	expected := []string{
		"docs/generated/sql/aggregates.md",
		"docs/generated/sql/functions.md",
		"docs/generated/sql/operators.md",
		"docs/generated/sql/window_functions.md",
	}
	out, err := OutputsOfGenrule("//docs/generated/sql:sql", xmlQueryOutput)
	require.NoError(t, err)
	require.Equal(t, out, expected)
	out, err = OutputsOfGenrule("//docs/generated/sql", xmlQueryOutput)
	require.NoError(t, err)
	require.Equal(t, out, expected)
}

func TestMergeXml(t *testing.T) {
	const xml1 = `<testsuites>
	<testsuite errors="0" failures="1" skipped="0" tests="6" time="0.010" name="github.com/cockroachdb/cockroach/pkg/cmd/dev.TestDataDriven" timestamp="2025-10-31T16:31:54.004Z">
		<testcase classname="dev" name="TestDataDriven" time="0.010"></testcase>
		<testcase classname="dev" name="TestDataDriven/bench" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/dev-build" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/generate" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/testlogic" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/ui" time="0.000">
			<failure message="Failed" type="">FAILED</failure>
		</testcase>
	</testsuite>
	<testsuite errors="0" failures="0" skipped="0" tests="11" time="0.002" name="github.com/cockroachdb/cockroach/pkg/cmd/dev.TestRecorderDriven" timestamp="2025-10-31T16:31:54.010Z">
		<testcase classname="dev" name="TestRecorderDriven" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/builder" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/builder#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/dev-build" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/dev-build#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/generate" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/generate#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/lint" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/lint#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/test" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/test#01" time="0.000"></testcase>
	</testsuite>
</testsuites>`
	const xml2 = `<testsuites>
	<testsuite errors="0" failures="0" skipped="0" tests="6" time="0.010" name="github.com/cockroachdb/cockroach/pkg/cmd/dev.TestDataDriven" timestamp="2025-10-31T16:31:54.004Z">
		<testcase classname="dev" name="TestDataDriven" time="0.010"></testcase>
		<testcase classname="dev" name="TestDataDriven/bench" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/dev-build" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/generate" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/testlogic" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/ui" time="0.000"></testcase>
	</testsuite>
	<testsuite errors="0" failures="0" skipped="0" tests="11" time="0.002" name="github.com/cockroachdb/cockroach/pkg/cmd/dev.TestRecorderDriven" timestamp="2025-10-31T16:31:54.010Z">
		<testcase classname="dev" name="TestRecorderDriven" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/builder" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/builder#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/dev-build" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/dev-build#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/generate" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/generate#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/lint" time="0.000">
			<failure message="Failed" type="">FAILED ALSO</failure>
		</testcase>
		<testcase classname="dev" name="TestRecorderDriven/lint#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/test" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/test#01" time="0.000"></testcase>
	</testsuite>
</testsuites>`
	const expected = `<testsuites>
	<testsuite errors="0" failures="1" skipped="0" tests="6" time="0.01" name="github.com/cockroachdb/cockroach/pkg/cmd/dev.TestDataDriven" timestamp="2025-10-31T16:31:54.004Z">
		<testcase classname="dev" name="TestDataDriven" time="0.010"></testcase>
		<testcase classname="dev" name="TestDataDriven/bench" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/dev-build" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/generate" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/testlogic" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/ui" time="0.000">
			<failure message="Failed" type="">FAILED</failure>
		</testcase>
	</testsuite>
	<testsuite errors="0" failures="1" skipped="0" tests="11" time="0.002" name="github.com/cockroachdb/cockroach/pkg/cmd/dev.TestRecorderDriven" timestamp="2025-10-31T16:31:54.010Z">
		<testcase classname="dev" name="TestRecorderDriven" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/builder" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/builder#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/dev-build" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/dev-build#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/generate" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/generate#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/lint" time="0.000">
			<failure message="Failed" type="">FAILED ALSO</failure>
		</testcase>
		<testcase classname="dev" name="TestRecorderDriven/lint#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/test" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/test#01" time="0.000"></testcase>
	</testsuite>
</testsuites>
`

	var suite1, suite2 TestSuites
	require.NoError(t, xml.Unmarshal([]byte(xml1), &suite1))
	require.NoError(t, xml.Unmarshal([]byte(xml2), &suite2))
	var buf bytes.Buffer
	require.NoError(t, MergeTestXMLs([]TestSuites{suite1, suite2}, &buf))
	require.Equal(t, expected, buf.String())
}

func TestMungeTestXML(t *testing.T) {
	beforeXml := `<testsuites>
	<testsuite errors="0" failures="0" skipped="0" tests="6" time="0.010" name="github.com/cockroachdb/cockroach/pkg/cmd/dev.TestDataDriven" timestamp="2025-10-30T21:36:57.401Z">
		<testcase classname="dev" name="TestDataDriven" time="0.010"></testcase>
		<testcase classname="dev" name="TestDataDriven/bench" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/dev-build" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/generate" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/testlogic" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/ui" time="0.000"></testcase>
	</testsuite>
	<testsuite errors="0" failures="0" skipped="0" tests="11" time="0.010" name="github.com/cockroachdb/cockroach/pkg/cmd/dev.TestRecorderDriven" timestamp="2025-10-30T21:36:57.406Z">
		<testcase classname="dev" name="TestRecorderDriven" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/builder" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/builder#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/dev-build" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/dev-build#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/generate" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/generate#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/lint" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/lint#01" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/test" time="0.000"></testcase>
		<testcase classname="dev" name="TestRecorderDriven/test#01" time="0.000"></testcase>
	</testsuite>
</testsuites>
`

	expected := `<testsuite errors="0" failures="0" skipped="0" tests="17" time="0.02" name="github.com/cockroachdb/cockroach/pkg/cmd/dev" timestamp="2025-10-30T21:36:57.401Z">
	<testcase classname="dev" name="TestDataDriven" time="0.010"></testcase>
	<testcase classname="dev" name="TestDataDriven/bench" time="0.000"></testcase>
	<testcase classname="dev" name="TestDataDriven/dev-build" time="0.000"></testcase>
	<testcase classname="dev" name="TestDataDriven/generate" time="0.000"></testcase>
	<testcase classname="dev" name="TestDataDriven/testlogic" time="0.000"></testcase>
	<testcase classname="dev" name="TestDataDriven/ui" time="0.000"></testcase>
	<testcase classname="dev" name="TestRecorderDriven" time="0.000"></testcase>
	<testcase classname="dev" name="TestRecorderDriven/builder" time="0.000"></testcase>
	<testcase classname="dev" name="TestRecorderDriven/builder#01" time="0.000"></testcase>
	<testcase classname="dev" name="TestRecorderDriven/dev-build" time="0.000"></testcase>
	<testcase classname="dev" name="TestRecorderDriven/dev-build#01" time="0.000"></testcase>
	<testcase classname="dev" name="TestRecorderDriven/generate" time="0.000"></testcase>
	<testcase classname="dev" name="TestRecorderDriven/generate#01" time="0.000"></testcase>
	<testcase classname="dev" name="TestRecorderDriven/lint" time="0.000"></testcase>
	<testcase classname="dev" name="TestRecorderDriven/lint#01" time="0.000"></testcase>
	<testcase classname="dev" name="TestRecorderDriven/test" time="0.000"></testcase>
	<testcase classname="dev" name="TestRecorderDriven/test#01" time="0.000"></testcase>
</testsuite>
`
	var buf bytes.Buffer
	require.NoError(t, MungeTestXML([]byte(beforeXml), &buf))
	require.Equal(t, expected, buf.String())
}
