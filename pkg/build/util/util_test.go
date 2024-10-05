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
	<testsuite errors="0" failures="1" skipped="0" tests="17" time="0.029" name="github.com/cockroachdb/cockroach/pkg/cmd/dev">
		<testcase classname="dev" name="TestDataDriven" time="0.010"></testcase>
		<testcase classname="dev" name="TestDataDriven/bench.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/build.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/builder.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/generate.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/lint.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/logic.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/bench.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/build.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/builder.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/generate.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/lint.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/logic.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/test.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/test.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestSetupPath" time="0.000">
			<failure message="Failed" type="">FAILED :(</failure>
		</testcase>
	</testsuite>
</testsuites>`
	const xml2 = `<testsuites>
	<testsuite errors="0" failures="1" skipped="0" tests="17" time="0.029" name="github.com/cockroachdb/cockroach/pkg/cmd/dev">
		<testcase classname="dev" name="TestDataDriven" time="0.010"></testcase>
		<testcase classname="dev" name="TestDataDriven/bench.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/build.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/builder.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/generate.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/lint.txt" time="0.000">
			<failure message="Failed" type="">ALSO FAILED :(</failure>
		</testcase>
		<testcase classname="dev" name="TestDataDriven/logic.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/bench.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/build.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/builder.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/generate.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/lint.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/logic.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/recording/test.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestDataDriven/test.txt" time="0.000"></testcase>
		<testcase classname="dev" name="TestSetupPath" time="0.000"></testcase>
	</testsuite>
</testsuites>`
	const expected = `<testsuites>
	<testsuite errors="0" failures="1" skipped="0" tests="17" time="0.029" name="github.com/cockroachdb/cockroach/pkg/cmd/dev">
		<testcase name="TestDataDriven" time="0.010"></testcase>
		<testcase name="TestDataDriven/bench.txt" time="0.000"></testcase>
		<testcase name="TestDataDriven/build.txt" time="0.000"></testcase>
		<testcase name="TestDataDriven/builder.txt" time="0.000"></testcase>
		<testcase name="TestDataDriven/generate.txt" time="0.000"></testcase>
		<testcase name="TestDataDriven/lint.txt" time="0.000">
			<failure message="Failed" type="">ALSO FAILED :(</failure>
		</testcase>
		<testcase name="TestDataDriven/logic.txt" time="0.000"></testcase>
		<testcase name="TestDataDriven/recording" time="0.000"></testcase>
		<testcase name="TestDataDriven/recording/bench.txt" time="0.000"></testcase>
		<testcase name="TestDataDriven/recording/build.txt" time="0.000"></testcase>
		<testcase name="TestDataDriven/recording/builder.txt" time="0.000"></testcase>
		<testcase name="TestDataDriven/recording/generate.txt" time="0.000"></testcase>
		<testcase name="TestDataDriven/recording/lint.txt" time="0.000"></testcase>
		<testcase name="TestDataDriven/recording/logic.txt" time="0.000"></testcase>
		<testcase name="TestDataDriven/recording/test.txt" time="0.000"></testcase>
		<testcase name="TestDataDriven/test.txt" time="0.000"></testcase>
		<testcase name="TestSetupPath" time="0.000">
			<failure message="Failed" type="">FAILED :(</failure>
		</testcase>
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
