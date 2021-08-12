// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOutputOfBinaryRule(t *testing.T) {
	require.Equal(t, OutputOfBinaryRule("//pkg/cmd/cockroach-short"), "pkg/cmd/cockroach-short/cockroach-short_/cockroach-short")
	require.Equal(t, OutputOfBinaryRule("//pkg/cmd/cockroach-short:cockroach-short"), "pkg/cmd/cockroach-short/cockroach-short_/cockroach-short")
	require.Equal(t, OutputOfBinaryRule("pkg/cmd/cockroach-short"), "pkg/cmd/cockroach-short/cockroach-short_/cockroach-short")

	require.Equal(t, OutputOfBinaryRule("@com_github_cockroachdb_stress//:stress"), "external/com_github_cockroachdb_stress/stress_/stress")
}

func TestOutputsOfGenrule(t *testing.T) {
	xmlQueryOutput := `<?xml version="1.1" encoding="UTF-8" standalone="no"?>
<query version="2">
    <rule class="genrule" location="/Users/ricky/go/src/github.com/cockroachdb/cockroach/docs/generated/sql/BUILD.bazel:1:8" name="//docs/generated/sql:sql">
        <string name="name" value="sql"/>
        <list name="exec_tools">
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
