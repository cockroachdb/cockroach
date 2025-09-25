// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCmdLogFileName(t *testing.T) {
	ts := time.Date(2000, 1, 1, 15, 4, 12, 0, time.Local)

	const exp = `run_150412.000000000_n1,3-4,9_cockroach-bla-foo-ba`
	nodes := option.NodeListOption{1, 3, 4, 9}
	assert.Equal(t,
		exp,
		cmdLogFileName(ts, nodes, "./cockroach", "bla", "--foo", "bar"),
	)
	assert.Equal(t,
		exp,
		cmdLogFileName(ts, nodes, "./cockroach bla --foo bar"),
	)
}

func TestToMarkdownTable(t *testing.T) {
	tests := []struct {
		name          string
		input         [][]string
		expectedOut   string
		shouldSucceed bool
	}{
		{
			name: "valid table",
			input: [][]string{
				{"Name", "Age", "City"},
				{"Alice", "30", "New York"},
				{"Bob", "25", "San Francisco"},
			},
			expectedOut: `| Name | Age | City |
| --- | --- | --- |
| Alice | 30 | New York |
| Bob | 25 | San Francisco |
`,
			shouldSucceed: true,
		},
		{
			name:          "empty data",
			input:         [][]string{},
			expectedOut:   "",
			shouldSucceed: false,
		},
		{
			name: "row with wrong number of columns",
			input: [][]string{
				{"Name", "Age", "City"},
				{"Alice", "30"},
			},
			expectedOut:   "",
			shouldSucceed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := ToMarkdownTable(tt.input)
			if err != nil {
				if tt.shouldSucceed {
					t.Errorf("ToMarkdownTable() failed when expected to succeed: error = %v", err)
					return
				}
			}
			if tt.shouldSucceed {
				require.Equal(t, tt.expectedOut, out)
			}
		})
	}
}
