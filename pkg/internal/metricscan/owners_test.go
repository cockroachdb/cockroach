// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metricscan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadMetricOwners(t *testing.T) {
	t.Run("classification", func(t *testing.T) {
		data := []byte(`owners:
  some_metric: team-a
  ^sql_: team-b
  foo_%s_bar: team-c
`)
		mo, err := LoadMetricOwners(data)
		require.NoError(t, err)

		// Exact entry.
		require.Equal(t, "team-a", mo.exact["some_metric"])

		// Prefix entry (^ is stripped from the key).
		require.Equal(t, "team-b", mo.prefixes["sql_"])

		// Pattern entry is compiled into a regexp.
		require.Len(t, mo.patterns, 1)
		require.Equal(t, "team-c", mo.patterns[0].owner)
		require.True(t, mo.patterns[0].re.MatchString("foo_hello_bar"))
		require.False(t, mo.patterns[0].re.MatchString("foo_bar"))
	})

	t.Run("invalid yaml", func(t *testing.T) {
		_, err := LoadMetricOwners([]byte(`{invalid`))
		require.Error(t, err)
	})
}

func TestResolve(t *testing.T) {
	data := []byte(`owners:
  some_metric: team-a
  ^sql_: team-b
  some_histo: team-c
  foo_bar: team-d
  fmt_%s_metric: team-e
  uptime: team-f
`)
	mo, err := LoadMetricOwners(data)
	require.NoError(t, err)

	tests := []struct {
		name      string
		input     string
		wantOwner string
		wantOK    bool
	}{
		{
			name:      "exact match",
			input:     "some_metric",
			wantOwner: "team-a",
			wantOK:    true,
		},
		{
			name:      "prefix match",
			input:     "sql_query_count",
			wantOwner: "team-b",
			wantOK:    true,
		},
		{
			name:      "histogram suffix _bucket",
			input:     "some_histo_bucket",
			wantOwner: "team-c",
			wantOK:    true,
		},
		{
			name:      "histogram suffix _sum",
			input:     "some_histo_sum",
			wantOwner: "team-c",
			wantOK:    true,
		},
		{
			name:      "histogram suffix _count",
			input:     "some_histo_count",
			wantOwner: "team-c",
			wantOK:    true,
		},
		{
			name:      "progressive suffix stripping",
			input:     "foo_bar_baz",
			wantOwner: "team-d",
			wantOK:    true,
		},
		{
			name:      "pattern match",
			input:     "fmt_hello_metric",
			wantOwner: "team-e",
			wantOK:    true,
		},
		{
			// The _internal suffix check fires when progressive
			// suffix stripping cannot find a candidate because the
			// base name is a single segment (no underscore).
			name:      "_internal suffix stripping",
			input:     "uptime_internal",
			wantOwner: "team-f",
			wantOK:    true,
		},
		{
			name:   "unknown metric",
			input:  "completely_unknown",
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			owner, ok := mo.Resolve(tc.input)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantOwner, owner)
			}
		})
	}
}
