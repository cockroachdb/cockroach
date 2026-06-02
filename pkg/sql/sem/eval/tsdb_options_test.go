// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/stretchr/testify/require"
)

func mustJSON(t *testing.T, s string) json.JSON {
	t.Helper()
	j, err := json.ParseJSON(s)
	require.NoError(t, err)
	return j
}

func TestParseAggregator(t *testing.T) {
	cases := []struct {
		name  string
		jsonV string
		want  Aggregator
		err   string // substring; empty = no error
	}{
		{"avg-upper", `"AVG"`, AggregatorAvg, ""},
		{"avg-lower", `"avg"`, AggregatorAvg, ""},
		{"avg-mixed", `"Avg"`, AggregatorAvg, ""},
		{"sum", `"SUM"`, AggregatorSum, ""},
		{"max", `"MAX"`, AggregatorMax, ""},
		{"min", `"MIN"`, AggregatorMin, ""},
		// FIRST/LAST/VARIANCE exist in the enum but the TSDB rejects them.
		{"first-rejected", `"FIRST"`, AggregatorDefault, `invalid downsampler "FIRST"; must be one of AVG, SUM, MAX, MIN`},
		{"last-rejected", `"LAST"`, AggregatorDefault, `invalid downsampler "LAST"; must be one of AVG, SUM, MAX, MIN`},
		{"variance-rejected", `"VARIANCE"`, AggregatorDefault, `invalid downsampler "VARIANCE"; must be one of AVG, SUM, MAX, MIN`},
		{"unknown-string", `"sun"`, AggregatorDefault, `invalid downsampler "sun"`},
		{"number", `5`, AggregatorDefault, `must be a string, got numeric`},
		{"array", `[]`, AggregatorDefault, `must be a string, got array`},
		{"object", `{}`, AggregatorDefault, `must be a string, got object`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got Aggregator
			err := parseAggregator(mustJSON(t, tc.jsonV), "downsampler", &got)
			if tc.err != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseDerivative(t *testing.T) {
	cases := []struct {
		name  string
		jsonV string
		want  Derivative
		err   string
	}{
		{"none", `"NONE"`, DerivativeNone, ""},
		{"none-lower", `"none"`, DerivativeNone, ""},
		{"derivative", `"DERIVATIVE"`, DerivativeFirstOrder, ""},
		{"non-negative", `"NON_NEGATIVE_DERIVATIVE"`, DerivativeNonNegativeFirstOrder, ""},
		{"non-negative-lower", `"non_negative_derivative"`, DerivativeNonNegativeFirstOrder, ""},
		{"unknown", `"rate"`, DerivativeDefault, `invalid derivative "rate"; must be one of NONE, DERIVATIVE, NON_NEGATIVE_DERIVATIVE`},
		{"number", `0`, DerivativeDefault, `must be a string, got numeric`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got Derivative
			err := parseDerivative(mustJSON(t, tc.jsonV), &got)
			if tc.err != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseInterval(t *testing.T) {
	cases := []struct {
		name  string
		jsonV string
		want  int64 // nanoseconds; ignored if err != ""
		err   string
	}{
		{"10s", `"10s"`, int64(10 * 1_000_000_000), ""},
		{"1m", `"1m"`, int64(60 * 1_000_000_000), ""},
		{"1 minute", `"1 minute"`, int64(60 * 1_000_000_000), ""},
		{"30s", `"30s"`, int64(30 * 1_000_000_000), ""},
		{"1h", `"1h"`, int64(3600 * 1_000_000_000), ""},
		{"sub-10s", `"5s"`, 0, "must be a positive multiple of 10s"},
		{"not-multiple", `"15s"`, 0, "must be a positive multiple of 10s"},
		{"zero", `"0s"`, 0, "must be positive"},
		{"negative", `"-10s"`, 0, "must be positive"},
		{"month", `"1 month"`, 0, "must not contain month or year components"},
		{"year", `"1 year"`, 0, "must not contain month or year components"},
		{"non-string", `60`, 0, "must be a string, got numeric"},
		{"unparseable", `"hello"`, 0, "invalid interval"},
		{"1 day", `"1 day"`, int64(24 * 3600 * 1_000_000_000), ""},
		{"1d", `"1d"`, int64(24 * 3600 * 1_000_000_000), ""},
		{"1 week", `"1 week"`, int64(7 * 24 * 3600 * 1_000_000_000), ""},
		{"day plus hour", `"1 day 2 hours"`, int64((24 + 2) * 3600 * 1_000_000_000), ""},
		{"negative day", `"-1 day"`, 0, "must be positive"},
		{"empty-string", `""`, 0, "invalid interval"},
		// 200000 days × 86_400_000_000_000 ns/day wraps int64; without
		// the overflow guard the wrap could survive the positive/multiple
		// checks and produce a corrupt bucket.
		{"days-overflow", `"200000 days"`, 0, "interval is out of range"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got int64
			err := parseInterval(mustJSON(t, tc.jsonV), &got)
			if tc.err != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseSources(t *testing.T) {
	cases := []struct {
		name  string
		jsonV string
		want  []string
		err   string
	}{
		{"empty", `[]`, nil, ""},
		{"single", `["1"]`, []string{"1"}, ""},
		{"multi", `["1","2","3"]`, []string{"1", "2", "3"}, ""},
		{"composite-source", `["1-2"]`, []string{"1-2"}, ""}, // store IDs use node-store format
		{"dedup-adjacent", `["1","1","2"]`, []string{"1", "2"}, ""},
		{"dedup-non-adjacent", `["1","2","1","3","2"]`, []string{"1", "2", "3"}, ""},
		{"dedup-all-same", `["1","1","1"]`, []string{"1"}, ""},
		{"non-array", `"1"`, nil, "must be an array of strings"},
		{"non-string-element", `[1, 2]`, nil, "must contain only strings; element 0 is numeric"},
		{"mixed-element", `["1", 2]`, nil, "must contain only strings; element 1 is numeric"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			err := parseSources(mustJSON(t, tc.jsonV), &got)
			if tc.err != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestTimeSeriesQuery_Validate(t *testing.T) {
	cases := []struct {
		name string
		q    TimeSeriesQuery
		err  string // substring; empty = no error
	}{
		{
			name: "fully-default-with-metric-name",
			q:    TimeSeriesQuery{MetricName: "cr.node.foo"},
		},
		{
			name: "downsampler-and-sample-set",
			q: TimeSeriesQuery{
				MetricName:  "cr.node.foo",
				Downsampler: AggregatorSum,
				SampleNanos: int64(60 * 1_000_000_000),
			},
		},
		{
			name: "empty-metric-name",
			q:    TimeSeriesQuery{},
			err:  "TimeSeriesQuery requires a non-empty MetricName",
		},
		{
			name: "downsampler-without-sample",
			q: TimeSeriesQuery{
				MetricName:  "cr.node.foo",
				Downsampler: AggregatorAvg,
			},
			err: "Downsampler is set but SampleNanos is 0",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.q.Validate()
			if tc.err != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestParseTSDBQueryOptions(t *testing.T) {
	cases := []struct {
		name  string
		input string // empty = nil JSONB
		check func(t *testing.T, q TimeSeriesQuery)
		err   string
	}{
		{"nil-blob", "", func(t *testing.T, q TimeSeriesQuery) {
			require.Equal(t, TimeSeriesQuery{}, q)
		}, ""},
		{"null-blob", "null", func(t *testing.T, q TimeSeriesQuery) {
			require.Equal(t, TimeSeriesQuery{}, q)
		}, ""},
		{"empty-obj", `{}`, func(t *testing.T, q TimeSeriesQuery) {
			require.Equal(t, TimeSeriesQuery{}, q)
		}, ""},
		{"all-five-knobs", `{
			"downsampler":"SUM","interval":"1 minute",
			"derivative":"NON_NEGATIVE_DERIVATIVE",
			"source_aggregator":"MAX","sources":["1","2"]
		}`, func(t *testing.T, q TimeSeriesQuery) {
			require.Equal(t, AggregatorSum, q.Downsampler)
			require.Equal(t, int64(60*1_000_000_000), q.SampleNanos)
			require.Equal(t, DerivativeNonNegativeFirstOrder, q.Derivative)
			require.Equal(t, AggregatorMax, q.SourceAggregator)
			require.Equal(t, []string{"1", "2"}, q.Sources)
		}, ""},
		{"null-field-keeps-default", `{"downsampler":null,"interval":"1m"}`, func(t *testing.T, q TimeSeriesQuery) {
			require.Equal(t, AggregatorDefault, q.Downsampler)
			require.Equal(t, int64(60*1_000_000_000), q.SampleNanos)
		}, ""},
		{"unknown-field", `{"downsamplr":"SUM"}`, nil,
			`unknown option "downsamplr"; accepted options: downsampler, interval, derivative, source_aggregator, sources`},
		{"non-object", `5`, nil, "must be a JSON object, got numeric"},
		{"array", `[]`, nil, "must be a JSON object, got array"},
		{"source-aggregator-first", `{"source_aggregator":"FIRST"}`, nil,
			`invalid source_aggregator "FIRST"; must be one of AVG, SUM, MAX, MIN`},
		{"source-aggregator-last", `{"source_aggregator":"LAST"}`, nil,
			`invalid source_aggregator "LAST"; must be one of AVG, SUM, MAX, MIN`},
		{"source-aggregator-variance", `{"source_aggregator":"VARIANCE"}`, nil,
			`invalid source_aggregator "VARIANCE"; must be one of AVG, SUM, MAX, MIN`},
		{"downsampler-variance", `{"downsampler":"VARIANCE","interval":"1m"}`, nil,
			`invalid downsampler "VARIANCE"; must be one of AVG, SUM, MAX, MIN`},
		{"downsampler-without-interval", `{"downsampler":"SUM"}`, nil,
			`"downsampler" requires "interval" to be set`},
		{"avg-explicit-without-interval-also-rejected", `{"downsampler":"AVG"}`, nil,
			`"downsampler" requires "interval" to be set`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var jBlob json.JSON
			if tc.input != "" {
				jBlob = mustJSON(t, tc.input)
			}
			var q TimeSeriesQuery
			err := ParseTSDBQueryOptions(jBlob, &q)
			if tc.err != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
				return
			}
			require.NoError(t, err)
			tc.check(t, q)
		})
	}
}
