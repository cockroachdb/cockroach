// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar

import (
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDataDriven tests the relevant to_char facilities.
func TestDataDriven(t *testing.T) {
	now := timeutil.Now().UTC()
	ds := pgdate.DateStyle{
		Style: pgdate.Style_ISO,
		Order: pgdate.Order_YMD,
	}
	is := duration.IntervalStyle_POSTGRES
	c := NewFormatCache(1000)
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var ret strings.Builder
			switch d.Cmd {
			case "timestamp":
				tz := parseArgs(t, d.CmdArgs)
				// Input is format, followed by timestamps separated by newlines.
				// Output is the same timestamps in the given format.
				lines := strings.Split(d.Input, "\n")
				for i, in := range lines[1:] {
					if i > 0 {
						ret.WriteString("\n")
					}
					ts, _, err := pgdate.ParseTimestamp(now, ds, in, nil /* h */)
					require.NoError(t, err)
					r, err := TimeToChar(ts.In(tz), c, lines[0])
					if err != nil {
						ret.WriteString(in + ": [ERROR] " + err.Error() + errorDetails(err))
					} else {
						ret.WriteString(in + ": " + r)
					}
				}
			case "timestamp_fmt":
				tz := parseArgs(t, d.CmdArgs)
				// Input is timestamp, followed by formats separated by newlines.
				// Output is the timestamp in the given formats.
				lines := strings.Split(d.Input, "\n")
				ts, _, err := pgdate.ParseTimestamp(now, ds, lines[0], nil /* h */)
				for i, in := range lines[1:] {
					if i > 0 {
						ret.WriteString("\n")
					}
					require.NoError(t, err)
					r, err := TimeToChar(ts.In(tz), c, in)
					if err != nil {
						ret.WriteString(in + ": [ERROR] " + err.Error() + errorDetails(err))
					} else {
						ret.WriteString(in + ": " + r)
					}
				}
			case "interval":
				// Input is format, followed by intervals separated by newlines.
				// Output is the same intervals in the given format.
				lines := strings.Split(d.Input, "\n")
				for i, in := range lines[1:] {
					if i > 0 {
						ret.WriteString("\n")
					}
					ivl, err := duration.ParseInterval(is, in, types.IntervalTypeMetadata{})
					require.NoError(t, err)
					r, err := DurationToChar(ivl, c, lines[0])
					if err != nil {
						ret.WriteString(in + ": [ERROR] " + err.Error() + errorDetails(err))
					} else {
						ret.WriteString(in + ": " + r)
					}
				}
			case "interval_fmt":
				// Input is interval, followed by formats separated by newlines.
				// Output is the interval in the given formats.
				lines := strings.Split(d.Input, "\n")
				ivl, err := duration.ParseInterval(is, lines[0], types.IntervalTypeMetadata{})
				require.NoError(t, err)
				for i, in := range lines[1:] {
					if i > 0 {
						ret.WriteString("\n")
					}
					require.NoError(t, err)
					r, err := DurationToChar(ivl, c, in)
					if err != nil {
						ret.WriteString(in + ": [ERROR] " + err.Error() + errorDetails(err))
					} else {
						ret.WriteString(in + ": " + r)
					}
				}
			default:
				t.Errorf("unknown command %s", d.Cmd)
			}
			return ret.String()
		})
	})
}

func errorDetails(err error) string {
	var sb strings.Builder
	for _, detail := range errors.GetAllDetails(err) {
		sb.WriteString("\n  DETAIL: ")
		sb.WriteString(detail)
	}
	for _, hint := range errors.GetAllHints(err) {
		sb.WriteString("\n  HINT: ")
		sb.WriteString(hint)
	}
	return sb.String()
}

// TestAllTimestamps does a basic sanity check that all time types are supported
// and do not panic.
func TestAllTimestamps(t *testing.T) {
	n := timeutil.Now()
	for _, kw := range dchKeywords {
		t.Run(kw.name, func(t *testing.T) {
			_, err := TimeToChar(n, nil, kw.name)
			if err != nil {
				require.True(t, pgerror.HasCandidateCode(err))
			}
		})
	}
}

// TestAllIntervals does a basic sanity check that all time types are supported
// and do not panic.
func TestAllIntervals(t *testing.T) {
	d := duration.MakeDuration(0, 10, 15)
	for _, kw := range dchKeywords {
		t.Run(kw.name, func(t *testing.T) {
			_, err := DurationToChar(d, nil, kw.name)
			if err != nil {
				require.True(t, pgerror.HasCandidateCode(err))
			}
		})
	}
}

func BenchmarkTimeToChar(b *testing.B) {
	c := NewFormatCache(1)
	t := time.Date(2020, 12, 22, 11, 13, 15, 123456, time.UTC)
	for i := 0; i < b.N; i++ {
		_, _ = TimeToChar(t, c, "YYYY-MM-DD HH24:MI:SS.US")
	}
}

func BenchmarkIntervalToChar(b *testing.B) {
	c := NewFormatCache(1)
	iv := duration.MakeDuration(int64(12*time.Hour+30*time.Minute+13*time.Second), 20, 30)
	for i := 0; i < b.N; i++ {
		_, _ = DurationToChar(iv, c, `YYYY"years" MM"months" DD"days" HH24:MI:SS.US`)
	}
}

func parseArgs(t *testing.T, cmdArgs []datadriven.CmdArg) *time.Location {
	tz := time.UTC
	for _, arg := range cmdArgs {
		switch arg.Key {
		case "tz":
			var err error
			tz, err = timeutil.LoadLocation(arg.Vals[0])
			require.NoError(t, err)
		default:
			t.Errorf("unknown arg %s", arg.Key)
		}
	}
	return tz
}
