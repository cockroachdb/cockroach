// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tochar

import (
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDataDriven tests the relevant to_char facilities.
func TestDataDriven(t *testing.T) {
	now := time.Now().UTC()
	ds := pgdate.DateStyle{
		Style: pgdate.Style_ISO,
		Order: pgdate.Order_YMD,
	}
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
					ts, _, err := pgdate.ParseTimestamp(now, ds, in)
					require.NoError(t, err)
					r, err := TimeToChar(ts.In(tz), lines[0])
					if err != nil {
						ret.WriteString(in + ": [ERROR] " + err.Error() + strings.Join(append(errors.GetAllDetails(err), errors.GetAllHints(err)...), " ... "))
					} else {
						ret.WriteString(in + ": " + r)
					}
				}
			case "timestamp_fmt":
				tz := parseArgs(t, d.CmdArgs)
				// Input is timestamp, followed by formats separated by newlines.
				// Output is the timestamp in the given formats.
				lines := strings.Split(d.Input, "\n")
				ts, _, err := pgdate.ParseTimestamp(now, ds, lines[0])
				for i, in := range lines[1:] {
					if i > 0 {
						ret.WriteString("\n")
					}
					require.NoError(t, err)
					r, err := TimeToChar(ts.In(tz), in)
					if err != nil {
						ret.WriteString(in + ": [ERROR] " + err.Error() + strings.Join(append(errors.GetAllDetails(err), errors.GetAllHints(err)...), " ... "))
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

// TestAllTimestamps does a basic sanity check that all time types are supported
// and do not panic.
func TestAllTimestamps(t *testing.T) {
	n := time.Now()
	for _, kw := range dchKeywords {
		t.Run(kw.name, func(t *testing.T) {
			_, err := TimeToChar(n, kw.name)
			if err != nil {
				require.True(t, pgerror.HasCandidateCode(err))
			}
		})
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
