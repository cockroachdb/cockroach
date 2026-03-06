// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
)

func TestReport(t *testing.T) {
	datadriven.RunTest(t, "testdata/report", func(t *testing.T, d *datadriven.TestData) string {
		generated, lookback := parseReportArgs(t, d)
		entries := parseEntries(t, d.Input)

		var buf bytes.Buffer
		var err error
		switch d.Cmd {
		case "text-report":
			err = WriteTextReport(&buf, entries, generated, lookback)
		case "json-report":
			err = WriteJSONReport(&buf, entries, generated, lookback)
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}

		if err != nil {
			return fmt.Sprintf("error: %v", err)
		}

		// Replace blank lines with "." for datadriven compatibility.
		lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
		for i, line := range lines {
			if line == "" {
				lines[i] = "."
			}
		}
		return strings.Join(lines, "\n")
	})
}

func parseReportArgs(t *testing.T, d *datadriven.TestData) (time.Time, time.Duration) {
	t.Helper()
	var generated time.Time
	var lookback time.Duration
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "generated":
			var err error
			generated, err = time.Parse(time.RFC3339Nano, arg.Vals[0])
			if err != nil {
				t.Fatalf("parsing generated: %v", err)
			}
		case "lookback":
			var err error
			lookback, err = time.ParseDuration(arg.Vals[0])
			if err != nil {
				t.Fatalf("parsing lookback: %v", err)
			}
		}
	}
	return generated, lookback
}

func parseEntries(t *testing.T, input string) []AggregatedASH {
	t.Helper()
	if strings.TrimSpace(input) == "" {
		return nil
	}
	var entries []AggregatedASH
	for _, line := range strings.Split(input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 4 {
			t.Fatalf("expected 4 fields (TYPE EVENT WORKLOAD_ID COUNT), got %d: %q", len(fields), line)
		}
		count, err := strconv.ParseInt(fields[3], 10, 64)
		if err != nil {
			t.Fatalf("parsing count %q: %v", fields[3], err)
		}
		entries = append(entries, AggregatedASH{
			WorkEventType: parseWorkEventType(t, fields[0]),
			WorkEvent:     fields[1],
			WorkloadID:    fields[2],
			Count:         count,
		})
	}
	return entries
}

func parseWorkEventType(t *testing.T, s string) WorkEventType {
	t.Helper()
	switch s {
	case "CPU":
		return WorkCPU
	case "ADMISSION":
		return WorkAdmission
	case "IO":
		return WorkIO
	case "NETWORK":
		return WorkNetwork
	case "LOCK":
		return WorkLock
	case "OTHER":
		return WorkOther
	case "UNKNOWN":
		return WorkUnknown
	default:
		t.Fatalf("unknown WorkEventType: %s", s)
		return WorkUnknown
	}
}
