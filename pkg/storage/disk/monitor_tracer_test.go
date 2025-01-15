// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package disk

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func parseStats(fields []string) (Stats, error) {
	stats := Stats{DeviceName: fields[0]}
	var err error
	if stats.ReadsCount, err = strconv.Atoi(fields[1]); err != nil {
		return Stats{}, err
	}
	if stats.ReadsMerged, err = strconv.Atoi(fields[2]); err != nil {
		return Stats{}, err
	}
	if stats.ReadsSectors, err = strconv.Atoi(fields[3]); err != nil {
		return Stats{}, err
	}
	if stats.ReadsDuration, err = time.ParseDuration(fields[4]); err != nil {
		return Stats{}, err
	}
	if stats.WritesCount, err = strconv.Atoi(fields[5]); err != nil {
		return Stats{}, err
	}
	if stats.WritesMerged, err = strconv.Atoi(fields[6]); err != nil {
		return Stats{}, err
	}
	if stats.WritesSectors, err = strconv.Atoi(fields[7]); err != nil {
		return Stats{}, err
	}
	if stats.WritesDuration, err = time.ParseDuration(fields[8]); err != nil {
		return Stats{}, err
	}
	if stats.InProgressCount, err = strconv.Atoi(fields[9]); err != nil {
		return Stats{}, err
	}
	if stats.CumulativeDuration, err = time.ParseDuration(fields[10]); err != nil {
		return Stats{}, err
	}
	if stats.WeightedIODuration, err = time.ParseDuration(fields[11]); err != nil {
		return Stats{}, err
	}
	return stats, nil
}

func TestMonitorTracer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var buf bytes.Buffer
	var tracer *monitorTracer
	datadriven.RunTest(t, "testdata/tracer", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "record":
			var capacity int
			td.ScanArgs(t, "capacity", &capacity)
			tracer = newMonitorTracer(capacity)

			if !strings.Contains(td.Input, "\n") {
				return ""
			}
			for _, line := range strings.Split(td.Input, "\n") {
				fields := strings.Fields(line)
				require.Equal(t, 13, len(fields))
				eventTime, err := time.Parse(time.RFC3339, fields[0])
				require.NoError(t, err)
				eventStats, err := parseStats(fields[1:13])
				require.NoError(t, err)
				event := traceEvent{
					time:  eventTime,
					stats: eventStats,
					err:   nil,
				}
				tracer.RecordEvent(event)
			}
			return ""
		case "latest":
			event := tracer.Latest()
			buf.Reset()
			fmt.Fprintf(&buf, "%q", event)
			return buf.String()
		case "trace":
			buf.Reset()
			fmt.Fprint(&buf, tracer)
			return buf.String()
		case "rolling-window":
			var timeString string
			td.ScanArgs(t, "time", &timeString)
			lowerBoundTime, err := time.Parse(time.RFC3339, timeString)
			require.NoError(t, err)

			events := tracer.RollingWindow(lowerBoundTime)
			buf.Reset()
			for _, event := range events {
				fmt.Fprintf(&buf, "%q\n", event)
			}
			return buf.String()
		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
}
