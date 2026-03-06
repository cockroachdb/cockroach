// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteTextReport(t *testing.T) {
	generated := time.Date(2026, 3, 5, 12, 34, 56, 789000000, time.UTC)
	lookback := 60 * time.Second

	t.Run("with entries", func(t *testing.T) {
		entries := []AggregatedASH{
			{WorkEventType: WorkCPU, WorkEvent: "ReplicaSend", WorkloadID: "00000000000002a", Count: 523},
			{WorkEventType: WorkLock, WorkEvent: "LockWait", WorkloadID: "000000000000001a", Count: 312},
			{WorkEventType: WorkIO, WorkEvent: "StorageEval", WorkloadID: "0000000000000003", Count: 201},
		}

		var buf bytes.Buffer
		err := WriteTextReport(&buf, entries, generated, lookback)
		require.NoError(t, err)

		output := buf.String()
		require.Contains(t, output, "ASH Aggregated Report")
		require.Contains(t, output, "Generated: 2026-03-05T12:34:56.789Z")
		require.Contains(t, output, "Lookback:  1m0s")
		require.Contains(t, output, "Samples:   1036")
		require.Contains(t, output, "COUNT")
		require.Contains(t, output, "WORK_EVENT_TYPE")
		require.Contains(t, output, "523")
		require.Contains(t, output, "ReplicaSend")
		require.Contains(t, output, "312")
		require.Contains(t, output, "LockWait")
		require.Contains(t, output, "201")
		require.Contains(t, output, "StorageEval")
	})

	t.Run("empty entries", func(t *testing.T) {
		var buf bytes.Buffer
		err := WriteTextReport(&buf, nil, generated, lookback)
		require.NoError(t, err)

		output := buf.String()
		require.Contains(t, output, "ASH Aggregated Report")
		require.Contains(t, output, "Samples:   0")
		require.Contains(t, output, "No samples in window.")
	})
}

func TestWriteJSONReport(t *testing.T) {
	generated := time.Date(2026, 3, 5, 12, 34, 56, 789000000, time.UTC)
	lookback := 60 * time.Second

	t.Run("with entries", func(t *testing.T) {
		entries := []AggregatedASH{
			{WorkEventType: WorkCPU, WorkEvent: "ReplicaSend", WorkloadID: "aaa", Count: 100},
			{WorkEventType: WorkIO, WorkEvent: "StorageEval", WorkloadID: "bbb", Count: 50},
		}

		var buf bytes.Buffer
		err := WriteJSONReport(&buf, entries, generated, lookback)
		require.NoError(t, err)

		// Verify it's valid JSON.
		var report jsonReport
		err = json.NewDecoder(strings.NewReader(buf.String())).Decode(&report)
		require.NoError(t, err)

		require.Equal(t, "2026-03-05T12:34:56.789Z", report.Generated)
		require.Equal(t, float64(60), report.LookbackSecs)
		require.Equal(t, int64(150), report.TotalSamples)
		require.Len(t, report.Groups, 2)

		require.Equal(t, "CPU", report.Groups[0].WorkEventType)
		require.Equal(t, "ReplicaSend", report.Groups[0].WorkEvent)
		require.Equal(t, "aaa", report.Groups[0].WorkloadID)
		require.Equal(t, int64(100), report.Groups[0].Count)

		require.Equal(t, "IO", report.Groups[1].WorkEventType)
		require.Equal(t, "StorageEval", report.Groups[1].WorkEvent)
		require.Equal(t, "bbb", report.Groups[1].WorkloadID)
		require.Equal(t, int64(50), report.Groups[1].Count)
	})

	t.Run("empty entries", func(t *testing.T) {
		var buf bytes.Buffer
		err := WriteJSONReport(&buf, nil, generated, lookback)
		require.NoError(t, err)

		var report jsonReport
		err = json.NewDecoder(strings.NewReader(buf.String())).Decode(&report)
		require.NoError(t, err)

		require.Equal(t, int64(0), report.TotalSamples)
		require.Empty(t, report.Groups)
	})
}
