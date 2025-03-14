// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSysbenchToGoBench(t *testing.T) {
	testCases := []struct {
		name           string
		result         string
		benchmarkName  string
		expectedOutput string
		expectError    bool
	}{
		{
			name: "valid",
			result: `SQL statistics:
    queries performed:
        read:                            10711078
        write:                           3060308
        other:                           1530154
        total:                           15301540
    transactions:                        765077 (1275.05 per sec.)
    queries:                             15301540 (25501.03 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

General statistics:
    total time:                          600.0347s
    total number of events:              765077

Latency (ms):
         min:                                   11.86
         avg:                                   50.19
         max:                                  276.58
         95th percentile:                       81.48
         sum:                             38399834.47

Threads fairness:
    events (avg/stddev):           11954.3281/1290.50
    execution time (avg/stddev):   599.9974/0.01`,
			benchmarkName:  "sysbench-settings/oltp_read_write/nodes=3/cpu=8/conc=64",
			expectedOutput: "BenchmarkSysbenchSettings/a=oltp_read_write/nodes=3/cpu=8/conc=64\t1\t25501.03 queries/sec\t1275.05 txns/sec\t11.86 ms/min\t50.19 ms/avg\t81.48 ms/p95\t276.58 ms/max",
			expectError:    false,
		},
		{
			name:           "bad-input",
			result:         `something`,
			benchmarkName:  "sysbench-settings/oltp_read_write/nodes=3/cpu=8/conc=64",
			expectedOutput: "",
			expectError:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := sysbenchToGoBench(tc.benchmarkName, tc.result)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedOutput, output)
			}
		})
	}
}
