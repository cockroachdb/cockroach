// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfrapb

import (
	fmt "fmt"
	"strings"
	"testing"
	time "time"

	"github.com/cockroachdb/cockroach/pkg/util/optional"
)

func TestComponentStatsMakeDeterminstic(t *testing.T) {
	testCases := []struct {
		stats    ComponentStats
		expected string
	}{
		{ // 0
			stats:    ComponentStats{},
			expected: "",
		},
		{ // 1
			stats: ComponentStats{
				NetRx: NetworkRxStats{
					Latency:             optional.MakeTimeValue(time.Second),
					WaitTime:            optional.MakeTimeValue(time.Second),
					DeserializationTime: optional.MakeTimeValue(time.Second),
					TuplesReceived:      optional.MakeUint(10),
					BytesReceived:       optional.MakeUint(12345),
				},
			},
			expected: `
network latency: 0µs
network wait time: 0µs
deserialization time: 0µs
network tuples received: 10
network bytes received: 80 B`,
		},
		{ // 2
			stats: ComponentStats{
				NetTx: NetworkTxStats{
					TuplesSent: optional.MakeUint(10),
					BytesSent:  optional.MakeUint(12345),
				},
			},
			expected: `
network tuples sent: 10
network bytes sent: 80 B`,
		},
		{ // 3
			stats: ComponentStats{
				KV: KVStats{
					KVTime:     optional.MakeTimeValue(time.Second),
					TuplesRead: optional.MakeUint(10),
					BytesRead:  optional.MakeUint(12345),
				},
			},
			expected: `
KV time: 0µs
KV tuples read: 10
KV bytes read: 80 B`,
		},
		{ // 4
			stats: ComponentStats{
				Exec: ExecStats{
					ExecTime:         optional.MakeTimeValue(time.Second),
					MaxAllocatedMem:  optional.MakeUint(1024),
					MaxAllocatedDisk: optional.MakeUint(1024),
				},
			},
			expected: `
execution time: 0µs
max memory allocated: 0 B
max scratch disk allocated: 0 B`,
		},
		{ // 5
			stats: ComponentStats{
				Output: OutputStats{
					NumBatches: optional.MakeUint(10),
					NumTuples:  optional.MakeUint(100),
				},
			},
			expected: `
batches output: 0
tuples output: 100`,
		},
		{ // 6
			stats: ComponentStats{
				Inputs: []InputStats{{
					NumTuples: optional.MakeUint(100),
					WaitTime:  optional.MakeTimeValue(time.Second),
				}},
			},
			expected: `
input tuples: 100
input stall time: 0µs`,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			c := tc.stats
			c.MakeDeterministic()
			result := strings.Join(c.StatsForQueryPlan(), "\n")
			expected := strings.TrimSpace(tc.expected)
			if result != expected {
				t.Errorf("Expected:\n%s\ngot:\n%s\n", expected, result)
			}
		})
	}
}
