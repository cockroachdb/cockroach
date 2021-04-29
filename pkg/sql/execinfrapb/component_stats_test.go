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
network rows received: 10
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
network rows sent: 10
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
KV rows read: 10
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
max sql temp disk usage: 0 B`,
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
rows output: 100`,
		},
		{ // 6
			stats: ComponentStats{
				Inputs: []InputStats{{
					NumTuples: optional.MakeUint(100),
					WaitTime:  optional.MakeTimeValue(time.Second),
				}},
			},
			expected: `
input rows: 100
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
			// Verify that making it deterministic again is a no-op.
			c.MakeDeterministic()
			result = strings.Join(c.StatsForQueryPlan(), "\n")
			if result != expected {
				t.Errorf("Expected:\n%s\ngot:\n%s\n", expected, result)
			}
		})
	}
}

func TestComponentStatsUnion(t *testing.T) {
	testCases := []struct {
		a, b     ComponentStats
		expected string
	}{
		{ // 0
			a:        ComponentStats{},
			b:        ComponentStats{},
			expected: "",
		},
		{ // 1
			a: ComponentStats{
				NetRx: NetworkRxStats{
					Latency:             optional.MakeTimeValue(time.Second),
					WaitTime:            optional.MakeTimeValue(time.Second),
					DeserializationTime: optional.MakeTimeValue(time.Second),
					TuplesReceived:      optional.MakeUint(10),
					BytesReceived:       optional.MakeUint(12345),
				},
				NetTx: NetworkTxStats{
					TuplesSent: optional.MakeUint(10),
					BytesSent:  optional.MakeUint(12345),
				},
				KV: KVStats{
					KVTime:     optional.MakeTimeValue(time.Second),
					TuplesRead: optional.MakeUint(10),
					BytesRead:  optional.MakeUint(12345),
				},
				Exec: ExecStats{
					ExecTime:         optional.MakeTimeValue(time.Second),
					MaxAllocatedMem:  optional.MakeUint(1024),
					MaxAllocatedDisk: optional.MakeUint(1024),
				},
				Output: OutputStats{
					NumBatches: optional.MakeUint(10),
					NumTuples:  optional.MakeUint(100),
				},
				Inputs: []InputStats{{
					NumTuples: optional.MakeUint(100),
					WaitTime:  optional.MakeTimeValue(time.Second),
				}},
				// TODO(radu): to test FlowStats, they should be emitted by
				// StatsForQueryPlan..
			},
			b: ComponentStats{},
			expected: `
network latency: 1s
network wait time: 1s
deserialization time: 1s
network rows received: 10
network bytes received: 12 KiB
network rows sent: 10
network bytes sent: 12 KiB
input rows: 100
input stall time: 1s
KV time: 1s
KV rows read: 10
KV bytes read: 12 KiB
execution time: 1s
max memory allocated: 1.0 KiB
max sql temp disk usage: 1.0 KiB
batches output: 10
rows output: 100`,
		},
		{ // 2
			a: ComponentStats{},
			b: ComponentStats{
				NetRx: NetworkRxStats{
					Latency:             optional.MakeTimeValue(time.Second),
					WaitTime:            optional.MakeTimeValue(time.Second),
					DeserializationTime: optional.MakeTimeValue(time.Second),
					TuplesReceived:      optional.MakeUint(10),
					BytesReceived:       optional.MakeUint(12345),
				},
				NetTx: NetworkTxStats{
					TuplesSent: optional.MakeUint(10),
					BytesSent:  optional.MakeUint(12345),
				},
				KV: KVStats{
					KVTime:     optional.MakeTimeValue(time.Second),
					TuplesRead: optional.MakeUint(10),
					BytesRead:  optional.MakeUint(12345),
				},
				Exec: ExecStats{
					ExecTime:         optional.MakeTimeValue(time.Second),
					MaxAllocatedMem:  optional.MakeUint(1024),
					MaxAllocatedDisk: optional.MakeUint(1024),
				},
				Output: OutputStats{
					NumBatches: optional.MakeUint(10),
					NumTuples:  optional.MakeUint(100),
				},
				Inputs: []InputStats{{
					NumTuples: optional.MakeUint(100),
					WaitTime:  optional.MakeTimeValue(time.Second),
				}},
			},
			expected: `
network latency: 1s
network wait time: 1s
deserialization time: 1s
network rows received: 10
network bytes received: 12 KiB
network rows sent: 10
network bytes sent: 12 KiB
input rows: 100
input stall time: 1s
KV time: 1s
KV rows read: 10
KV bytes read: 12 KiB
execution time: 1s
max memory allocated: 1.0 KiB
max sql temp disk usage: 1.0 KiB
batches output: 10
rows output: 100`,
		},
		{ // 3
			a: ComponentStats{
				NetRx: NetworkRxStats{
					Latency:             optional.MakeTimeValue(time.Second),
					DeserializationTime: optional.MakeTimeValue(time.Second),
				},
				NetTx: NetworkTxStats{
					TuplesSent: optional.MakeUint(10),
				},
				KV: KVStats{
					BytesRead: optional.MakeUint(12345),
				},
				Exec: ExecStats{
					MaxAllocatedMem:  optional.MakeUint(1024),
					MaxAllocatedDisk: optional.MakeUint(1024),
				},
				Output: OutputStats{
					NumBatches: optional.MakeUint(10),
					NumTuples:  optional.MakeUint(100),
				},
			},
			b: ComponentStats{
				NetRx: NetworkRxStats{
					Latency:        optional.MakeTimeValue(time.Second * 100),
					WaitTime:       optional.MakeTimeValue(time.Second),
					TuplesReceived: optional.MakeUint(10),
					BytesReceived:  optional.MakeUint(12345),
				},
				NetTx: NetworkTxStats{
					TuplesSent: optional.MakeUint(10),
					BytesSent:  optional.MakeUint(12345),
				},
				KV: KVStats{
					KVTime:     optional.MakeTimeValue(time.Second),
					TuplesRead: optional.MakeUint(10),
					BytesRead:  optional.MakeUint(12345 * 1000),
				},
				Exec: ExecStats{
					ExecTime:        optional.MakeTimeValue(time.Second),
					MaxAllocatedMem: optional.MakeUint(1024 * 1000),
				},
				Output: OutputStats{
					NumBatches: optional.MakeUint(10000),
				},
				Inputs: []InputStats{{
					NumTuples: optional.MakeUint(100),
					WaitTime:  optional.MakeTimeValue(time.Second),
				}},
			},
			expected: `
network latency: 1s
network wait time: 1s
deserialization time: 1s
network rows received: 10
network bytes received: 12 KiB
network rows sent: 10
network bytes sent: 12 KiB
input rows: 100
input stall time: 1s
KV time: 1s
KV rows read: 10
KV bytes read: 12 KiB
execution time: 1s
max memory allocated: 1.0 KiB
max sql temp disk usage: 1.0 KiB
batches output: 10
rows output: 100`,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			c := tc.a.Union(&tc.b)
			result := strings.Join(c.StatsForQueryPlan(), "\n")
			expected := strings.TrimSpace(tc.expected)
			if result != expected {
				t.Errorf("Expected:\n%s\ngot:\n%s\n", expected, result)
			}
		})
	}
}
