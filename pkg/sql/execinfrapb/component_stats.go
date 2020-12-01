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
	"fmt"
	"strings"
	"time"

	optional "github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/dustin/go-humanize"
)

// Stats is part of SpanStats interface.
func (s *ComponentStats) Stats() map[string]string {
	result := make(map[string]string, 4)
	s.formatStats(func(key string, value interface{}) {
		// The key becomes a tracing span tag. Replace spaces with dots and use
		// only lowercase characters.
		key = strings.ToLower(strings.ReplaceAll(key, " ", "."))
		result[key] = fmt.Sprint(value)
	})
	return result
}

// StatsForQueryPlan is part of DistSQLSpanStats interface.
func (s *ComponentStats) StatsForQueryPlan() []string {
	result := make([]string, 0, 4)
	s.formatStats(func(key string, value interface{}) {
		result = append(result, fmt.Sprintf("%s: %v", key, value))
	})
	return result
}

// formatStats calls fn for each statistic that is set.
func (s *ComponentStats) formatStats(fn func(suffix string, value interface{})) {
	// Network Rx stats.
	if s.NetRx.Latency.HasValue() {
		fn("network latency", s.NetRx.Latency.Value().Round(time.Microsecond))
	}
	if s.NetRx.WaitTime.HasValue() {
		fn("network wait time", s.NetRx.WaitTime.Value().Round(time.Microsecond))
	}
	if s.NetRx.DeserializationTime.HasValue() {
		fn("deserialization time", s.NetRx.DeserializationTime.Value().Round(time.Microsecond))
	}
	if s.NetRx.TuplesReceived.HasValue() {
		fn("network tuples received", s.NetRx.TuplesReceived.Value())
	}
	if s.NetRx.BytesReceived.HasValue() {
		fn("network bytes received", humanize.IBytes(s.NetRx.BytesReceived.Value()))
	}

	// Network Tx stats.
	if s.NetTx.TuplesSent.HasValue() {
		fn("network tuples sent", s.NetTx.TuplesSent.Value())
	}
	if s.NetTx.BytesSent.HasValue() {
		fn("network bytes sent", humanize.IBytes(s.NetTx.BytesSent.Value()))
	}

	// Input stats.
	switch len(s.Inputs) {
	case 1:
		if s.Inputs[0].NumTuples.HasValue() {
			fn("input tuples", s.Inputs[0].NumTuples.Value())
		}
		if s.Inputs[0].WaitTime.HasValue() {
			fn("input stall time", s.Inputs[0].WaitTime.Value().Round(time.Microsecond))
		}

	case 2:
		if s.Inputs[0].NumTuples.HasValue() {
			fn("left tuples", s.Inputs[0].NumTuples.Value())
		}
		if s.Inputs[0].WaitTime.HasValue() {
			fn("left stall time", s.Inputs[0].WaitTime.Value().Round(time.Microsecond))
		}
		if s.Inputs[1].NumTuples.HasValue() {
			fn("right tuples", s.Inputs[1].NumTuples.Value())
		}
		if s.Inputs[1].WaitTime.HasValue() {
			fn("right stall time", s.Inputs[1].WaitTime.Value().Round(time.Microsecond))
		}
	}

	// KV stats.
	if s.KV.KVTime.HasValue() {
		fn("KV time", s.KV.KVTime.Value().Round(time.Microsecond))
	}
	if s.KV.ContentionTime.HasValue() {
		// TODO(asubiotto): Round once KV layer produces real contention events.
		fn("KV contention time", s.KV.ContentionTime.Value())
	}
	if s.KV.TuplesRead.HasValue() {
		fn("KV tuples read", s.KV.TuplesRead.Value())
	}
	if s.KV.BytesRead.HasValue() {
		fn("KV bytes read", humanize.IBytes(s.KV.BytesRead.Value()))
	}

	// Exec stats.
	if s.Exec.ExecTime.HasValue() {
		fn("execution time", s.Exec.ExecTime.Value().Round(time.Microsecond))
	}
	if s.Exec.MaxAllocatedMem.HasValue() {
		fn("max memory allocated", humanize.IBytes(s.Exec.MaxAllocatedMem.Value()))
	}
	if s.Exec.MaxAllocatedDisk.HasValue() {
		fn("max scratch disk allocated", humanize.IBytes(s.Exec.MaxAllocatedDisk.Value()))
	}

	// Output stats.
	if s.Output.NumBatches.HasValue() {
		fn("batches output", s.Output.NumBatches.Value())
	}
	if s.Output.NumTuples.HasValue() {
		fn("tuples output", s.Output.NumTuples.Value())
	}
}

// MakeDeterministic is used only for testing; it modifies any non-deterministic
// statistics like elapsed time or exact number of bytes to fixed or
// manufactured values.
//
// Note that it does not modify which fields that are set. In other words, a
// field will have a non-zero protobuf value iff it had a non-zero protobuf
// value before. This allows tests to verify the set of stats that were
// collected.
func (s *ComponentStats) MakeDeterministic() {
	// resetUint resets an optional.Uint to 0, if it was set.
	resetUint := func(v *optional.Uint) {
		if v.HasValue() {
			v.Set(0)
		}
	}
	// timeVal resets a duration to 1ns, if it was set.
	timeVal := func(v *optional.Duration) {
		if v.HasValue() {
			v.Set(0)
		}
	}

	// NetRx.
	timeVal(&s.NetRx.Latency)
	timeVal(&s.NetRx.WaitTime)
	timeVal(&s.NetRx.DeserializationTime)
	if s.NetRx.BytesReceived.HasValue() {
		// BytesReceived is overridden to a useful value for tests.
		s.NetRx.BytesReceived.Set(8 * s.Output.NumTuples.Value())
	}

	// NetTx.
	resetUint(&s.NetTx.BytesSent)

	// KV.
	timeVal(&s.KV.KVTime)
	timeVal(&s.KV.ContentionTime)
	if s.KV.BytesRead.HasValue() {
		// BytesRead is overridden to a useful value for tests.
		s.KV.BytesRead.Set(8 * s.KV.TuplesRead.Value())
	}

	// Exec.
	timeVal(&s.Exec.ExecTime)
	resetUint(&s.Exec.MaxAllocatedMem)
	resetUint(&s.Exec.MaxAllocatedDisk)

	// Output.
	resetUint(&s.Output.NumBatches)

	// Inputs.
	for i := range s.Inputs {
		timeVal(&s.Inputs[i].WaitTime)
	}
}
