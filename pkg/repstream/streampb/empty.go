// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streampb

import "sync/atomic"

type DebugProducerStatus struct {
	// Identification info.
	StreamID StreamID
	// Properties.
	Spec StreamPartitionSpec

	RF struct {
		Checkpoints, Advances atomic.Int64
		LastAdvanceMicros     atomic.Int64
		ResolvedMicros        atomic.Int64
	}
	Flushes struct {
		Batches, Checkpoints atomic.Int64
	}
	LastCheckpoint struct {
		Micros atomic.Int64
		Spans  atomic.Value
	}
}
