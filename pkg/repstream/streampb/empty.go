// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
