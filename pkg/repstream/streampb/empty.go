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

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

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
		Batches, Checkpoints, Bytes, EmitWaitNanos, ProduceWaitNanos atomic.Int64
		LastProduceWaitNanos, LastEmitWaitNanos                      atomic.Int64
	}
	LastCheckpoint struct {
		Micros atomic.Int64
		Spans  atomic.Value
	}
}

// TODO(dt): this really should be per server instead of process-global, i.e. if
// we added a generic map to the job registry -- which is per server -- in which
// every components of a job could register itself while it is running.
var activeProducerStatuses = struct {
	syncutil.Mutex
	m map[*DebugProducerStatus]struct{}
}{m: make(map[*DebugProducerStatus]struct{})}

// RegisterProducerStatus registers a DebugProducerStatus so that it is returned
// by GetActiveProducerStatuses. It *must* be unregistered with
// UnregisterProducerStatus when its processor closes to prevent leaks.
func RegisterProducerStatus(s *DebugProducerStatus) {
	activeProducerStatuses.Lock()
	defer activeProducerStatuses.Unlock()
	activeProducerStatuses.m[s] = struct{}{}
}

// UnregisterProducerStatus unregisters a previously registered
// DebugProducerStatus. It is idempotent.
func UnregisterProducerStatus(s *DebugProducerStatus) {
	activeProducerStatuses.Lock()
	defer activeProducerStatuses.Unlock()
	delete(activeProducerStatuses.m, s)
}

// GetActiveProducerStatuses gets the DebugProducerStatus for all registered
// stream producers in the process.
func GetActiveProducerStatuses() []*DebugProducerStatus {
	activeProducerStatuses.Lock()
	defer activeProducerStatuses.Unlock()
	res := make([]*DebugProducerStatus, 0, len(activeProducerStatuses.m))
	for e := range activeProducerStatuses.m {
		res = append(res, e)
	}
	return res
}

// activeLogicalConsumerStatuses is the debug statuses of all active logical
// writer processors in the process at any given time, across all jobs in all
// shared service tenants.
//
// TODO(dt): this really should be per server instead of process-global, i.e. if
// we added a generic map to the job registry -- which is per server -- in which
// every components of a job could register itself while it is running.
var activeLogicalConsumerStatuses = struct {
	syncutil.Mutex
	m map[*DebugLogicalConsumerStatus]struct{}
}{m: make(map[*DebugLogicalConsumerStatus]struct{})}

// RegisterActiveLogicalConsumerStatus registers a DebugLogicalConsumerStatus so
// that it is returned by GetActiveLogicalConsumerStatuses. It *must* be
// unregistered with UnregisterActiveLogicalConsumerStatus when its processor
// closes to prevent leaks.
func RegisterActiveLogicalConsumerStatus(s *DebugLogicalConsumerStatus) {
	activeLogicalConsumerStatuses.Lock()
	defer activeLogicalConsumerStatuses.Unlock()
	activeLogicalConsumerStatuses.m[s] = struct{}{}
}

// UnregisterActiveLogicalConsumerStatus unregisters a previously registered
// DebugLogicalConsumerStatus. It is idempotent.
func UnregisterActiveLogicalConsumerStatus(s *DebugLogicalConsumerStatus) {
	activeLogicalConsumerStatuses.Lock()
	defer activeLogicalConsumerStatuses.Unlock()
	delete(activeLogicalConsumerStatuses.m, s)
}

// GetActiveLogicalConsumerStatuses gets the DebugLogicalConsumerStatus for all
// registered logical consumer processors in the process.
func GetActiveLogicalConsumerStatuses() []*DebugLogicalConsumerStatus {
	activeLogicalConsumerStatuses.Lock()
	defer activeLogicalConsumerStatuses.Unlock()
	res := make([]*DebugLogicalConsumerStatus, 0, len(activeLogicalConsumerStatuses.m))
	for e := range activeLogicalConsumerStatuses.m {
		res = append(res, e)
	}
	return res
}

// DebugLogicalConsumerStatus captures debug state of a logical stream consumer.
type DebugLogicalConsumerStatus struct {
	// Identification info.
	StreamID    StreamID
	ProcessorID int32

	Recv struct {
		LastWaitNanos, TotalWaitNanos atomic.Int64
	}

	Flushes struct {
		Count, Nanos atomic.Int64
		Last         struct {
			Nanos             atomic.Int64
			KVs, Bytes        atomic.Int64
			SlowestBatchNanos atomic.Int64
			// TODO(dt): Errors     atomic.Int64
		}
		Current struct {
			StartedUnixMicros      atomic.Int64
			ProcessedKVs, TotalKVs atomic.Int64
			Batches                atomic.Int64
			// TODO(dt):  BatchErrors atomic.Int64
			// TODO(dt): LastBatchErr atomic.Value
		}
	}
}
