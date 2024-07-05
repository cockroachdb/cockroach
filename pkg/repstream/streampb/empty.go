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
	time "time"

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
	mu          struct {
		syncutil.Mutex
		stats                DebugLogicalConsumerStats
		injectFailurePercent uint32
	}
}

type DebugLogicalConsumerStats struct {
	Recv struct {
		LastWaitNanos, TotalWaitNanos int64
	}

	Flushes struct {
		Count, Nanos, KVs, Bytes, Batches int64

		Current struct {
			StartedUnixMicros, ProcessedKVs, TotalKVs, Batches, SlowestBatchNanos int64
			// TODO(dt):  BatchErrors atomic.Int64
			// TODO(dt): LastBatchErr atomic.Value
		}
		Last struct {
			Nanos, KVs, Bytes, Batches, SlowestBatchNanos int64
			// TODO(dt): Errors     atomic.Int64
		}
	}
}

func (d *DebugLogicalConsumerStatus) GetStats() DebugLogicalConsumerStats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.mu.stats
}

func (d *DebugLogicalConsumerStatus) RecordRecv(wait time.Duration) {
	nanos := wait.Nanoseconds()
	d.mu.Lock()
	d.mu.stats.Recv.LastWaitNanos = nanos
	d.mu.stats.Recv.TotalWaitNanos += nanos
	d.mu.Unlock()
}

func (d *DebugLogicalConsumerStatus) SetInjectedFailurePercent(percent uint32) {
	d.mu.Lock()
	d.mu.injectFailurePercent = percent
	d.mu.Unlock()
}

func (d *DebugLogicalConsumerStatus) RecordFlushStart(start time.Time, keyCount int64) uint32 {
	micros := start.UnixMicro()
	d.mu.Lock()
	d.mu.stats.Flushes.Current.TotalKVs = keyCount
	d.mu.stats.Flushes.Current.StartedUnixMicros = micros
	failPercent := d.mu.injectFailurePercent
	d.mu.Unlock()
	return failPercent
}

func (d *DebugLogicalConsumerStatus) RecordBatchApplied(t time.Duration, keyCount int64) {
	nanos := t.Nanoseconds()
	d.mu.Lock()
	d.mu.stats.Flushes.Current.Batches++
	d.mu.stats.Flushes.Current.ProcessedKVs += keyCount
	if d.mu.stats.Flushes.Current.SlowestBatchNanos < nanos { // nolint:deferunlockcheck
		d.mu.stats.Flushes.Current.SlowestBatchNanos = nanos
	}
	d.mu.Unlock() // nolint:deferunlockcheck
}

func (d *DebugLogicalConsumerStatus) RecordFlushComplete(totalNanos, keyCount, byteSize int64) {
	d.mu.Lock()

	d.mu.stats.Flushes.Count++
	d.mu.stats.Flushes.Nanos += totalNanos
	d.mu.stats.Flushes.KVs += keyCount
	d.mu.stats.Flushes.Bytes += byteSize
	d.mu.stats.Flushes.Batches += d.mu.stats.Flushes.Current.Batches

	d.mu.stats.Flushes.Last.Nanos = totalNanos
	d.mu.stats.Flushes.Last.Batches = d.mu.stats.Flushes.Current.Batches
	d.mu.stats.Flushes.Last.SlowestBatchNanos = d.mu.stats.Flushes.Current.SlowestBatchNanos
	d.mu.stats.Flushes.Last.KVs = keyCount
	d.mu.stats.Flushes.Last.Bytes = byteSize

	d.mu.stats.Flushes.Current.StartedUnixMicros = 0
	d.mu.stats.Flushes.Current.SlowestBatchNanos = 0
	d.mu.stats.Flushes.Current.Batches = 0
	d.mu.stats.Flushes.Current.TotalKVs = 0
	d.mu.stats.Flushes.Current.ProcessedKVs = 0

	d.mu.Unlock() // nolint:deferunlockcheck
}
