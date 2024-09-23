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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

const (
	ProcessingState = "processing-other"
	FlushingState   = "processing-flushing"
	ReceiveState    = "receiving"
)

type DebugLogicalConsumerStats struct {
	Recv struct {
		CurReceiveStart               time.Time
		LastWaitNanos, TotalWaitNanos int64
	}

	Ingest struct {
		// TotalIngestNanos is the total time spent not waiting for data.
		CurIngestStart   time.Time
		TotalIngestNanos int64
	}

	Flushes struct {
		Count, Nanos, KVs, Bytes, Batches int64

		Last struct {
			CurFlushStart                                                      time.Time
			LastFlushNanos, ProcessedKVs, TotalKVs, Batches, SlowestBatchNanos int64
			// TODO(dt):  BatchErrors atomic.Int64
			// TODO(dt): LastBatchErr atomic.Value
		}
	}
	Checkpoints struct {
		Count                    int64
		LastCheckpoint, Resolved time.Time
	}

	CurrentState string

	Purgatory struct {
		CurrentCount int64
	}
}

func (d *DebugLogicalConsumerStatus) GetStats() DebugLogicalConsumerStats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.mu.stats
}
func (d *DebugLogicalConsumerStatus) RecordRecvStart() {
	d.mu.Lock()
	d.mu.stats.Recv.CurReceiveStart = timeutil.Now() // nolint:deferunlockcheck
	d.mu.stats.CurrentState = ReceiveState
	if !d.mu.stats.Ingest.CurIngestStart.IsZero() { // nolint:deferunlockcheck
		d.mu.stats.Ingest.TotalIngestNanos += timeutil.Since(d.mu.stats.Ingest.CurIngestStart).Nanoseconds() // nolint:deferunlockcheck
	} // nolint:deferunlockcheck
	d.mu.stats.Recv.LastWaitNanos = 0
	d.mu.stats.Ingest.CurIngestStart = time.Time{}
	d.mu.Unlock() // nolint:deferunlockcheck
}

func (d *DebugLogicalConsumerStatus) RecordRecv() {
	d.mu.Lock()
	nanos := timeutil.Since(d.mu.stats.Recv.CurReceiveStart).Nanoseconds() // nolint:deferunlockcheck
	d.mu.stats.CurrentState = ProcessingState
	d.mu.stats.Recv.LastWaitNanos = nanos
	d.mu.stats.Recv.TotalWaitNanos += nanos

	d.mu.stats.Ingest.CurIngestStart = timeutil.Now() // nolint:deferunlockcheck
	d.mu.stats.Recv.CurReceiveStart = time.Time{}
	d.mu.Unlock() // nolint:deferunlockcheck
}

func (d *DebugLogicalConsumerStatus) SetInjectedFailurePercent(percent uint32) {
	d.mu.Lock()
	d.mu.injectFailurePercent = percent
	d.mu.Unlock() // nolint:deferunlockcheck
}

func (d *DebugLogicalConsumerStatus) RecordFlushStart(start time.Time, keyCount int64) uint32 {
	d.mu.Lock()
	// Reset the incremental flush stats.
	d.mu.stats.Flushes.Last.SlowestBatchNanos = 0
	d.mu.stats.Flushes.Last.Batches = 0
	d.mu.stats.Flushes.Last.ProcessedKVs = 0
	d.mu.stats.Flushes.Last.LastFlushNanos = 0

	d.mu.stats.CurrentState = FlushingState
	d.mu.stats.Flushes.Last.CurFlushStart = start
	d.mu.stats.Flushes.Last.TotalKVs = keyCount
	failPercent := d.mu.injectFailurePercent
	d.mu.Unlock() // nolint:deferunlockcheck
	return failPercent
}

func (d *DebugLogicalConsumerStatus) RecordBatchApplied(t time.Duration, keyCount int64) {
	nanos := t.Nanoseconds()
	d.mu.Lock()
	d.mu.stats.Flushes.Last.Batches++
	d.mu.stats.Flushes.Last.ProcessedKVs += keyCount
	if d.mu.stats.Flushes.Last.SlowestBatchNanos < nanos { // nolint:deferunlockcheck
		d.mu.stats.Flushes.Last.SlowestBatchNanos = nanos
	}
	d.mu.Unlock() // nolint:deferunlockcheck
}

func (d *DebugLogicalConsumerStatus) RecordFlushComplete(totalNanos, keyCount, byteSize int64) {
	d.mu.Lock()
	d.mu.stats.CurrentState = ProcessingState
	d.mu.stats.Flushes.Count++
	d.mu.stats.Flushes.Nanos += totalNanos
	d.mu.stats.Flushes.KVs += keyCount
	d.mu.stats.Flushes.Bytes += byteSize
	d.mu.stats.Flushes.Batches += d.mu.stats.Flushes.Last.Batches
	d.mu.stats.Flushes.Last.LastFlushNanos = totalNanos // nolint:deferunlockcheck
	d.mu.stats.Flushes.Last.CurFlushStart = time.Time{}
	d.mu.Unlock() // nolint:deferunlockcheck
}

func (d *DebugLogicalConsumerStatus) RecordCheckpoint(resolved time.Time) {
	d.mu.Lock()
	d.mu.stats.Checkpoints.Count++
	d.mu.stats.Checkpoints.Resolved = resolved
	d.mu.stats.Checkpoints.LastCheckpoint = timeutil.Now() // nolint:deferunlockcheck
	d.mu.Unlock()
}

func (d *DebugLogicalConsumerStatus) RecordPurgatory(netEvents int64) {
	d.mu.Lock()
	d.mu.stats.Purgatory.CurrentCount += netEvents
	d.mu.Unlock()
}
