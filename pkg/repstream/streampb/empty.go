// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	SeqNo    atomic.Uint64

	// Properties.
	Spec  StreamPartitionSpec
	State atomic.Int64

	RF struct {
		Checkpoints, Advances atomic.Int64
		LastAdvanceMicros     atomic.Int64
		ResolvedMicros        atomic.Int64
	}
	Flushes struct {
		Batches, Checkpoints, Bytes             atomic.Int64
		EmitWaitNanos, ProduceWaitNanos         atomic.Int64
		LastProduceWaitNanos, LastEmitWaitNanos atomic.Int64
		LastSize                                atomic.Int64
		Full, Ready, Forced                     atomic.Int64
	}
	LastCheckpoint struct {
		Micros atomic.Int64
		Spans  atomic.Value
	}
	LastPolledMicros atomic.Int64
}

type ProducerState int64

const (
	Producing ProducerState = iota
	Emitting
)

func (p ProducerState) String() string {
	switch p {
	case Producing:
		return "read"
	case Emitting:
		return "emit"
	default:
		return "othr"
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

type LogicalConsumerState int

const (
	Other LogicalConsumerState = iota
	Flushing
	Waiting
)

func (s LogicalConsumerState) String() string {
	switch s {
	case Other:
		return "other"
	case Flushing:
		return "flush"
	case Waiting:
		return "receive"
	default:
		return "unknown"
	}
}

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
		Last                              struct {
			ProcessedKVs, TotalKVs, Batches   int64
			CurFlushStart                     time.Time
			LastFlushNanos, SlowestBatchNanos int64
			ChunksRunning, ChunksDone         int64
		}
	}
	Checkpoints struct {
		Count                    int64
		LastCheckpoint, Resolved time.Time
	}

	CurrentState LogicalConsumerState

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
	defer d.mu.Unlock()
	d.mu.stats.Recv.CurReceiveStart = timeutil.Now()
	d.mu.stats.CurrentState = Waiting
	if !d.mu.stats.Ingest.CurIngestStart.IsZero() {
		d.mu.stats.Ingest.TotalIngestNanos += timeutil.Since(d.mu.stats.Ingest.CurIngestStart).Nanoseconds()
	}
	d.mu.stats.Recv.LastWaitNanos = 0
	d.mu.stats.Ingest.CurIngestStart = time.Time{}
}

func (d *DebugLogicalConsumerStatus) RecordRecv() {
	d.mu.Lock()
	defer d.mu.Unlock()
	nanos := timeutil.Since(d.mu.stats.Recv.CurReceiveStart).Nanoseconds()
	d.mu.stats.CurrentState = Other
	d.mu.stats.Recv.LastWaitNanos = nanos
	d.mu.stats.Recv.TotalWaitNanos += nanos

	d.mu.stats.Ingest.CurIngestStart = timeutil.Now()
	d.mu.stats.Recv.CurReceiveStart = time.Time{}
}

func (d *DebugLogicalConsumerStatus) SetInjectedFailurePercent(percent uint32) {
	d.mu.Lock()
	d.mu.injectFailurePercent = percent
	d.mu.Unlock() // nolint:deferunlockcheck
}

func (d *DebugLogicalConsumerStatus) RecordFlushStart(start time.Time, keyCount int64) uint32 {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Reset the incremental flush stats.
	d.mu.stats.Flushes.Last.SlowestBatchNanos = 0
	d.mu.stats.Flushes.Last.Batches = 0
	d.mu.stats.Flushes.Last.ProcessedKVs = 0
	d.mu.stats.Flushes.Last.LastFlushNanos = 0

	d.mu.stats.CurrentState = Flushing
	d.mu.stats.Flushes.Last.CurFlushStart = start
	d.mu.stats.Flushes.Last.TotalKVs = keyCount
	d.mu.stats.Flushes.Last.ChunksRunning = 0
	d.mu.stats.Flushes.Last.ChunksDone = 0
	failPercent := d.mu.injectFailurePercent
	return failPercent
}

func (d *DebugLogicalConsumerStatus) RecordChunkStart() {
	atomic.AddInt64(&d.mu.stats.Flushes.Last.ChunksRunning, 1)
}

func (d *DebugLogicalConsumerStatus) RecordChunkComplete() {
	atomic.AddInt64(&d.mu.stats.Flushes.Last.ChunksRunning, -1)
	atomic.AddInt64(&d.mu.stats.Flushes.Last.ChunksDone, 1)
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
	defer d.mu.Unlock()
	d.mu.stats.CurrentState = Other
	d.mu.stats.Flushes.Count++
	d.mu.stats.Flushes.Nanos += totalNanos
	d.mu.stats.Flushes.KVs += keyCount
	d.mu.stats.Flushes.Bytes += byteSize
	d.mu.stats.Flushes.Batches += d.mu.stats.Flushes.Last.Batches
	d.mu.stats.Flushes.Last.LastFlushNanos = totalNanos
	d.mu.stats.Flushes.Last.CurFlushStart = time.Time{}
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
