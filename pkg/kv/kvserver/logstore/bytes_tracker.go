// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// raftEntriesMemoryLimit is the "global" soft limit for the total size of raft
// log entries pulled into memory simultaneously. Currently, this only includes
// the entries pulled as part of the local state machine application flow.
//
// No limit if <= 0.
var raftEntriesMemoryLimit = envutil.EnvOrDefaultBytes(
	"COCKROACH_RAFT_ENTRIES_MEMORY_LIMIT", 0)

// NewRaftEntriesSoftLimit returns the SoftLimit configured with the default
// memory limit.
func NewRaftEntriesSoftLimit() *SoftLimit {
	reservedBytesMetric := metric.NewGauge(metric.Metadata{
		Name:        "raft.loaded_entries.reserved.bytes",
		Help:        "Bytes allocated by raft Storage.Entries calls that are still kept in memory",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	})
	return &SoftLimit{Metric: reservedBytesMetric, Limit: raftEntriesMemoryLimit}
}

// The reservation sizes are optimized with the following constraints in mind:
//
//   - Per kvserver.defaultRaftSchedulerConcurrency, there are <= 128 raft
//     scheduler workers.
//   - Per bulk.IngestBatchSize and kvserverbase.MaxCommandSize, raft entries are
//     up to 16 MB in size. Entries are typically small, but can reach this
//     limit, e.g. if these are AddSSTable commands or large batch INSERTs.
//     Typically, compression reduces the max size to < 8MB.
const (
	// minReserveSize is the granularity at which bytes are reserved from the
	// SoftLimit for small allocations (below smallReserveSize). Batching small
	// allocations this way amortizes the runtime cost of SoftLimit which is
	// typically shared by many goroutines.
	minReserveSize = 256 << 10
	// smallReserveSize is the threshold below which the size is considered
	// "small", and causes reserving in multiples of minReserveSize at a time.
	smallReserveSize = 4 << 20
)

// SoftLimit is a byte size limit with weak guarantees. It tracks the global
// usage in a metric, and gives the user a hint when the usage has reached the
// soft limit.
//
// When at the limit, the user is supposed to back off acquiring resources, but
// is not strictly required to. There can be overflows when the user acquires
// resources optimistically. It is recommended to make sure that the possible
// overflows are bounded (e.g. there is a bounded number of workers, and each
// can optimistically acquire a bounded amount), and to account for these
// overflows when picking the soft limit.
//
// TODO(pav-kv): integrate with mon.BytesMonitor.
type SoftLimit struct {
	Metric *metric.Gauge // the "global" usage metric
	Limit  int64         // the soft limit
}

// acquire accepts the given number of bytes for tracking.
func (s *SoftLimit) acquire(x uint64) {
	s.Metric.Inc(int64(x))
}

// withinLimit returns true iff the bytes usage hasn't reached the soft limit.
func (s *SoftLimit) withinLimit() bool {
	return s.Limit <= 0 || s.Metric.Value() < s.Limit
}

// release removes the given number of bytes from tracking.
func (s *SoftLimit) release(x uint64) {
	s.Metric.Dec(int64(x))
}

// BytesAccount acquires bytes from SoftLimit, and releases them at the end of
// the lifetime.
type BytesAccount struct {
	lim      *SoftLimit
	metric   *metric.Gauge // the "local" usage metric
	used     uint64
	reserved uint64 // reserved bytes are not used yet but are accounted for in lim
}

// NewAccount creates a BytesAccount consuming from this SoftLimit.
func (s *SoftLimit) NewAccount(metric *metric.Gauge) BytesAccount {
	return BytesAccount{lim: s, metric: metric}
}

// Initialized returns true iff this BytesAccount is usable.
func (b *BytesAccount) Initialized() bool {
	return b != nil && b.lim != nil
}

// Grow optimistically acquires and accounts for the given number of bytes.
// Returns false when this leads to the SoftLimit overflow, in which case the
// user should back off acquiring any more resources.
func (b *BytesAccount) Grow(x uint64) (withinLimit bool) {
	if !b.Initialized() {
		return true
	}
	withinLimit = true
	if x > b.reserved {
		need := roundSize(x - b.reserved)
		b.lim.acquire(need)
		b.reserved += need
		if withinLimit = b.lim.withinLimit(); !withinLimit && b.reserved > x {
			// If we reached the soft limit, drain the remainder of the reserved bytes
			// to the limiter. We will not use it - the client typically stops calling
			// Grow after this.
			b.lim.release(b.reserved - x)
			b.reserved = x
		}
	}
	b.reserved -= x
	b.used += x
	if b.metric != nil {
		b.metric.Inc(int64(x))
	}
	return withinLimit
}

// Clear returns all the reserved bytes into the SoftLimit.
func (b *BytesAccount) Clear() {
	if !b.Initialized() {
		return
	}
	if b.metric != nil {
		b.metric.Dec(int64(b.used))
	}
	b.lim.release(b.used + b.reserved)
	b.used, b.reserved = 0, 0
}

func roundSize(size uint64) uint64 {
	if size >= smallReserveSize {
		// Don't round the size up if the allocation is large. This also avoids edge
		// cases in the math below if size == math.MaxInt64.
		return size
	}
	return (size + minReserveSize - 1) / minReserveSize * minReserveSize
}

// sizeHelper helps to build a batch of entries with the total size not
// exceeding the static and dynamic byte limits. The user calls the add() method
// until sizeHelper.done becomes true. For all add() calls that returned true,
// the corresponding data can be added to the batch.
//
// In some exceptional cases, the size of the batch can exceed the limits:
//   - maxBytes can be exceeded by the first added entry,
//   - the soft limit can be exceeded by the last added entry.
type sizeHelper struct {
	bytes    uint64
	maxBytes uint64
	account  *BytesAccount
	done     bool
}

// add returns true if the given number of bytes can be added to the batch
// without exceeding the maxBytes limit, or overflowing the bytes account. The
// first add call always returns true.
//
// Must not be called after sizeHelper.done becomes true.
func (s *sizeHelper) add(bytes uint64) bool {
	if s.bytes == 0 { // this is the first entry, always take it
		s.bytes += bytes
		s.done = !s.account.Grow(bytes) || s.bytes > s.maxBytes
		return true
	} else if s.bytes+bytes > s.maxBytes {
		s.done = true
		return false
	}
	s.bytes += bytes
	s.done = !s.account.Grow(bytes)
	return true
}
