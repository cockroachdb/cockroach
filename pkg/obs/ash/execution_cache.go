// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// ExecutionAttrs holds the per-execution attributes used to enrich ASH
// samples. Attributes are captured on the gateway node when a statement
// begins executing, and are looked up by enrichment_id during sampling
// (locally on the gateway, or via RPC from a remote node).
type ExecutionAttrs struct {
	User     string
	Database string
	Query    string
	TxnID    uuid.UUID
}

// ExecutionCache is a node-local, bounded FIFO cache of per-execution
// attributes keyed by a monotonically increasing enrichment_id.
//
// Lifecycle: a Put is performed by the gateway node at the start of
// statement execution. Get is performed by the ASH sampler (locally or
// via RPC) when emitting a sample referencing the enrichment_id. The
// expected retention is short, but long enough for downstream samplers
// to retrieve the attributes for the executions in flight.
//
// Eviction is FIFO: when the entry count exceeds the configured limit,
// the lowest (oldest) IDs are deleted first. Because IDs are issued by
// a monotonically increasing counter and Put is the only way to insert,
// the smallest extant ID is always the oldest.
type ExecutionCache struct {
	mu struct {
		syncutil.Mutex
		nextID   uint64
		oldestID uint64
		entries  map[uint64]ExecutionAttrs
	}
	// limit is the maximum number of entries. If zero, Put returns 0
	// without recording anything.
	limit int
}

// executionCacheFullLog rate-limits the eviction warning.
var executionCacheFullLog = log.Every(time.Minute)

// NewExecutionCache returns an empty ExecutionCache with the given limit.
// A limit of zero disables the cache (Put returns 0).
func NewExecutionCache(limit int) *ExecutionCache {
	c := &ExecutionCache{limit: limit}
	c.mu.entries = make(map[uint64]ExecutionAttrs)
	c.mu.oldestID = 1
	return c
}

// Put records the attributes and returns a new enrichment_id. Returns 0
// if the cache is disabled (limit == 0).
func (c *ExecutionCache) Put(attrs ExecutionAttrs) uint64 {
	if c.limit == 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.nextID++
	id := c.mu.nextID
	c.mu.entries[id] = attrs
	for len(c.mu.entries) > c.limit {
		delete(c.mu.entries, c.mu.oldestID)
		c.mu.oldestID++
		if executionCacheFullLog.ShouldLog() {
			log.Ops.Warningf(
				context.Background(),
				"ASH execution cache is at capacity (%d); evicting oldest entries",
				c.limit,
			)
		}
	}
	return id
}

// Get returns the attributes for id, or the zero value and false if not
// present.
func (c *ExecutionCache) Get(id uint64) (ExecutionAttrs, bool) {
	if id == 0 {
		return ExecutionAttrs{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	attrs, ok := c.mu.entries[id]
	return attrs, ok
}

// GetMappings returns a map of id → attrs for every requested id that is
// present in the cache. IDs that miss are omitted.
func (c *ExecutionCache) GetMappings(ids []uint64) map[uint64]ExecutionAttrs {
	result := make(map[uint64]ExecutionAttrs, len(ids))
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range ids {
		if attrs, ok := c.mu.entries[id]; ok {
			result[id] = attrs
		}
	}
	return result
}

// PutExecution stores the attributes in the process-wide cache and
// returns an enrichment_id. A nil sv means "skip the enabled gate" and
// is used by callers that don't have a settings.Values handy (e.g.,
// the status RPC handler). Returns 0 when the feature is gated off or
// when ASH sampling itself is disabled (no consumer to enrich for).
func PutExecution(sv *settings.Values, attrs ExecutionAttrs) uint64 {
	if !enabled.Load() {
		return 0
	}
	if sv != nil && !ExecutionCacheEnabled.Get(sv) {
		return 0
	}
	return getGlobalExecutionCache().Put(attrs)
}

// GetExecution returns the attributes for id, or the zero value and
// false if not present (or if the feature is gated off). A nil sv
// means "skip the enabled gate" — see PutExecution.
func GetExecution(sv *settings.Values, id uint64) (ExecutionAttrs, bool) {
	if sv != nil && !ExecutionCacheEnabled.Get(sv) {
		return ExecutionAttrs{}, false
	}
	return getGlobalExecutionCache().Get(id)
}

// GetExecutionMappings is the batch variant used by the status RPC.
func GetExecutionMappings(ids []uint64) map[uint64]ExecutionAttrs {
	return getGlobalExecutionCache().GetMappings(ids)
}
