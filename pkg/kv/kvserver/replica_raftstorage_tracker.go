// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"go.etcd.io/raft/v3/raftpb"
)

type logRangeSize struct {
	lastIndex uint64
	size      uint64
}

func WatchRaftMemoryBudgetingSetting(s *Stores, sv *settings.Values) {
	raftMemoryBudgetingEnabled.SetOnChange(sv, func(ctx context.Context) {
		if raftMemoryBudgetingEnabled.Get(sv) {
			return
		}
		_ = s.VisitStores(func(s *Store) error {
			s.VisitReplicas(func(r *Replica) bool {
				r.tracker.clear()
				return true
			})
			return nil
		})
	})
}

// raftStorageTracker tracks all in-memory entry slices pulled from RaftStorage.
type raftStorageTracker struct {
	mu syncutil.Mutex
	// sizes tracks the total size of all entry slices pulled into memory, by the
	// first entry index.
	sizes map[uint64]logRangeSize
}

func newRaftStorageTracker() *raftStorageTracker {
	return &raftStorageTracker{sizes: make(map[uint64]logRangeSize)}
}

func (r *raftStorageTracker) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sizes = make(map[uint64]logRangeSize)
}

func (r *raftStorageTracker) add(entries []raftpb.Entry, size uint64) {
	if len(entries) == 0 {
		return
	}
	first := entries[0].Index
	r.mu.Lock()
	defer r.mu.Unlock()

	info := r.sizes[first]
	info.lastIndex = max(info.lastIndex, first+uint64(len(entries))-1)
	info.size += size
	r.sizes[first] = info
}

func (r *raftStorageTracker) free(entries []raftpb.Entry) uint64 {
	if r == nil || len(entries) == 0 {
		return 0
	}
	first := entries[0].Index
	r.mu.Lock()
	defer r.mu.Unlock()

	info := r.sizes[first]
	end := min(info.lastIndex+1, first+uint64(len(entries)))

	size := uint64(0)
	for i := range entries[:end-first] {
		next := size + uint64(entries[i].Size())
		if next > info.size {
			size = info.size
			break
		}
		size = next
	}

	if size == info.size {
		delete(r.sizes, first)
	} else { // size < info.size
		info.size -= size
		r.sizes[first] = info
	}
	return size
}
