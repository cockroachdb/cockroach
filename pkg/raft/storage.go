// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/errors"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
//
// TODO(pav-kv): this is used only in tests. Remove it.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// LogStorage is a read-only API for the raft log.
type LogStorage interface {
	// Entries returns a slice of consecutive log entries in the range [lo, hi),
	// starting from lo. The maxSize limits the total size of the log entries
	// returned, but Entries returns at least one entry if any.
	//
	// The caller of Entries owns the returned slice, and may append to it. The
	// individual entries in the slice must not be mutated, neither by the Storage
	// implementation nor the caller. Note that raft may forward these entries
	// back to the application via Ready struct, so the corresponding handler must
	// not mutate entries either (see comments in Ready struct).
	//
	// Since the caller may append to the returned slice, Storage implementation
	// must protect its state from corruption that such appends may cause. For
	// example, common ways to do so are:
	//  - allocate the slice before returning it (safest option),
	//  - return a slice protected by Go full slice expression, which causes
	//  copying on appends (see MemoryStorage).
	//
	// Returns ErrCompacted if entry lo has been compacted, or ErrUnavailable if
	// encountered an unavailable entry in [lo, hi).
	//
	// TODO(pav-kv): all log slices in raft are constructed in context of being
	// appended after a particular log index, so (lo, hi] semantics fits better
	// than [lo, hi).
	//
	// TODO(#132789): change the semantics so that maxSize can be exceeded not
	// only if the first entry is large. It should be ok to exceed maxSize if the
	// last entry makes it so. In the underlying storage implementation, we have
	// paid the cost of fetching this entry anyway, so there is no need to drop it
	// from the result.
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)

	// Term returns the term of the entry at the given index, which must be in the
	// valid range: [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the rest of that
	// entry may not be available.
	Term(index uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	// TODO(pav-kv): replace this with LastEntryID() which never fails.
	LastIndex() uint64
	// FirstIndex returns the index of the first log entry that is possibly
	// available via Entries. Older entries have been incorporated into the
	// StateStorage.Snapshot.
	//
	// If storage only contains the dummy entry or initial snapshot then
	// FirstIndex still returns the snapshot index + 1, yet the first log entry at
	// this index is not available.
	//
	// TODO(pav-kv): replace this with a Prev() method equivalent to LogSlice's
	// prev field. The log storage is just a storage-backed LogSlice.
	FirstIndex() uint64

	// LogSnapshot returns an immutable point-in-time log storage snapshot.
	LogSnapshot() LogStorageSnapshot
}

// LogStorageSnapshot is a read-only API for the raft log which has extended
// immutability guarantees outside RawNode. The immutability must be provided by
// the application layer.
type LogStorageSnapshot interface {
	LogStorage
}

// StateStorage provides read access to the state machine storage.
type StateStorage interface {
	// Snapshot returns the most recent state machine snapshot.
	Snapshot() (pb.Snapshot, error)
}

// Storage is an interface that should be implemented by the application to
// provide raft with access to the log and state machine storage.
//
// If any method returns an error other than ErrCompacted or ErrUnavailable, the
// raft instance generally does not behave gracefully, e.g. it may panic.
//
// TODO(pav-kv): audit all error handling and document the contract.
type Storage interface {
	// InitialState returns the saved HardState and ConfState information.
	//
	// TODO(sep-raft-log): this would need to be fetched (fully or partially) from
	// both log and state machine storage on startup, to detect which of the two
	// storages is ahead, and initialize correctly.
	InitialState() (pb.HardState, pb.ConfState, error)

	LogStorage
	StateStorage
}

type inMemStorageCallStats struct {
	initialState, firstIndex, lastIndex, entries, term, snapshot int
}

// MemoryStorage implements the Storage interface backed by an
// in-memory array.
type MemoryStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	sync.Mutex

	hardState pb.HardState
	snapshot  pb.Snapshot
	// ents[i] has raft log position i+snapshot.Metadata.Index
	ents []pb.Entry

	callStats inMemStorageCallStats
}

// NewMemoryStorage creates an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		ents: make([]pb.Entry, 1),
	}
}

// InitialState implements the Storage interface.
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	ms.callStats.initialState++
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

// SetHardState saves the current HardState.
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	ms.callStats.entries++
	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		raftlogger.GetLogger().Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}
	// only contains dummy entries.
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}

	ents := limitSize(ms.ents[lo-offset:hi-offset], entryEncodingSize(maxSize))
	// NB: use the full slice expression to limit what the caller can do with the
	// returned slice. For example, an append will reallocate and copy this slice
	// instead of corrupting the neighbouring ms.ents.
	return ents[:len(ents):len(ents)], nil
}

// Term implements the Storage interface.
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	ms.callStats.term++
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex implements the Storage interface.
func (ms *MemoryStorage) LastIndex() uint64 {
	ms.Lock()
	defer ms.Unlock()
	ms.callStats.lastIndex++
	return ms.lastIndex()
}

func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.
func (ms *MemoryStorage) FirstIndex() uint64 {
	ms.Lock()
	defer ms.Unlock()
	ms.callStats.firstIndex++
	return ms.firstIndex()
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// LogSnapshot implements the LogStorage interface.
func (ms *MemoryStorage) LogSnapshot() LogStorageSnapshot {
	// TODO(pav-kv): return an immutable subset of MemoryStorage.
	return ms
}

// Snapshot implements the Storage interface.
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	ms.callStats.snapshot++
	return ms.snapshot, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	//handle check for old snapshot being applied
	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex {
		return ErrSnapOutOfDate
	}

	ms.snapshot = snap
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
func (ms *MemoryStorage) CreateSnapshot(
	i uint64, cs *pb.ConfState, data []byte,
) (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	if i <= ms.snapshot.Metadata.Index {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := ms.ents[0].Index
	if i > ms.lastIndex() {
		raftlogger.GetLogger().Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}

	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data
	return ms.snapshot, nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		raftlogger.GetLogger().Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset
	// NB: allocate a new slice instead of reusing the old ms.ents. Entries in
	// ms.ents are immutable, and can be referenced from outside MemoryStorage
	// through slices returned by ms.Entries().
	ents := make([]pb.Entry, 1, uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex()
	last := entries[0].Index + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	if last < first {
		return nil
	}
	// truncate compacted entries
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		// NB: full slice expression protects ms.ents at index >= offset from
		// rewrites, as they may still be referenced from outside MemoryStorage.
		ms.ents = append(ms.ents[:offset:offset], entries...)
	case uint64(len(ms.ents)) == offset:
		ms.ents = append(ms.ents, entries...)
	default:
		raftlogger.GetLogger().Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}
