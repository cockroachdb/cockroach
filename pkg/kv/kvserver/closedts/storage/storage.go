// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SingleStorage stores and manages closed timestamp updates originating from a
// single source (i.e. node). A SingleStorage internally maintains multiple
// buckets for historical closed timestamp information. The reason for this is
// twofold:
//
// 1. The most recent closed timestamp update is also the hardest to prove a
// read for, since it comes with larger minimum lease applied indexes. In
// situations in which followers are lagging behind with their command
// application, this could lead to a runaway scenario, in which a closed
// timestamp update can never be used until it is replaced by a new one, which
// in turn also will never be used, etc. Instead, a SingleStorage keeps some
// amount of history and upstream systems can try to prove a follower read using
// an older closed timestamp instead.
//
// 2. Follower reads can be used to implement recovery of a consistent
// cluster-wide snapshot after catastrophic loss of quorum. To do this, the
// mechanism must locate at least one replica of every range in the cluster, and
// for each range find the largest possible timestamp at which follower reads
// are possible among the surviving replicas. Of all these per-range timestamps,
// the smallest can be used to read from all ranges, resulting in a consistent
// snapshot. This makes it crucial that every replica can serve at least some
// follower reads, even when regularly outpaced by the closed timestamp
// frontier. Emitted MLAIs may never even be proposed to Raft in the event of
// an ill-timed crash, and so historic information is invaluable.
//
// TODO(tschottdorf): revisit whether this shouldn't be a concrete impl instead,
// with only the buckets abstracted out.
type SingleStorage interface {
	fmt.Stringer
	// VisitAscending walks through the buckets of the storage in ascending
	// closed timestamp order, until the closure returns true (or all buckets
	// have been visited).
	VisitAscending(func(ctpb.Entry) (done bool))
	// VisitDescending walks through the buckets of the storage in descending
	// closed timestamp order, until the closure returns true (or all buckets
	// have been visited).
	VisitDescending(func(ctpb.Entry) (done bool))
	// Add adds a new Entry to this storage. The entry is added to the most
	// recent bucket and remaining buckets are rotated as indicated by their age
	// relative to the newly added Entry.
	Add(ctpb.Entry)
	// Clear removes all Entries from this storage.
	Clear()
}

type entry struct {
	SingleStorage
}

// MultiStorage implements the closedts.Storage interface.
type MultiStorage struct {
	// constructor creates a SingleStorage whenever one is initialized for a new
	// NodeID.
	constructor func() SingleStorage
	// TODO(tschottdorf): clean up storages that haven't been used for extended
	// periods of time.
	m syncutil.IntMap
}

var _ closedts.Storage = (*MultiStorage)(nil)

// NewMultiStorage sets up a MultiStorage which uses the given factory method
// for setting up the SingleStorage used for each individual NodeID for which
// operations are received.
func NewMultiStorage(constructor func() SingleStorage) *MultiStorage {
	return &MultiStorage{constructor: constructor}
}

func (ms *MultiStorage) getOrCreate(nodeID roachpb.NodeID) SingleStorage {
	key := int64(nodeID)
	p, found := ms.m.Load(key)
	if found {
		// Fast path that avoids calling f().
		return (*entry)(p).SingleStorage
	}

	ss := ms.constructor()
	p, _ = ms.m.LoadOrStore(key, unsafe.Pointer(&entry{ss}))
	return (*entry)(p).SingleStorage
}

// VisitAscending implements closedts.Storage.
func (ms *MultiStorage) VisitAscending(nodeID roachpb.NodeID, f func(ctpb.Entry) (done bool)) {
	ss := ms.getOrCreate(nodeID)
	ss.VisitAscending(f)
}

// VisitDescending implements closedts.Storage.
func (ms *MultiStorage) VisitDescending(nodeID roachpb.NodeID, f func(ctpb.Entry) (done bool)) {
	ss := ms.getOrCreate(nodeID)
	ss.VisitDescending(f)
}

// Add implements closedts.Storage.
func (ms *MultiStorage) Add(nodeID roachpb.NodeID, entry ctpb.Entry) {
	ss := ms.getOrCreate(nodeID)
	ss.Add(entry)
}

// Clear implements closedts.Storage.
func (ms *MultiStorage) Clear() {
	ms.m.Range(func(_ int64, p unsafe.Pointer) bool {
		(*entry)(p).SingleStorage.Clear()
		return true // continue
	})
}

// String prints a tabular rundown of the contents of the MultiStorage.
func (ms *MultiStorage) String() string {
	return ms.StringForNodes()
}

// StringForNodes is like String, but restricted to the supplied NodeIDs.
// If none are specified, is equivalent to String().
func (ms *MultiStorage) StringForNodes(nodes ...roachpb.NodeID) string {
	type tuple struct {
		roachpb.NodeID
		SingleStorage
	}

	var shouldPrint map[roachpb.NodeID]struct{}
	if len(nodes) > 0 {
		shouldPrint = make(map[roachpb.NodeID]struct{}, len(nodes))
		for _, nodeID := range nodes {
			shouldPrint[nodeID] = struct{}{}
		}
	}

	var sl []tuple
	ms.m.Range(func(k int64, p unsafe.Pointer) bool {
		nodeID := roachpb.NodeID(k)
		if _, ok := shouldPrint[nodeID]; ok || len(shouldPrint) == 0 {
			sl = append(sl, tuple{nodeID, (*entry)(p).SingleStorage})
		}
		return true // want more
	})
	sort.Slice(sl, func(i, j int) bool {
		return sl[i].NodeID < sl[j].NodeID
	})
	var buf bytes.Buffer
	for i := range sl {
		buf.WriteString(fmt.Sprintf("***** n%d *****\n", sl[i].NodeID))
		buf.WriteString(sl[i].SingleStorage.String())
	}
	return buf.String()
}
