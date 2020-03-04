// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TestStorage is a storage backed by a map[NodeID]Entry.
type TestStorage struct {
	mu syncutil.Mutex
	m  map[roachpb.NodeID][]ctpb.Entry
}

// VisitAscending implements closedts.Storage.
func (s *TestStorage) VisitAscending(nodeID roachpb.NodeID, f func(ctpb.Entry) (done bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range s.m[nodeID] {
		if f(entry) {
			break
		}
	}
}

// VisitDescending implements closedts.Storage.
func (s *TestStorage) VisitDescending(nodeID roachpb.NodeID, f func(entry ctpb.Entry) (done bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := len(s.m[nodeID]) - 1; i >= 0; i-- {
		if f(s.m[nodeID][i]) {
			break
		}
	}
}

// Add implements closedts.Storage.
func (s *TestStorage) Add(nodeID roachpb.NodeID, entry ctpb.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.m == nil {
		s.m = map[roachpb.NodeID][]ctpb.Entry{}
	}

	s.m[nodeID] = append(s.m[nodeID], entry)
	sort.Slice(s.m[nodeID], func(i, j int) bool {
		e1, e2 := s.m[nodeID][i], s.m[nodeID][j]
		if e1.ClosedTimestamp == e2.ClosedTimestamp {
			return e1.Epoch < e2.Epoch
		}
		return e1.ClosedTimestamp.Less(e2.ClosedTimestamp)
	})
}

// Clear implements closedts.Storage.
func (s *TestStorage) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m = nil
}

// Snapshot returns a copy of the data contain within the TestStorage.
func (s *TestStorage) Snapshot() map[roachpb.NodeID][]ctpb.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	m := map[roachpb.NodeID][]ctpb.Entry{}
	for nodeID, entries := range s.m {
		m[nodeID] = append([]ctpb.Entry(nil), entries...)
	}
	return m
}
