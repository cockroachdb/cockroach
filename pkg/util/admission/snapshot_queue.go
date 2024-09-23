// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TODO(aaditya): consider sync.pool for this
type SnapshotWorkItem struct {
	admitCh        chan bool
	enqueueingTime time.Time
}

// SnapshotQueue implements the requester interface. It is used to request
// admission for KV range snapshot requests.
type SnapshotQueue struct {
	snapshotGranter kvStoreTokenChildGranter
	mu              struct {
		syncutil.Mutex
		q []*SnapshotWorkItem
	}
}

var _ requester = &SnapshotQueue{}

func (s *SnapshotQueue) hasWaitingRequests() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.mu.q) > 0
}

func (s *SnapshotQueue) granted(_ grantChainID) int64 {
	// TODO(aaditya): add more logic here. Look at the WorkQueue granted method.
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.popLocked()
	item.admitCh <- true
	// TODO(aaditya): return non-zero
	return 0
}

func (s *SnapshotQueue) close() {
	//TODO implement me
	panic("implement me")
}

func (s *SnapshotQueue) Admit(ctx context.Context, item *SnapshotWorkItem) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshotGranter.tryGet(1)
	s.addLocked(item)
	// TODO(aaditya): add waiting logic and metrics
	select {
	case <-ctx.Done():
		return
	case <-item.admitCh:
		// Do something here
	}
}

func (s *SnapshotQueue) addLocked(item *SnapshotWorkItem) {
	item.enqueueingTime = time.Now()
	s.mu.q = append(s.mu.q, item)
}

func (s *SnapshotQueue) popLocked() *SnapshotWorkItem {
	if len(s.mu.q) == 0 {
		return nil
	}
	item := s.mu.q[0]
	s.mu.q = s.mu.q[1:]
	return item
}
