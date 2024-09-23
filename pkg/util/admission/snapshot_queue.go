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
	"github.com/cockroachdb/errors"
)

// TODO(aaditya): consider sync.pool for this
type snapshotWorkItem struct {
	admitCh        chan bool
	enqueueingTime time.Time
	count          int64
}

// SnapshotQueue implements the requester interface. It is used to request
// admission for KV range snapshot requests.
type SnapshotQueue struct {
	snapshotGranter granter
	mu              struct {
		syncutil.Mutex
		q []*snapshotWorkItem
	}
}

func MakeSnapshotQueue(snapshotGranter granter) *SnapshotQueue {
	q := &SnapshotQueue{snapshotGranter: snapshotGranter}
	q.mu.q = []*snapshotWorkItem{}
	return q
}

var _ requester = &SnapshotQueue{}

func (s *SnapshotQueue) hasWaitingRequests() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.mu.q) > 0
}

func (s *SnapshotQueue) granted(_ grantChainID) int64 {
	// TODO(aaditya): maybe add more logic here. Look at the WorkQueue granted method.
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.popLocked()
	item.admitCh <- true
	return item.count
}

func (s *SnapshotQueue) close() {
	// TODO(aaditya): nothing to do here?
	return
}

func (s *SnapshotQueue) Admit(ctx context.Context, count int64) error {
	if s.snapshotGranter.tryGet(count) {
		return nil
	}

	// We were unable to get tokens for admission, so we queue.
	s.mu.Lock()
	item := &snapshotWorkItem{
		admitCh:        make(chan bool),
		enqueueingTime: time.Now(),
		count:          count,
	}
	s.addLocked(item)
	s.mu.Unlock()

	// Start waiting for admission.
	select {
	case <-ctx.Done():
		waitDur := time.Now().Sub(item.enqueueingTime)
		s.snapshotGranter.returnGrant(item.count)
		deadline, _ := ctx.Deadline()
		return errors.Wrapf(ctx.Err(),
			"context canceled while waiting in queue: deadline: %v, start: %v, dur: %v",
			deadline, item.enqueueingTime, waitDur)
	case <-item.admitCh:
		waitDur := time.Now().Sub(item.enqueueingTime)
		// 	TODO(aaditya): add waiting metrics
		return nil
	}
}

func (s *SnapshotQueue) addLocked(item *snapshotWorkItem) {
	item.enqueueingTime = time.Now()
	s.mu.q = append(s.mu.q, item)
}

func (s *SnapshotQueue) popLocked() *snapshotWorkItem {
	if len(s.mu.q) == 0 {
		return nil
	}
	item := s.mu.q[0]
	s.mu.q = s.mu.q[1:]
	return item
}
