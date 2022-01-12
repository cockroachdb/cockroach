// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnidcache

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const defaultRingSize = 4

type shard struct {
	syncutil.RWMutex
	ring     []*strip
	head     int64
	tail     int64
	ringSize int64

	evictedCount *metric.Counter
}

var _ storage = &shard{}

func newShard(
	capacity contentionutils.CapacityLimiter, ringSize int64, evictedCount *metric.Counter,
) *shard {
	shard := &shard{
		ring:         make([]*strip, ringSize),
		head:         0,
		tail:         0,
		ringSize:     ringSize,
		evictedCount: evictedCount,
	}

	for i := int64(0); i < ringSize; i++ {
		shard.ring[i] = newStrip(func() int64 {
			return capacity() / ringSize
		})
	}

	return shard
}

func (s *shard) Lookup(txnID uuid.UUID) (roachpb.TransactionFingerprintID, bool) {
	s.RLock()
	defer s.RUnlock()

	for i := s.head; ; i = s.prevIdx(i) {
		fingerprintID, found := s.ring[i].Lookup(txnID)
		if found {
			return fingerprintID, found
		}
		if i == s.tail {
			break
		}
	}
	return roachpb.TransactionFingerprintID(0), false
}

func (s *shard) push(block messageBlock) {
	s.Lock()
	defer s.Unlock()

	blockOffset := 0
	more := false
	for {
		strip := s.ring[s.head]
		blockOffset, more = strip.tryInsertBlock(block, blockOffset)

		if more {
			s.rotateRing()
		} else {
			break
		}
	}
}

func (s *shard) rotateRing() {
	s.head = s.nextIdx(s.head)
	if s.head == s.tail {
		s.tail = s.nextIdx(s.tail)
		s.ring[s.head].clear()
		s.evictedCount.Inc(s.ring[s.head].capacity())
	}
}

func (s *shard) nextIdx(idx int64) int64 {
	return (idx + 1) % s.ringSize
}

func (s *shard) prevIdx(idx int64) int64 {
	if idx == 0 {
		return s.ringSize - 1
	}
	return idx - 1
}
