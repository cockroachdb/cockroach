// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstorebench

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type replicaQueue struct {
	// NB: this does not randomize which replicas are handed to
	// workers. This is not expected to change the outcomes, but
	// might be worth revisiting.
	q chan *replicaWriteState
}

func makeReplicaQueue(numReplicas int) *replicaQueue {
	q := &replicaQueue{
		q: make(chan *replicaWriteState, numReplicas),
	}

	prefixLen := 10
	dedupRanges := make(map[string]bool)
	keyPrefix := make([]byte, prefixLen)
	rng := rand.New(rand.NewSource(0))
	for i := 0; i < numReplicas; i++ {
		for {
			rng.Read(keyPrefix)
			// Don't accidentally mix with the local keys.
			keyPrefix[0] = 'a'
			if !dedupRanges[string(keyPrefix)] {
				dedupRanges[string(keyPrefix)] = true
				break
			}
		}
		r := &replicaWriteState{
			keyPrefix:        keyPrefix,
			rangeID:          roachpb.RangeID(i + 1),
			rangeIDPrefixBuf: keys.MakeRangeIDPrefixBuf(roachpb.RangeID(i + 1)),
			nextRaftLogIndex: 1,
		}
		q.push(r)
	}
	return q
}

func (q *replicaQueue) pop() *replicaWriteState {
	return <-q.q
}

func (q *replicaQueue) push(r *replicaWriteState) {
	q.q <- r
}

type replicaWriteState struct {
	keyPrefix        []byte
	rangeID          roachpb.RangeID
	rangeIDPrefixBuf keys.RangeIDPrefixBuf

	nextRaftLogIndex  uint64
	truncatedLogIndex uint64
	logSizeBytes      int64

	pendingTruncIndex         uint64
	pendingTruncCallbackCount int64

	hs    raftpb.HardState
	hsBuf []byte

	buf []byte
}
