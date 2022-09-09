// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvevent

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
)

func TestBufferEntryQueue(t *testing.T) {
	rand, _ := randutil.NewTestRand()

	q := bufferEventChunkQueue{}

	// Add one event and remove it
	assert.True(t, q.empty())
	q.enqueue(Event{})
	assert.False(t, q.empty())
	_, ok := q.dequeue()
	assert.True(t, ok)
	assert.True(t, q.empty())

	// Add events to fill 5 chunks and assert they are consumed in fifo order.
	eventCount := bufferEventChunkArrSize * 5
	var lastPop int64 = -1
	var lastPush int64 = -1

	for eventCount > 0 {
		op := rand.Intn(2)
		if op == 0 {
			q.enqueue(Event{backfillTimestamp: hlc.Timestamp{WallTime: lastPush + 1}})
			lastPush++
		} else {
			e, ok := q.dequeue()
			if !ok {
				assert.Equal(t, lastPop, lastPush)
				assert.True(t, q.empty())
			} else {
				assert.Equal(t, lastPop+1, e.backfillTimestamp.WallTime)
				lastPop++
				eventCount--
			}
		}
	}

	// Verify that purging works.
	eventCount = bufferEventChunkArrSize * 2.5
	for eventCount > 0 {
		q.enqueue(Event{})
		eventCount--
	}
	q.purge()
	assert.True(t, q.empty())
}
