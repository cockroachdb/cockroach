// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	// Fill 5 chunks and then pop each one, ensuring empty() returns the correct
	// value each time.
	eventCount := bufferEventChunkArrSize * 5
	for i := 0; i < eventCount; i++ {
		q.enqueue(Event{})
	}
	for {
		assert.Equal(t, eventCount <= 0, q.empty())
		_, ok = q.dequeue()
		if !ok {
			assert.True(t, q.empty())
			break
		} else {
			eventCount--
		}
	}
	assert.Equal(t, 0, eventCount)
	q.enqueue(Event{})
	assert.False(t, q.empty())
	q.dequeue()
	assert.True(t, q.empty())

	// Add events to fill 5 chunks and assert they are consumed in fifo order.
	eventCount = bufferEventChunkArrSize * 5
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
