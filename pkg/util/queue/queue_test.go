// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package queue

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestQueue(t *testing.T) {
	rng, _ := randutil.NewTestRand()

	const eventCount = 1000000
	chunkSize := rng.Intn(255) + 1

	q, err := NewQueue[*testQueueItem](WithChunkSize[*testQueueItem](chunkSize))
	require.NoError(t, err)
	runQueueTest(t, q, eventCount)
}

func TestChunkSize(t *testing.T) {
	q, err := NewQueue[*testQueueItem](WithChunkSize[*testQueueItem](0))
	require.Error(t, err)
	require.Nil(t, q)

	q, err = NewQueue[*testQueueItem](WithChunkSize[*testQueueItem](1))
	require.Equal(t, 1, q.chunkSize)
	require.NoError(t, err)

	q, err = NewQueue[*testQueueItem]()
	require.Equal(t, defaultChunkSize, q.chunkSize)
	require.NoError(t, err)
}

func BenchmarkQueue(b *testing.B) {
	b.ReportAllocs()
	const eventCount = 2000000

	q, err := NewQueue[*testQueueItem]()
	require.NoError(b, err)

	for i := 0; i < b.N; i++ {
		for i := 0; i < eventCount; i++ {
			q.Enqueue(&testQueueItem{})
		}
		q.removeAll()
	}

	for i := 0; i < b.N; i++ {
		for i := 0; i < eventCount; i++ {
			q.Enqueue(&testQueueItem{})
		}
		for i := 0; i < eventCount; i++ {
			q.Dequeue()
		}
	}
}
