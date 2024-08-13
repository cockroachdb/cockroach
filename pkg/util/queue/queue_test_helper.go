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

	"github.com/stretchr/testify/require"
)

func (q *Queue[T]) removeAll() {
	for q.head != nil {
		q.head = q.head.next
		// The previous value of q.head will be garbage collected.
	}
	q.tail = q.head
}

func (q *Queue[T]) checkNil(t *testing.T) {
	require.Nil(t, q.head)
	require.Nil(t, q.tail)
}

// noop for Queue implementation since it doesn't track event count in the
// queue.
func (q *Queue[T]) checkEventCount(t *testing.T, _ int) {
}

// checkInvariants checks the invariants of the queue.
func (q *Queue[T]) checkInvariants(t *testing.T) {
	if q.head == nil && q.tail == nil {
		require.True(t, q.Empty())
	} else if q.head != nil && q.tail == nil {
		t.Fatal("head is nil but tail is non-nil")
	} else if q.head == nil && q.tail != nil {
		t.Fatal("tail is nil but head is non-nil")
	} else {
		// The queue maintains an invariant that it contains no finished chunks.
		require.False(t, q.head.finished())
		require.False(t, q.tail.finished())

		if q.head == q.tail {
			require.Nil(t, q.head.next)
		} else {
			// q.tail is non-nil and not equal to q.head. There must be a non-empty
			// chunk after q.head.
			require.False(t, q.Empty())
			if q.head.empty() {
				require.False(t, q.head.next.empty())
			}
			// The q.tail is never empty in this case. The tail can only be empty
			// when it is equal to q.head and q.head is empty.
			require.False(t, q.tail.empty())
		}
	}
}
