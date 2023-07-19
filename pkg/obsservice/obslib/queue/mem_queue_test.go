// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package queue

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestMemoryQueue_Enqueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	sizeFn := func(e *testElement) (int, error) {
		return e.Size(), nil
	}

	tests := []struct {
		name      string
		maxBytes  int
		toEnqueue []*testElement
		expLen    int
		// Expected size of queue in bytes after a single Dequeue call
		expByteSize int
		// applies to the final enqueue call for elements in toEnqueue
		wantErr bool
	}{
		{
			name:     "enqueues up to maxBytes",
			maxBytes: 20,
			toEnqueue: []*testElement{
				newTestEl(10),
				newTestEl(10),
			},
			expByteSize: 20,
			expLen:      2,
		},
		{
			name:     "returns error past maxBytes, does not enqueue",
			maxBytes: 29,
			toEnqueue: []*testElement{
				newTestEl(10),
				newTestEl(10),
				newTestEl(10),
			},
			expByteSize: 20,
			expLen:      2,
			wantErr:     true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := NewMemoryQueue[*testElement](tc.maxBytes, sizeFn, "enqueue_test")
			var err error
			for i, el := range tc.toEnqueue {
				err = q.Enqueue(el)
				if i < len(tc.toEnqueue)-1 {
					require.NoError(t, err)
				}
			}
			func() {
				q.mu.Lock()
				defer q.mu.Unlock()
				require.Equalf(t, q.mu.curSize, tc.expByteSize, "unexpected size in bytes")
			}()
			require.Equalf(t, q.Len(), tc.expLen, "unexpected queue length")
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestMemoryQueue_Dequeue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	sizeFn := func(e *testElement) (int, error) {
		return e.Size(), nil
	}

	element1 := newTestEl(10)
	element2 := newTestEl(10)
	element3 := newTestEl(10)
	tests := []struct {
		name      string
		maxBytes  int
		toEnqueue []*testElement
		expected  *testElement
		// Expected size of queue in bytes after a single Dequeue call
		expByteSize int
		// Expected length after a single Dequeue call
		expLen int
	}{
		{
			name:     "one in, one out",
			maxBytes: 10,
			toEnqueue: []*testElement{
				element1,
			},
			expected:    element1,
			expByteSize: 0,
			expLen:      0,
		},
		{
			name:     "two in, one out",
			maxBytes: 20,
			toEnqueue: []*testElement{
				element1,
				element2,
			},
			expected:    element1,
			expByteSize: 10,
			expLen:      1,
		},
		{
			name:     "three in, max bytes reached, one out",
			maxBytes: 20,
			toEnqueue: []*testElement{
				element1,
				element2,
				element3,
			},
			expected:    element1,
			expByteSize: 10,
			expLen:      1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := NewMemoryQueue[*testElement](tc.maxBytes, sizeFn, "enqueue_test")
			for _, el := range tc.toEnqueue {
				// Expected returns of Enqueue are tested separately.
				_ = q.Enqueue(el)
			}
			actual := q.Dequeue()
			require.Equalf(t, q.Len(), tc.expLen, "unexpected queue length")
			require.Equal(t, tc.expected, actual)
			func() {
				q.mu.Lock()
				defer q.mu.Unlock()
				require.Equalf(t, q.mu.curSize, tc.expByteSize, "unexpected size in bytes")
			}()
		})
	}
}

type testElement struct {
	size int
}

func newTestEl(size int) *testElement {
	return &testElement{size: size}
}

func (t *testElement) Size() int {
	return t.size
}

var _ proto.Sizer = (*testElement)(nil)
