// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package heaputil_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/container/heaputil"
	"github.com/stretchr/testify/require"
)

func TestValid(t *testing.T) {
	for name, tc := range map[string]struct {
		h     intHeap
		valid bool
	}{
		"nil heap": {
			h:     nil,
			valid: true,
		},
		"empty heap": {
			h:     []int{},
			valid: true,
		},
		"sorted heap": {
			h:     []int{1, 2, 3, 4, 5, 6},
			valid: true,
		},
		"valid heap": {
			h:     []int{2, 4, 6, 5, 9, 7},
			valid: true,
		},
		"invalid heap due to left child": {
			h:     []int{5, 1, 7},
			valid: false,
		},
		"invalid heap due to right child": {
			h:     []int{4, 6, 1},
			valid: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual := heaputil.Valid(&tc.h)
			require.Equal(t, tc.valid, actual)
		})
	}
}

type intHeap []int

var _ heap.Interface[int] = (*intHeap)(nil)

func (h intHeap) Len() int           { return len(h) }
func (h intHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h intHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *intHeap) Push(x int) {
	*h = append(*h, x)
}

func (h *intHeap) Pop() int {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
