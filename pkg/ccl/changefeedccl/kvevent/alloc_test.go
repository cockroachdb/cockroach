// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvevent

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

type testAllocPool struct {
	syncutil.Mutex
	n int64
}

// Release implements kvevent.pool interface.
func (ap *testAllocPool) Release(ctx context.Context, bytes, entries int64) {
	ap.Lock()
	defer ap.Unlock()
	if ap.n == 0 {
		panic("can't release zero resources")
	}
	ap.n -= bytes
}

func (ap *testAllocPool) alloc() Alloc {
	ap.Lock()
	defer ap.Unlock()
	ap.n++
	return TestingMakeAlloc(1, ap)
}

func TestAllocMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	p1 := &testAllocPool{}
	p2 := &testAllocPool{}
	p3 := &testAllocPool{}

	p1main := p1.alloc()
	p1other := p1.alloc()

	p2alloc := p2.alloc()

	p3main := p3.alloc()
	p3other := p3.alloc()

	// Merge together p3 allocs.
	p3main.Merge(&p3other)
	require.EqualValues(t, 2, p3main.entries)
	require.EqualValues(t, 0, p3other.entries)

	// Merge p2 alloc into p3
	p3main.Merge(&p2alloc)
	require.EqualValues(t, 0, p2alloc.entries)
	require.Equal(t, 1, len(p3main.otherPoolAllocs))

	// Merge p3 allocs into p1other.
	p1other.Merge(&p3main)
	require.EqualValues(t, 0, p3main.entries)
	require.Nil(t, p3main.otherPoolAllocs)
	require.Equal(t, 1, len(p1other.otherPoolAllocs))

	// Merge p1 allocs together.
	p1main.Merge(&p1other)

	require.EqualValues(t, 2, p1.n)
	require.EqualValues(t, 1, p2.n)
	require.EqualValues(t, 2, p3.n)

	// Releasing main alloc should release all other allocs.
	p1main.Release(context.Background())

	require.EqualValues(t, 0, p1.n)
	require.EqualValues(t, 0, p2.n)
	require.EqualValues(t, 0, p3.n)
}
