// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanlatch

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/poison"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var read = false
var write = true
var zeroTS = hlc.Timestamp{}

func spans(from, to string, write bool, ts hlc.Timestamp) *spanset.SpanSet {
	var spanSet spanset.SpanSet
	add(&spanSet, from, to, write, ts)
	return &spanSet
}

func add(spanSet *spanset.SpanSet, from, to string, write bool, ts hlc.Timestamp) {
	var start, end roachpb.Key
	if to == "" {
		start = roachpb.Key(from)
	} else {
		start = roachpb.Key(from)
		end = roachpb.Key(to)
	}
	if strings.HasPrefix(from, "local") {
		start = append(keys.LocalRangePrefix, start...)
		if end != nil {
			end = append(keys.LocalRangePrefix, end...)
		}
	}
	access := spanset.SpanReadOnly
	if write {
		access = spanset.SpanReadWrite
	}

	if strings.HasPrefix(from, "local") {
		spanSet.AddNonMVCC(access, roachpb.Span{Key: start, EndKey: end})
	} else {
		spanSet.AddMVCC(access, roachpb.Span{Key: start, EndKey: end}, ts)
	}
}

func testLatchSucceeds(t *testing.T, a Attempt) *Guard {
	t.Helper()
	select {
	case err := <-a.errCh:
		require.NoError(t, err)
		return a.lg
	case <-time.After(testutils.DefaultSucceedsSoonDuration):
		// False positives are not ok, so we use a more
		// conservative timeout than in testLatchBlocks.
		t.Fatal("latch acquisition should succeed")
	}
	return nil
}

func testLatchBlocks(t *testing.T, a Attempt) {
	t.Helper()
	select {
	case <-a.errCh:
		t.Fatal("latch acquisition should block")
	case <-time.After(3 * time.Millisecond):
		// False positives are ok as long as they are rare, so we
		// use an aggressive timeout to avoid slowing down tests.
	}
}

// MustAcquire is like Acquire, except it can't return context cancellation
// errors.
func (m *Manager) MustAcquire(spans *spanset.SpanSet) *Guard {
	lg, err := m.Acquire(context.Background(), spans, poison.Policy_Error)
	if err != nil {
		panic(err)
	}
	return lg
}

// Attempt is a testing helper returned from MustAcquireCh.
type Attempt struct {
	lg    *Guard
	errCh <-chan error
}

// MustAcquireCh is like Acquire, except it only sequences the latch attempt
// synchronously and waits on dependent latches asynchronously. It returns an
// `Attempt` helper whose error channel is signaled with the result of
// sequencing. Use MustAcquireChExt when testing context cancellation or
// poisoning.
func (m *Manager) MustAcquireCh(spans *spanset.SpanSet) Attempt {
	return m.MustAcquireChExt(context.Background(), spans, poison.Policy_Error)
}

// MustAcquireChExt is like MustAcquireCh, except it accepts a context and
// poison.Policy.
func (m *Manager) MustAcquireChExt(
	ctx context.Context, spans *spanset.SpanSet, pp poison.Policy,
) Attempt {
	errCh := make(chan error, 1)
	lg, snap := m.sequence(spans, pp)
	go func() {
		err := m.wait(ctx, lg, snap)
		if err != nil {
			m.Release(lg)
		}
		errCh <- err
	}()
	return Attempt{lg: lg, errCh: errCh}
}

func TestLatchManager(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Try latches with no overlapping already-acquired latches.
	lg1 := m.MustAcquire(spans("a", "", write, zeroTS))
	m.Release(lg1)

	lg2 := m.MustAcquire(spans("a", "b", write, zeroTS))
	m.Release(lg2)

	// Add a latch and verify overlapping latches wait on it.
	lg3 := m.MustAcquire(spans("a", "b", write, zeroTS))
	lg4C := m.MustAcquireCh(spans("a", "b", write, zeroTS))

	// Second write should block.
	testLatchBlocks(t, lg4C)

	// First write completes, second grabs latch.
	m.Release(lg3)
	testLatchSucceeds(t, lg4C)
}

func TestLatchManagerAcquireOverlappingSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Acquire overlapping latches with different access patterns.
	//    |----------|        <- Read latch [a-c)@t1
	//        |----------|    <- Write latch [b-d)@t1
	//
	//    ^   ^      ^   ^
	//    |   |      |   |
	//    a   b      c   d
	//
	var ts0, ts1 = hlc.Timestamp{WallTime: 0}, hlc.Timestamp{WallTime: 1}
	var spanSet spanset.SpanSet
	add(&spanSet, "a", "c", read, ts1)
	add(&spanSet, "b", "d", write, ts1)
	lg1 := m.MustAcquire(&spanSet)

	lg2C := m.MustAcquireCh(spans("a", "b", read, ts0))
	lg2 := testLatchSucceeds(t, lg2C)
	m.Release(lg2)

	// We acquire reads at lower timestamps than writes to check for blocked
	// acquisitions based on the original latch, not the latches declared in
	// earlier test cases.
	var attempts []Attempt
	attempts = append(attempts, m.MustAcquireCh(spans("a", "b", write, ts1)))
	attempts = append(attempts, m.MustAcquireCh(spans("b", "c", read, ts0)))
	attempts = append(attempts, m.MustAcquireCh(spans("b", "c", write, ts1)))
	attempts = append(attempts, m.MustAcquireCh(spans("c", "d", write, ts1)))
	attempts = append(attempts, m.MustAcquireCh(spans("c", "d", read, ts0)))

	for _, lgC := range attempts {
		testLatchBlocks(t, lgC)
	}

	m.Release(lg1)

	for _, lgC := range attempts {
		lg := testLatchSucceeds(t, lgC)
		m.Release(lg)
	}
}

func TestLatchManagerAcquiringReadsVaryingTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	var ts0, ts1 = hlc.Timestamp{WallTime: 0}, hlc.Timestamp{WallTime: 1}
	var spanSet spanset.SpanSet
	add(&spanSet, "a", "", read, ts0)
	add(&spanSet, "a", "", read, ts1)
	lg1 := m.MustAcquire(&spanSet)

	for _, walltime := range []int64{0, 1, 2} {
		ts := hlc.Timestamp{WallTime: walltime}
		lg := testLatchSucceeds(t, m.MustAcquireCh(spans("a", "", read, ts)))
		m.Release(lg)
	}

	var attempts []Attempt
	for _, walltime := range []int64{0, 1, 2} {
		ts := hlc.Timestamp{WallTime: walltime}
		attempts = append(attempts, m.MustAcquireCh(spans("a", "", write, ts)))
	}

	for _, lgC := range attempts {
		testLatchBlocks(t, lgC)
	}

	m.Release(lg1)

	for _, lgC := range attempts {
		lg := testLatchSucceeds(t, lgC)
		m.Release(lg)
	}
}

func TestLatchManagerNoWaitOnReadOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Acquire latch for read-only span.
	m.MustAcquire(spans("a", "", read, zeroTS))

	// Verify no wait with another read-only span.
	m.MustAcquire(spans("a", "", read, zeroTS))
}

func TestLatchManagerWriteWaitForMultipleReads(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Acquire latch for read-only span.
	lg1 := m.MustAcquire(spans("a", "", read, zeroTS))
	// Acquire another one on top.
	lg2 := m.MustAcquire(spans("a", "", read, zeroTS))

	// A write span should have to wait for **both** reads.
	lg3C := m.MustAcquireCh(spans("a", "", write, zeroTS))

	// Certainly blocks now.
	testLatchBlocks(t, lg3C)

	// The second read releases latch, but the first one remains.
	m.Release(lg2)

	// Should still block.
	testLatchBlocks(t, lg3C)

	// First read releases latch.
	m.Release(lg1)

	// Now it goes through.
	testLatchSucceeds(t, lg3C)
}

func TestLatchManagerMultipleOverlappingLatches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Acquire multiple latches.
	a1 := m.MustAcquireCh(spans("a", "", write, zeroTS))
	a2 := m.MustAcquireCh(spans("b", "c", write, zeroTS))
	a3 := m.MustAcquireCh(spans("a", "d", write, zeroTS))

	// Attempt to acquire latch which overlaps them all.
	lg4C := m.MustAcquireCh(spans("0", "z", write, zeroTS))
	testLatchBlocks(t, lg4C)
	m.Release(testLatchSucceeds(t, a1))
	testLatchBlocks(t, lg4C)
	m.Release(testLatchSucceeds(t, a2))
	testLatchBlocks(t, lg4C)
	m.Release(testLatchSucceeds(t, a3))
	testLatchSucceeds(t, lg4C)
}

func TestLatchManagerMultipleOverlappingSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Acquire multiple latches.
	lg1 := m.MustAcquire(spans("a", "", write, zeroTS))
	lg2 := m.MustAcquire(spans("b", "c", read, zeroTS))
	lg3 := m.MustAcquire(spans("d", "f", write, zeroTS))
	lg4 := m.MustAcquire(spans("g", "", write, zeroTS))

	// Attempt to acquire latches overlapping each of them.
	var spans spanset.SpanSet
	spans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("a")})
	spans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("b")})
	spans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("e")})
	lg5C := m.MustAcquireCh(&spans)

	// Blocks until the first three prerequisite latches release.
	testLatchBlocks(t, lg5C)
	m.Release(lg2)
	testLatchBlocks(t, lg5C)
	m.Release(lg3)
	testLatchBlocks(t, lg5C)
	m.Release(lg1)
	lg5 := testLatchSucceeds(t, lg5C)
	m.Release(lg4)
	m.Release(lg5)
}

func TestLatchManagerDependentLatches(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		name      string
		sp1       *spanset.SpanSet
		sp2       *spanset.SpanSet
		dependent bool
	}{
		{
			name:      "point writes, same key",
			sp1:       spans("a", "", write, zeroTS),
			sp2:       spans("a", "", write, zeroTS),
			dependent: true,
		},
		{
			name:      "point writes, different key",
			sp1:       spans("a", "", write, zeroTS),
			sp2:       spans("b", "", write, zeroTS),
			dependent: false,
		},
		{
			name:      "range writes, overlapping span",
			sp1:       spans("a", "c", write, zeroTS),
			sp2:       spans("b", "d", write, zeroTS),
			dependent: true,
		},
		{
			name:      "range writes, non-overlapping span",
			sp1:       spans("a", "b", write, zeroTS),
			sp2:       spans("b", "c", write, zeroTS),
			dependent: false,
		},
		{
			name:      "point reads, same key",
			sp1:       spans("a", "", read, zeroTS),
			sp2:       spans("a", "", read, zeroTS),
			dependent: false,
		},
		{
			name:      "point reads, different key",
			sp1:       spans("a", "", read, zeroTS),
			sp2:       spans("b", "", read, zeroTS),
			dependent: false,
		},
		{
			name:      "range reads, overlapping span",
			sp1:       spans("a", "c", read, zeroTS),
			sp2:       spans("b", "d", read, zeroTS),
			dependent: false,
		},
		{
			name:      "range reads, non-overlapping span",
			sp1:       spans("a", "b", read, zeroTS),
			sp2:       spans("b", "c", read, zeroTS),
			dependent: false,
		},
		{
			name:      "read and write, same ts",
			sp1:       spans("a", "", write, hlc.Timestamp{WallTime: 1}),
			sp2:       spans("a", "", read, hlc.Timestamp{WallTime: 1}),
			dependent: true,
		},
		{
			name:      "read and write, causal ts",
			sp1:       spans("a", "", write, hlc.Timestamp{WallTime: 1}),
			sp2:       spans("a", "", read, hlc.Timestamp{WallTime: 2}),
			dependent: true,
		},
		{
			name:      "read and write, non-causal ts",
			sp1:       spans("a", "", write, hlc.Timestamp{WallTime: 2}),
			sp2:       spans("a", "", read, hlc.Timestamp{WallTime: 1}),
			dependent: false,
		},
		{
			name:      "read and write, zero ts read",
			sp1:       spans("a", "", write, hlc.Timestamp{WallTime: 1}),
			sp2:       spans("a", "", read, hlc.Timestamp{WallTime: 0}),
			dependent: true,
		},
		{
			name:      "point reads, different ts",
			sp1:       spans("a", "", read, hlc.Timestamp{WallTime: 1}),
			sp2:       spans("a", "", read, hlc.Timestamp{WallTime: 0}),
			dependent: false,
		},
		{
			name:      "read and write, zero ts write",
			sp1:       spans("a", "", write, hlc.Timestamp{WallTime: 0}),
			sp2:       spans("a", "", read, hlc.Timestamp{WallTime: 1}),
			dependent: true,
		},
		{
			name:      "read and write, non-overlapping",
			sp1:       spans("a", "b", write, zeroTS),
			sp2:       spans("b", "", read, zeroTS),
			dependent: false,
		},
		{
			name:      "local range writes, overlapping span",
			sp1:       spans("local a", "local c", write, zeroTS),
			sp2:       spans("local b", "local d", write, zeroTS),
			dependent: true,
		},
		{
			name:      "local range writes, non-overlapping span",
			sp1:       spans("local a", "local b", write, zeroTS),
			sp2:       spans("local b", "local c", write, zeroTS),
			dependent: false,
		},
		{
			name:      "local range reads, overlapping span",
			sp1:       spans("local a", "local c", read, zeroTS),
			sp2:       spans("local b", "local d", read, zeroTS),
			dependent: false,
		},
		{
			name:      "local range reads, non-overlapping span",
			sp1:       spans("local a", "local b", read, zeroTS),
			sp2:       spans("local b", "local c", read, zeroTS),
			dependent: false,
		},
		{
			name:      "local read and write, same ts",
			sp1:       spans("local a", "", write, hlc.Timestamp{WallTime: 1}),
			sp2:       spans("local a", "", read, hlc.Timestamp{WallTime: 1}),
			dependent: true,
		},
		{
			name:      "local read and write, causal ts",
			sp1:       spans("local a", "", write, hlc.Timestamp{WallTime: 1}),
			sp2:       spans("local a", "", read, hlc.Timestamp{WallTime: 2}),
			dependent: true,
		},
		{
			name:      "local read and write, non-causal ts",
			sp1:       spans("local a", "", write, hlc.Timestamp{WallTime: 2}),
			sp2:       spans("local a", "", read, hlc.Timestamp{WallTime: 1}),
			dependent: true,
		},
		{
			name:      "local read and write, zero ts read",
			sp1:       spans("local a", "", write, hlc.Timestamp{WallTime: 1}),
			sp2:       spans("local a", "", read, hlc.Timestamp{WallTime: 0}),
			dependent: true,
		},
		{
			name:      "local read and write, zero ts write",
			sp1:       spans("local a", "", write, hlc.Timestamp{WallTime: 0}),
			sp2:       spans("local a", "", read, hlc.Timestamp{WallTime: 1}),
			dependent: true,
		},
		{
			name:      "local read and write, non-overlapping",
			sp1:       spans("a", "b", write, zeroTS),
			sp2:       spans("b", "", read, zeroTS),
			dependent: false,
		},
		{
			name:      "local read and global write, overlapping",
			sp1:       spans("a", "b", write, zeroTS),
			sp2:       spans("local b", "", read, zeroTS),
			dependent: false,
		},
		{
			name:      "local write and global read, overlapping",
			sp1:       spans("local a", "local b", write, zeroTS),
			sp2:       spans("b", "", read, zeroTS),
			dependent: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "inv", func(t *testing.T, inv bool) {
				c := c
				if inv {
					c.sp1, c.sp2 = c.sp2, c.sp1
				}

				var m Manager
				lg1 := m.MustAcquire(c.sp1)
				lg2C := m.MustAcquireCh(c.sp2)
				if c.dependent {
					testLatchBlocks(t, lg2C)
					m.Release(lg1)
					lg2 := testLatchSucceeds(t, lg2C)
					m.Release(lg2)
				} else {
					lg2 := testLatchSucceeds(t, lg2C)
					m.Release(lg1)
					m.Release(lg2)
				}
			})
		})
	}
}

func TestLatchManagerPoison(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	testLatchPoisons := func(t *testing.T, a Attempt) {
		t.Helper()
		select {
		case err := <-a.errCh:
			require.True(t, errors.HasType(err, (*poison.PoisonedError)(nil)), "%+v", err)
		case <-time.After(testutils.DefaultSucceedsSoonDuration):
			t.Fatal("timed out")
		}
	}

	var m Manager
	a1 := m.MustAcquireChExt(ctx, spans("a", "", write, zeroTS), poison.Policy_Wait)
	a2 := m.MustAcquireChExt(ctx, spans("a", "", write, zeroTS), poison.Policy_Error)
	a3 := m.MustAcquireChExt(ctx, spans("a", "", write, zeroTS), poison.Policy_Error)
	a4 := m.MustAcquireChExt(ctx, spans("a", "", write, zeroTS), poison.Policy_Wait)
	a5 := m.MustAcquireChExt(ctx, spans("a", "", write, zeroTS), poison.Policy_Error)

	ga1 := testLatchSucceeds(t, a1)

	// chga5 blocks on chga4 blocks on chga3 blocks on chga2 blocks on chga1.
	testLatchBlocks(t, a2)
	testLatchBlocks(t, a3)
	testLatchBlocks(t, a4)
	testLatchBlocks(t, a5)

	// Poison a1. This poisons a2, a3, a4, and a5. However, a4 is Policy_Wait so
	// we don't see that yet (but since a5 waits on a4 and a5 poisons, we are
	// pretty sure).
	ga1.poison.signal()

	testLatchPoisons(t, a2)
	testLatchPoisons(t, a3)
	testLatchPoisons(t, a5)

	// NB: we must use SucceedsSoon here & do this before we release ga1 below.
	// Otherwise, it is possible for a4 to never observe a poisoned latch during
	// sequencing.
	testutils.SucceedsSoon(t, func() error {
		if !a4.lg.poison.sig.signaled() {
			return errors.New("not signaled yet")
		}
		return nil
	})

	// Release ga1, which allows ga4 to sequence. At that point, we can check
	// directly that it is poisoned.
	m.Release(ga1)
	ga4 := testLatchSucceeds(t, a4)
	m.Release(ga4)
}

func TestLatchManagerContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Attempt to acquire three latches that all block on each other.
	lg1 := m.MustAcquire(spans("a", "", write, zeroTS))
	// The second one is given a cancelable context.
	ctx2, cancel2 := context.WithCancel(context.Background())
	a2C := m.MustAcquireChExt(ctx2, spans("a", "", write, zeroTS), poison.Policy_Error)
	a3C := m.MustAcquireCh(spans("a", "", write, zeroTS))

	// The second and third latch attempt block on the first.
	testLatchBlocks(t, a2C)
	testLatchBlocks(t, a3C)

	// Cancel the second acquisition's context. It should stop waiting.
	cancel2()
	require.ErrorIs(t, <-a2C.errCh, context.Canceled)

	// The third latch attempt still blocks.
	testLatchBlocks(t, a3C)

	// Release the first latch. The third succeeds in acquiring the latch.
	m.Release(lg1)
	testLatchSucceeds(t, a3C)
}

func TestLatchManagerOptimistic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Acquire latches, no conflict.
	lg1 := m.AcquireOptimistic(spans("d", "f", write, zeroTS), poison.Policy_Error)
	require.True(t, m.CheckOptimisticNoConflicts(lg1, spans("d", "f", write, zeroTS)), poison.Policy_Error)
	lg1, err := m.WaitUntilAcquired(context.Background(), lg1)
	require.NoError(t, err)

	// Optimistic acquire encounters conflict in some cases.
	lg2 := m.AcquireOptimistic(spans("a", "e", read, zeroTS), poison.Policy_Error)
	require.False(t, m.CheckOptimisticNoConflicts(lg2, spans("a", "e", read, zeroTS)))
	require.True(t, m.CheckOptimisticNoConflicts(lg2, spans("a", "d", read, zeroTS)))
	waitUntilAcquiredCh := func(g *Guard) Attempt {
		errCh := make(chan error, 1)
		go func() {
			_, err := m.WaitUntilAcquired(context.Background(), g)
			errCh <- err
		}()
		return Attempt{lg: g, errCh: errCh}
	}
	a2 := waitUntilAcquiredCh(lg2)
	testLatchBlocks(t, a2)
	m.Release(lg1)
	testLatchSucceeds(t, a2)

	// Optimistic acquire encounters conflict.
	lg3 := m.AcquireOptimistic(spans("a", "e", write, zeroTS), poison.Policy_Error)
	require.False(t, m.CheckOptimisticNoConflicts(lg3, spans("a", "e", write, zeroTS)))
	m.Release(lg2)
	// There is still a conflict even though lg2 has been released.
	require.False(t, m.CheckOptimisticNoConflicts(lg3, spans("a", "e", write, zeroTS)))
	lg3, err = m.WaitUntilAcquired(context.Background(), lg3)
	require.NoError(t, err)
	m.Release(lg3)

	// Optimistic acquire for read below write encounters no conflict.
	oneTS, twoTS := hlc.Timestamp{WallTime: 1}, hlc.Timestamp{WallTime: 2}
	lg4 := m.MustAcquire(spans("c", "e", write, twoTS))
	lg5 := m.AcquireOptimistic(spans("a", "e", read, oneTS), poison.Policy_Error)
	require.True(t, m.CheckOptimisticNoConflicts(lg5, spans("a", "e", read, oneTS)))
	require.True(t, m.CheckOptimisticNoConflicts(lg5, spans("a", "c", read, oneTS)))
	lg5, err = m.WaitUntilAcquired(context.Background(), lg5)
	require.NoError(t, err)
	m.Release(lg5)
	m.Release(lg4)
}

func TestLatchManagerWaitFor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Acquire latches, no conflict.
	lg1, err := m.Acquire(context.Background(), spans("d", "f", write, zeroTS), poison.Policy_Error)
	require.NoError(t, err)

	// See if WaitFor waits for above latch.
	waitForCh := func() Attempt {
		errCh := make(chan error)
		go func() {
			errCh <- m.WaitFor(context.Background(), spans("a", "e", read, zeroTS), poison.Policy_Error)
		}()
		return Attempt{errCh: errCh}
	}
	ch2 := waitForCh()
	testLatchBlocks(t, ch2)
	m.Release(lg1)
	testLatchSucceeds(t, ch2)

	// Optimistic acquire should _not_ encounter conflict - as WaitFor should
	// not lay any latches.
	lg3 := m.AcquireOptimistic(spans("a", "e", write, zeroTS), poison.Policy_Error)
	require.True(t, m.CheckOptimisticNoConflicts(lg3, spans("a", "e", write, zeroTS)))
	lg3, err = m.WaitUntilAcquired(context.Background(), lg3)
	require.NoError(t, err)
	m.Release(lg3)
}

func BenchmarkLatchManagerReadOnlyMix(b *testing.B) {
	for _, size := range []int{1, 4, 16, 64, 128, 256} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			var m Manager
			ss := spans("a", "b", read, zeroTS)
			for i := 0; i < size; i++ {
				_ = m.MustAcquire(ss)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = m.MustAcquire(ss)
			}
		})
	}
}

func BenchmarkLatchManagerReadWriteMix(b *testing.B) {
	for _, readsPerWrite := range []int{0, 1, 4, 16, 64, 128, 256} {
		b.Run(fmt.Sprintf("readsPerWrite=%d", readsPerWrite), func(b *testing.B) {
			var m Manager
			lgBuf := make(chan *Guard, 16)

			spans := make([]spanset.SpanSet, b.N)
			for i := range spans {
				a, b := randBytes(100), randBytes(100)
				// Overwrite first byte so that we do not mix local and global ranges
				a[0], b[0] = 'a', 'a'
				if bytes.Compare(a, b) > 0 {
					a, b = b, a
				}
				span := roachpb.Span{Key: a, EndKey: b}
				access := spanset.SpanReadOnly
				if i%(readsPerWrite+1) == 0 {
					access = spanset.SpanReadWrite
				}
				spans[i].AddNonMVCC(access, span)
			}

			b.ResetTimer()
			for i := range spans {
				lg, snap := m.sequence(&spans[i], poison.Policy_Error)
				snap.close()
				if len(lgBuf) == cap(lgBuf) {
					m.Release(<-lgBuf)
				}
				lgBuf <- lg
			}
		})
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}
