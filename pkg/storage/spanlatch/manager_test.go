// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package spanlatch

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

var read = false
var write = true
var zeroTS = hlc.Timestamp{}

func spans(from, to string, write bool) *spanset.SpanSet {
	var span roachpb.Span
	if to == "" {
		span = roachpb.Span{Key: roachpb.Key(from)}
	} else {
		span = roachpb.Span{Key: roachpb.Key(from), EndKey: roachpb.Key(to)}
	}
	if strings.HasPrefix(from, "local") {
		span.Key = append(keys.LocalRangePrefix, span.Key...)
		if span.EndKey != nil {
			span.EndKey = append(keys.LocalRangePrefix, span.EndKey...)
		}
	}
	var spans spanset.SpanSet
	access := spanset.SpanReadOnly
	if write {
		access = spanset.SpanReadWrite
	}
	spans.Add(access, span)
	return &spans
}

func testLatchSucceeds(t *testing.T, lgC <-chan *Guard) *Guard {
	t.Helper()
	select {
	case lg := <-lgC:
		return lg
	case <-time.After(15 * time.Millisecond):
		t.Fatal("latch acquisition should succeed")
	}
	return nil
}

func testLatchBlocks(t *testing.T, lgC <-chan *Guard) {
	t.Helper()
	select {
	case <-lgC:
		t.Fatal("latch acquisition should block")
	case <-time.After(3 * time.Millisecond):
	}
}

// MustAcquire is like Acquire, except it can't return context cancellation
// errors.
func (m *Manager) MustAcquire(spans *spanset.SpanSet, ts hlc.Timestamp) *Guard {
	lg, err := m.Acquire(context.Background(), spans, ts)
	if err != nil {
		panic(err)
	}
	return lg
}

// MustAcquireCh is like Acquire, except it only sequences the latch latch
// attempt synchronously and waits on dependent latches asynchronously. It
// returns a channel that provides the Guard when the latches are acquired (i.e.
// after waiting). If the context expires, a nil Guard will be delivered on the
// channel.
func (m *Manager) MustAcquireCh(spans *spanset.SpanSet, ts hlc.Timestamp) <-chan *Guard {
	return m.MustAcquireChCtx(context.Background(), spans, ts)
}

// MustAcquireChCtx is like MustAcquireCh, except it accepts a context.
func (m *Manager) MustAcquireChCtx(
	ctx context.Context, spans *spanset.SpanSet, ts hlc.Timestamp,
) <-chan *Guard {
	ch := make(chan *Guard)
	lg, snap := m.sequence(spans, ts)
	go func() {
		err := m.wait(ctx, lg, ts, snap)
		if err != nil {
			m.Release(lg)
			lg = nil
		}
		ch <- lg
	}()
	return ch
}

func TestLatchManager(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Try latch with no overlapping already-acquired lathes.
	lg1 := m.MustAcquire(spans("a", "", write), zeroTS)
	m.Release(lg1)

	lg2 := m.MustAcquire(spans("a", "b", write), zeroTS)
	m.Release(lg2)

	// Add a latch and verify overlapping latches wait on it.
	lg3 := m.MustAcquire(spans("a", "b", write), zeroTS)
	lg4C := m.MustAcquireCh(spans("a", "b", write), zeroTS)

	// Second write should block.
	testLatchBlocks(t, lg4C)

	// First write completes, second grabs latch.
	m.Release(lg3)
	testLatchSucceeds(t, lg4C)
}

func TestLatchManagerNoWaitOnReadOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Acquire latch for read-only span.
	m.MustAcquire(spans("a", "", read), zeroTS)

	// Verify no wait with another read-only span.
	m.MustAcquire(spans("a", "", read), zeroTS)
}

func TestLatchManagerWriteWaitForMultipleReads(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Acquire latch for read-only span.
	lg1 := m.MustAcquire(spans("a", "", read), zeroTS)
	// Acquire another one on top.
	lg2 := m.MustAcquire(spans("a", "", read), zeroTS)

	// A write span should have to wait for **both** reads.
	lg3C := m.MustAcquireCh(spans("a", "", write), zeroTS)

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
	lg1C := m.MustAcquireCh(spans("a", "", write), zeroTS)
	lg2C := m.MustAcquireCh(spans("b", "c", write), zeroTS)
	lg3C := m.MustAcquireCh(spans("a", "d", write), zeroTS)

	// Attempt to acquire latch which overlaps them all.
	lg4C := m.MustAcquireCh(spans("0", "z", write), zeroTS)
	testLatchBlocks(t, lg4C)
	m.Release(<-lg1C)
	testLatchBlocks(t, lg4C)
	m.Release(<-lg2C)
	testLatchBlocks(t, lg4C)
	m.Release(<-lg3C)
	testLatchSucceeds(t, lg4C)
}

func TestLatchManagerMultipleOverlappingSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Acquire multiple latches.
	lg1 := m.MustAcquire(spans("a", "", write), zeroTS)
	lg2 := m.MustAcquire(spans("b", "c", read), zeroTS)
	lg3 := m.MustAcquire(spans("d", "f", write), zeroTS)
	lg4 := m.MustAcquire(spans("g", "", write), zeroTS)

	// Attempt to acquire latches overlapping each of them.
	var spans spanset.SpanSet
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("a")})
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("b")})
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("e")})
	lg5C := m.MustAcquireCh(&spans, zeroTS)

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
		ts1       hlc.Timestamp
		sp2       *spanset.SpanSet
		ts2       hlc.Timestamp
		dependent bool
	}{
		{
			name:      "point writes, same key",
			sp1:       spans("a", "", write),
			sp2:       spans("a", "", write),
			dependent: true,
		},
		{
			name:      "point writes, different key",
			sp1:       spans("a", "", write),
			sp2:       spans("b", "", write),
			dependent: false,
		},
		{
			name:      "range writes, overlapping span",
			sp1:       spans("a", "c", write),
			sp2:       spans("b", "d", write),
			dependent: true,
		},
		{
			name:      "range writes, non-overlapping span",
			sp1:       spans("a", "b", write),
			sp2:       spans("b", "c", write),
			dependent: false,
		},
		{
			name:      "point reads, same key",
			sp1:       spans("a", "", read),
			sp2:       spans("a", "", read),
			dependent: false,
		},
		{
			name:      "point reads, different key",
			sp1:       spans("a", "", read),
			sp2:       spans("b", "", read),
			dependent: false,
		},
		{
			name:      "range reads, overlapping span",
			sp1:       spans("a", "c", read),
			sp2:       spans("b", "d", read),
			dependent: false,
		},
		{
			name:      "range reads, non-overlapping span",
			sp1:       spans("a", "b", read),
			sp2:       spans("b", "c", read),
			dependent: false,
		},
		{
			name:      "read and write, same ts",
			sp1:       spans("a", "", write),
			ts1:       hlc.Timestamp{WallTime: 1},
			sp2:       spans("a", "", read),
			ts2:       hlc.Timestamp{WallTime: 1},
			dependent: true,
		},
		{
			name:      "read and write, causal ts",
			sp1:       spans("a", "", write),
			ts1:       hlc.Timestamp{WallTime: 1},
			sp2:       spans("a", "", read),
			ts2:       hlc.Timestamp{WallTime: 2},
			dependent: true,
		},
		{
			name:      "read and write, non-causal ts",
			sp1:       spans("a", "", write),
			ts1:       hlc.Timestamp{WallTime: 2},
			sp2:       spans("a", "", read),
			ts2:       hlc.Timestamp{WallTime: 1},
			dependent: false,
		},
		{
			name:      "read and write, zero ts read",
			sp1:       spans("a", "", write),
			ts1:       hlc.Timestamp{WallTime: 1},
			sp2:       spans("a", "", read),
			ts2:       hlc.Timestamp{WallTime: 0},
			dependent: true,
		},
		{
			name:      "read and write, zero ts write",
			sp1:       spans("a", "", write),
			ts1:       hlc.Timestamp{WallTime: 0},
			sp2:       spans("a", "", read),
			ts2:       hlc.Timestamp{WallTime: 1},
			dependent: true,
		},
		{
			name:      "read and write, non-overlapping",
			sp1:       spans("a", "b", write),
			sp2:       spans("b", "", read),
			dependent: false,
		},
		{
			name:      "local range writes, overlapping span",
			sp1:       spans("local a", "local c", write),
			sp2:       spans("local b", "local d", write),
			dependent: true,
		},
		{
			name:      "local range writes, non-overlapping span",
			sp1:       spans("local a", "local b", write),
			sp2:       spans("local b", "local c", write),
			dependent: false,
		},
		{
			name:      "local range reads, overlapping span",
			sp1:       spans("local a", "local c", read),
			sp2:       spans("local b", "local d", read),
			dependent: false,
		},
		{
			name:      "local range reads, non-overlapping span",
			sp1:       spans("local a", "local b", read),
			sp2:       spans("local b", "local c", read),
			dependent: false,
		},
		{
			name:      "local read and write, same ts",
			sp1:       spans("local a", "", write),
			ts1:       hlc.Timestamp{WallTime: 1},
			sp2:       spans("local a", "", read),
			ts2:       hlc.Timestamp{WallTime: 1},
			dependent: true,
		},
		{
			name:      "local read and write, causal ts",
			sp1:       spans("local a", "", write),
			ts1:       hlc.Timestamp{WallTime: 1},
			sp2:       spans("local a", "", read),
			ts2:       hlc.Timestamp{WallTime: 2},
			dependent: true,
		},
		{
			name:      "local read and write, non-causal ts",
			sp1:       spans("local a", "", write),
			ts1:       hlc.Timestamp{WallTime: 2},
			sp2:       spans("local a", "", read),
			ts2:       hlc.Timestamp{WallTime: 1},
			dependent: true,
		},
		{
			name:      "local read and write, zero ts read",
			sp1:       spans("local a", "", write),
			ts1:       hlc.Timestamp{WallTime: 1},
			sp2:       spans("local a", "", read),
			ts2:       hlc.Timestamp{WallTime: 0},
			dependent: true,
		},
		{
			name:      "local read and write, zero ts write",
			sp1:       spans("local a", "", write),
			ts1:       hlc.Timestamp{WallTime: 0},
			sp2:       spans("local a", "", read),
			ts2:       hlc.Timestamp{WallTime: 1},
			dependent: true,
		},
		{
			name:      "local read and write, non-overlapping",
			sp1:       spans("a", "b", write),
			sp2:       spans("b", "", read),
			dependent: false,
		},
		{
			name:      "local read and global write, overlapping",
			sp1:       spans("a", "b", write),
			sp2:       spans("local b", "", read),
			dependent: false,
		},
		{
			name:      "local write and global read, overlapping",
			sp1:       spans("local a", "local b", write),
			sp2:       spans("b", "", read),
			dependent: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "inv", func(t *testing.T, inv bool) {
				c := c
				if inv {
					c.sp1, c.sp2 = c.sp2, c.sp1
					c.ts1, c.ts2 = c.ts2, c.ts1
				}

				var m Manager
				lg1 := m.MustAcquire(c.sp1, c.ts1)
				lg2C := m.MustAcquireCh(c.sp2, c.ts2)
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

func TestLatchManagerContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m Manager

	// Attempt to acquire three latches that all block on each other.
	lg1 := m.MustAcquire(spans("a", "", write), zeroTS)
	// The second one is given a cancelable context.
	ctx2, cancel2 := context.WithCancel(context.Background())
	lg2C := m.MustAcquireChCtx(ctx2, spans("a", "", write), zeroTS)
	lg3C := m.MustAcquireCh(spans("a", "", write), zeroTS)

	// The second and third latch attempt block on the first.
	testLatchBlocks(t, lg2C)
	testLatchBlocks(t, lg3C)

	// Cancel the second acquisition's context. It should stop waiting.
	cancel2()
	require.Nil(t, <-lg2C)

	// The third latch attempt still blocks.
	testLatchBlocks(t, lg3C)

	// Release the first latch. The third succeeds in acquiring the latch.
	m.Release(lg1)
	testLatchSucceeds(t, lg3C)
}

func BenchmarkLatchManagerReadOnlyMix(b *testing.B) {
	for _, size := range []int{1, 4, 16, 64, 128, 256} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			var m Manager
			ss := spans("a", "b", read)
			for i := 0; i < size; i++ {
				_ = m.MustAcquire(ss, zeroTS)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = m.MustAcquire(ss, zeroTS)
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
				span := roachpb.Span{
					Key:    roachpb.Key(a),
					EndKey: roachpb.Key(b),
				}
				access := spanset.SpanReadOnly
				if i%(readsPerWrite+1) == 0 {
					access = spanset.SpanReadWrite
				}
				spans[i].Add(access, span)
			}

			b.ResetTimer()
			for i := range spans {
				lg, snap := m.sequence(&spans[i], zeroTS)
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
