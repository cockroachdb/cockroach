// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package span

import (
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func entriesStr(f Frontier) string {
	var buf strings.Builder
	f.Entries(func(sp roachpb.Span, ts hlc.Timestamp) OpResult {
		if buf.Len() != 0 {
			buf.WriteString(` `)
		}
		fmt.Fprintf(&buf, `%s@%d`, sp, ts.WallTime)
		return ContinueMatch
	})
	return buf.String()
}

type frontierForwarder struct {
	t        *testing.T
	f        Frontier
	advanced bool
}

func (f frontierForwarder) expectedAdvanced(expected bool) frontierForwarder {
	require.Equal(f.t, expected, f.advanced, entriesStr(f.f))
	return f
}
func (f frontierForwarder) expectFrontier(wall int64) frontierForwarder {
	require.Equal(f.t, hlc.Timestamp{WallTime: wall}, f.f.Frontier(),
		"expected %d, found %s", wall, f.f.Frontier())
	return f
}
func (f frontierForwarder) expectEntries(expected string) frontierForwarder {
	require.Equal(f.t, expected, entriesStr(f.f))
	return f
}

func makeFrontierForwarded(
	t *testing.T, f Frontier,
) func(s roachpb.Span, wall int64) frontierForwarder {
	t.Helper()
	return func(s roachpb.Span, wall int64) frontierForwarder {
		advanced, err := f.Forward(s, hlc.Timestamp{WallTime: wall})
		require.NoError(t, err)
		return frontierForwarder{
			t:        t,
			f:        f,
			advanced: advanced,
		}
	}
}
func TestSpanFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "btree", func(t *testing.T, useBtreeFrontier bool) {
		defer enableBtreeFrontier(useBtreeFrontier)()

		keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
		keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

		spAB := roachpb.Span{Key: keyA, EndKey: keyB}
		spAC := roachpb.Span{Key: keyA, EndKey: keyC}
		spAD := roachpb.Span{Key: keyA, EndKey: keyD}
		spBC := roachpb.Span{Key: keyB, EndKey: keyC}
		spBD := roachpb.Span{Key: keyB, EndKey: keyD}
		spCD := roachpb.Span{Key: keyC, EndKey: keyD}

		f, err := MakeFrontier(spAD)
		require.NoError(t, err)
		require.Equal(t, hlc.Timestamp{}, f.Frontier())
		require.Equal(t, `{a-d}@0`, entriesStr(f))

		forwardFrontier := makeFrontierForwarded(t, f)

		// Untracked spans are ignored
		forwardFrontier(roachpb.Span{Key: []byte("d"), EndKey: []byte("e")}, 1).
			expectedAdvanced(false).
			expectFrontier(0).
			expectEntries(`{a-d}@0`)

		// Forward the entire tracked spanspace.
		forwardFrontier(spAD, 1).
			expectedAdvanced(true).
			expectFrontier(1).
			expectEntries(`{a-d}@1`)

		// Forward it again.
		forwardFrontier(spAD, 2).
			expectedAdvanced(true).
			expectFrontier(2).
			expectEntries(`{a-d}@2`)

		// Forward to the previous frontier.
		forwardFrontier(spAD, 2).
			expectedAdvanced(false).
			expectFrontier(2).
			expectEntries(`{a-d}@2`)

		// Forward into the past is ignored.
		forwardFrontier(spAD, 1).
			expectedAdvanced(false).
			expectFrontier(2).
			expectEntries(`{a-d}@2`)

		// Forward a subset.
		forwardFrontier(spBC, 3).
			expectedAdvanced(false).
			expectFrontier(2).
			expectEntries(`{a-b}@2 {b-c}@3 {c-d}@2`)

		// Forward it more.
		forwardFrontier(spBC, 4).
			expectedAdvanced(false).
			expectFrontier(2).
			expectEntries(`{a-b}@2 {b-c}@4 {c-d}@2`)

		// Forward all tracked spans to timestamp before BC (currently at 4).
		// Advances to the min of tracked spans. Note that this requires the
		// forwarded span to be split into two spans, one on each side of BC.
		forwardFrontier(spAD, 3).
			expectedAdvanced(true).
			expectFrontier(3).
			expectEntries(`{a-b}@3 {b-c}@4 {c-d}@3`)

		// Forward everything but BC, advances to the min of tracked spans.
		forwardFrontier(spAB, 5).
			expectedAdvanced(false).
			expectFrontier(3)

		forwardFrontier(spCD, 5).
			expectedAdvanced(true).
			expectFrontier(4).
			expectEntries(`{a-b}@5 {b-c}@4 {c-d}@5`)

		// Catch BC up: spans collapse.
		forwardFrontier(spBC, 5).
			expectedAdvanced(true).
			expectFrontier(5).
			expectEntries(`{a-d}@5`)

		// Forward them all at once.
		forwardFrontier(spAD, 6).
			expectedAdvanced(true).
			expectFrontier(6).
			expectEntries(`{a-d}@6`)

		// Split AC with BD.
		forwardFrontier(spCD, 7).
			expectedAdvanced(false).
			expectFrontier(6).
			expectEntries(`{a-c}@6 {c-d}@7`)

		forwardFrontier(spBD, 8).
			expectedAdvanced(false).
			expectFrontier(6).
			expectEntries(`{a-b}@6 {b-d}@8`)

		forwardFrontier(spAB, 8).
			expectedAdvanced(true).
			expectFrontier(8).
			expectEntries(`{a-d}@8`)

		// Split BD with AC.
		forwardFrontier(spAC, 9).
			expectedAdvanced(false).
			expectFrontier(8).
			expectEntries(`{a-c}@9 {c-d}@8`)

		forwardFrontier(spCD, 9).
			expectedAdvanced(true).
			expectFrontier(9).
			expectEntries(`{a-d}@9`)
	})
}

func TestSpanFrontierDisjointSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "btree", func(t *testing.T, useBtreeFrontier bool) {
		defer enableBtreeFrontier(useBtreeFrontier)()

		keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
		keyD, keyE, keyF := roachpb.Key("d"), roachpb.Key("e"), roachpb.Key("f")
		spAB := roachpb.Span{Key: keyA, EndKey: keyB}
		spAD := roachpb.Span{Key: keyA, EndKey: keyD}
		spCE := roachpb.Span{Key: keyC, EndKey: keyE}
		spDF := roachpb.Span{Key: keyD, EndKey: keyF}

		f, err := MakeFrontier(spAB, spCE)
		require.NoError(t, err)
		require.Equal(t, hlc.Timestamp{}, f.Frontier())
		require.Equal(t, `{a-b}@0 {c-e}@0`, entriesStr(f))

		forwardFrontier := makeFrontierForwarded(t, f)

		// Advance just the tracked spans
		forwardFrontier(spCE, 1).
			expectedAdvanced(false).
			expectFrontier(0).
			expectEntries(`{a-b}@0 {c-e}@1`)

		forwardFrontier(spAB, 1).
			expectedAdvanced(true).
			expectFrontier(1).
			expectEntries(`{a-b}@1 {c-e}@1`)

		// Advance a span that partially overlaps the tracked spans
		forwardFrontier(spDF, 2).
			expectedAdvanced(false).
			expectFrontier(1).
			expectEntries(`{a-b}@1 {c-d}@1 {d-e}@2`)

		// Advance one span that covers two tracked spans and so needs two entries.
		forwardFrontier(spAD, 3).
			expectedAdvanced(true).
			expectFrontier(2).
			expectEntries(`{a-b}@3 {c-d}@3 {d-e}@2`)

		// Advance span that overlaps all the spans tracked by this frontier.
		// {c-d} and {d-e} should collapse.
		forwardFrontier(roachpb.Span{Key: roachpb.Key(`0`), EndKey: roachpb.Key(`q`)}, 4).
			expectedAdvanced(true).
			expectFrontier(4).
			expectEntries(`{a-b}@4 {c-e}@4`)
	})
}

func TestSpanFrontierHeap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	spAB := roachpb.Span{Key: keyA, EndKey: keyB}
	spBC := roachpb.Span{Key: keyB, EndKey: keyC}

	var fh frontierHeap

	mkFrontierEntry := func(s roachpb.Span, wall int64) *btreeFrontierEntry {
		e := &btreeFrontierEntry{Start: s.Key, End: s.EndKey, ts: hlc.Timestamp{WallTime: wall}}
		return e
	}
	eAB1 := mkFrontierEntry(spAB, 1)
	eBC1 := mkFrontierEntry(spBC, 1)
	eAB2 := mkFrontierEntry(spAB, 2)

	// Push one
	heap.Push(&fh, eAB1)
	require.Equal(t, eAB1, heap.Pop(&fh))

	// Push different spans and times
	heap.Push(&fh, eAB1)
	heap.Push(&fh, eBC1)
	heap.Push(&fh, eAB2)
	require.Equal(t, eAB1, heap.Pop(&fh))
	require.Equal(t, eBC1, heap.Pop(&fh))
	require.Equal(t, eAB2, heap.Pop(&fh))

	// Push in a different span order
	heap.Push(&fh, eBC1)
	heap.Push(&fh, eAB1)
	heap.Push(&fh, eAB2)
	require.Equal(t, eAB1, heap.Pop(&fh))
	require.Equal(t, eBC1, heap.Pop(&fh))
	require.Equal(t, eAB2, heap.Pop(&fh))

	// Push in a different time order
	heap.Push(&fh, eAB2)
	heap.Push(&fh, eAB1)
	heap.Push(&fh, eBC1)
	require.Equal(t, eAB1, heap.Pop(&fh))
	require.Equal(t, eBC1, heap.Pop(&fh))
	require.Equal(t, eAB2, heap.Pop(&fh))
}

func TestSequentialSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "btree", func(t *testing.T, useBtreeFrontier bool) {
		defer enableBtreeFrontier(useBtreeFrontier)()

		f, err := MakeFrontier(roachpb.Span{Key: roachpb.Key("A"), EndKey: roachpb.Key("Z")})
		require.NoError(t, err)

		var expectedRanges []string
		for r := 'A'; r <= 'Z'-1; r++ {
			var sp roachpb.Span
			sp.Key = append(sp.Key, byte(r))
			sp.EndKey = append(sp.EndKey, byte(r+1))
			wall := r - 'A' + 1
			_, err := f.Forward(sp, hlc.Timestamp{WallTime: int64(wall)})
			require.NoError(t, err)
			expectedRanges = append(expectedRanges, fmt.Sprintf("%s@%d", sp, wall))
		}
		require.Equal(t, strings.Join(expectedRanges, " "), entriesStr(f))
	})
}

func makeSingleCharSpan(start, end byte) roachpb.Span {
	return roachpb.Span{Key: roachpb.Key{start}, EndKey: roachpb.Key{end}}
}

func makeSpan(start, end string) roachpb.Span {
	return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
}

func TestSpanEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	advance := func(f Frontier, s roachpb.Span, wall int64) {
		_, err := f.Forward(s, hlc.Timestamp{WallTime: wall})
		require.NoError(t, err)
	}

	spanEntries := func(f Frontier, sp roachpb.Span) string {
		var buf strings.Builder
		f.SpanEntries(sp, func(s roachpb.Span, ts hlc.Timestamp) OpResult {
			if buf.Len() != 0 {
				buf.WriteString(` `)
			}
			fmt.Fprintf(&buf, `%s@%d`, s, ts.WallTime)
			return ContinueMatch
		})
		return buf.String()
	}

	testutils.RunTrueAndFalse(t, "btree", func(t *testing.T, useBtreeFrontier bool) {
		defer enableBtreeFrontier(useBtreeFrontier)()

		t.Run("contiguous frontier", func(t *testing.T) {
			spAZ := makeSingleCharSpan('A', 'Z')
			f, err := MakeFrontier(spAZ)
			require.NoError(t, err)
			// Nothing overlaps span fully to the left of frontier.
			require.Equal(t, ``, spanEntries(f, makeSingleCharSpan('0', '9')))
			// Nothing overlaps span fully to the right of the frontier.
			require.Equal(t, ``, spanEntries(f, makeSingleCharSpan('a', 'z')))

			// Span overlaps entire frontier.
			require.Equal(t, `{A-Z}@0`, spanEntries(f, spAZ))
			advance(f, spAZ, 1)
			require.Equal(t, `{A-Z}@1`, spanEntries(f, spAZ))

			// Span overlaps part of the frontier, with left part outside frontier.
			require.Equal(t, `{A-C}@1`, spanEntries(f, makeSingleCharSpan('0', 'C')))

			// Span overlaps part of the frontier, with right part outside frontier.
			require.Equal(t, `{Q-Z}@1`, spanEntries(f, makeSingleCharSpan('Q', 'c')))

			// Span fully inside frontier.
			require.Equal(t, `{P-W}@1`, spanEntries(f, makeSingleCharSpan('P', 'W')))

			// Advance part of the frontier.
			advance(f, makeSingleCharSpan('C', 'E'), 2)
			advance(f, makeSingleCharSpan('H', 'M'), 5)
			advance(f, makeSingleCharSpan('N', 'Q'), 3)

			// Span overlaps various parts of the frontier.
			require.Equal(t,
				`{A-C}@1 {C-E}@2 {E-H}@1 {H-M}@5 {M-N}@1 {N-P}@3`,
				spanEntries(f, makeSingleCharSpan('3', 'P')))
		})

		t.Run("disjoint frontier", func(t *testing.T) {
			spAB := makeSingleCharSpan('A', 'B')
			spCE := makeSingleCharSpan('C', 'E')
			f, err := MakeFrontier(spAB, spCE)
			require.NoError(t, err)

			// Nothing overlaps between the two spans in the frontier.
			require.Equal(t, ``, spanEntries(f, makeSingleCharSpan('B', 'C')))

			// Overlap with only one entry in the frontier
			require.Equal(t, `{C-D}@0`, spanEntries(f, makeSingleCharSpan('B', 'D')))
		})
	})
}

func checkContiguousFrontier(f Frontier) (startKey, endKey []byte, retErr error) {
	// Iterate frontier to make sure it is sane.
	prev := struct {
		s  roachpb.Span
		ts hlc.Timestamp
	}{}

	frontierSpan := f.PeekFrontierSpan()
	frontierTS := f.Frontier()
	sawFrontierSpan := false

	f.Entries(func(s roachpb.Span, ts hlc.Timestamp) (done OpResult) {
		if prev.s.Key == nil && prev.s.EndKey == nil {
			prev.s = s
			prev.ts = ts
			startKey = s.Key
			endKey = s.EndKey
			return ContinueMatch
		}

		if s.Key.Equal(prev.s.EndKey) {
			// Contiguous spans with the same timestamps are expected to be merged.
			// However, LLRB based frontier has some gaps in its merge logic, so just
			// let it be.
			if useBtreeFrontier && ts.Equal(prev.ts) {
				retErr = errors.Newf("expected ranges with equal timestamp to be merged, found %s and %s: %s", prev.s, s, f)
				return StopMatch
			}
		} else {
			// We expect frontier entries to be contiguous.
			retErr = errors.Newf("expected contiguous entries, found gap between %s and %s: %s", prev.s, s, f)
			return StopMatch
		}

		if s.Equal(frontierSpan) && ts.Equal(frontierTS) {
			sawFrontierSpan = true
		}

		endKey = s.EndKey
		prev.s = s
		prev.ts = ts
		return ContinueMatch
	})

	if !sawFrontierSpan {
		return startKey, endKey, errors.Newf("expected to find frontier span %s@%s", frontierSpan, frontierTS)
	}

	return startKey, endKey, retErr
}

// forwardWithErrorCheck forwards span timestamp.
// It verifies if the returned error is consistent with the input span.
func forwardWithErrorCheck(f Frontier, s roachpb.Span, wall int64) error {
	if _, err := f.Forward(s, hlc.Timestamp{WallTime: wall}); err != nil {
		switch s.Key.Compare(s.EndKey) {
		case 1:
			if !errors.Is(err, interval.ErrInvertedRange) {
				return errors.Wrapf(err, "expected inverted span error for span %s", s)
			}
		case 0:
			if len(s.Key) == 0 && len(s.EndKey) == 0 {
				if !errors.Is(err, interval.ErrNilRange) {
					return errors.Wrapf(err, "expected nil range error for span %s", s)
				}
			} else if !errors.Is(err, interval.ErrEmptyRange) {
				return errors.Wrapf(err, "expected empty span error for span %s", s)
			}
		default:
			return errors.Wrapf(err, "f=%s", f)
		}
	}
	return nil
}

func advanceFrontier(t *testing.T, f Frontier, s roachpb.Span, wall int64) {
	t.Helper()
	require.NoError(t, forwardWithErrorCheck(f, s, wall))
	_, _, err := checkContiguousFrontier(f)
	require.NoError(t, err)
}

// TestForwardInvertedSpan is a replay of a failure uncovered by FuzzLLRBFrontier test.
// It verifies frontier behaves as expected when attempting to forward inverted span.
func TestForwardInvertedSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	spAZ := makeSingleCharSpan('A', 'Z')
	testutils.RunTrueAndFalse(t, "btree", func(t *testing.T, useBtreeFrontier bool) {
		defer enableBtreeFrontier(useBtreeFrontier)()

		f, err := MakeFrontier(spAZ)
		require.NoError(t, err)

		advanceFrontier(t, f, makeSpan("AUgymc", "OOyXp"), 1831)
		advanceFrontier(t, f, makeSpan("AUgymc", "OOyXp"), 1923)
		advanceFrontier(t, f, makeSpan("AUgymc", "OOyXp"), 2009)
		advanceFrontier(t, f, makeSpan("AUggymcymc", "OOyXp"), 2009)
		advanceFrontier(t, f, makeSpan("pOyXOmcymc", "pOyXO"), 2009) // NB: inverted span.
		advanceFrontier(t, f, makeSpan("a94", "a948"), 1865)
		advanceFrontier(t, f, makeSpan("a94", "a948"), 1865)
		advanceFrontier(t, f, makeSpan("03hO2Z", "RJRxCy"), 1864)
		advanceFrontier(t, f, makeSpan("03hO2Z", "RJRxCy"), 1864)
		advanceFrontier(t, f, makeSpan("03", "RJRxCy"), 1864)
		advanceFrontier(t, f, makeSpan("0", "RJRxCy"), 1864)
		advanceFrontier(t, f, makeSpan("0", "RJRxCy"), 1864)
		advanceFrontier(t, f, makeSpan("0", "RJRxCy"), 1864)
		advanceFrontier(t, f, makeSpan("0", "RJ"), 1864)
		advanceFrontier(t, f, makeSpan("0", "R"), 1864)
		advanceFrontier(t, f, makeSpan("0", "0"), 1864)
	})
}

func TestForwardToSameTimestamp(t *testing.T) {
	defer enableBtreeFrontier(true)() // LLRB frontier fails this test
	spAZ := makeSingleCharSpan('A', 'Z')

	f, err := MakeFrontier(spAZ)
	require.NoError(t, err)
	advanceFrontier(t, f, makeSpan("Axj", "L"), 0)
	// Frontier should remain the same since we forwarded a subspan
	// to the same timestamp.
	require.Equal(t, "[{A-Z}@0,0]", f.String())
}

func TestAddOverlappingSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := func(wall int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: wall}
	}

	testutils.RunTrueAndFalse(t, "btree", func(t *testing.T, useBtreeFrontier bool) {
		defer enableBtreeFrontier(useBtreeFrontier)()

		f, err := MakeFrontier()
		require.NoError(t, err)

		for r := 'A'; r < 'Z'; r++ {
			require.NoError(t, f.AddSpansAt(ts(int64(r-'A'+1)), makeSpan(string(r), string(r+'a'-'A'))))
		}
		require.NoError(t, f.AddSpansAt(ts(42), makeSpan("a", "z")))
		require.Equal(t, hlc.Timestamp{WallTime: 42}, f.Frontier(), "f=%s", f)
	})
}

// symbols that can make up spans.
var spanSymbols = []byte("@$0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

type spanMaker struct {
	rnd        *rand.Rand
	numSymbols int
	starts     []interval.Comparable
}

func newSpanMaker(numSymbols int, rnd *rand.Rand) (*spanMaker, roachpb.Span) {
	m := &spanMaker{
		rnd:        rnd,
		numSymbols: numSymbols,
	}
	span := roachpb.Span{
		Key:    roachpb.Key{'A'},
		EndKey: roachpb.Key{'z'},
	}
	return m, span
}

func (m *spanMaker) rndKey() interval.Comparable {
	var key []byte
	for n := 1 + m.rnd.Intn(m.numSymbols); n > 0; n-- {
		key = append(key, spanSymbols[m.rnd.Intn(len(spanSymbols))])
	}
	return key
}

func (m *spanMaker) rndSpan() roachpb.Span {
	var startKey interval.Comparable

	if len(m.starts) > 0 && m.rnd.Int()%37 == 0 {
		// With some probability use previous starting point.
		startKey = append(startKey, m.starts[m.rnd.Intn(len(m.starts))]...)
		// Just for fun, nudge start a bit forward or back.
		if dice := m.rnd.Intn(3) - 1; dice != 0 {
			startKey[len(startKey)-1] += byte(dice)
		}
	} else {
		// Generate a new start.
		startKey = m.rndKey()
		m.starts = append(m.starts, startKey)
	}

	endKey := m.rndKey()
	// With some probability, make startKey prefix of endKey.
	if m.rnd.Int()%97 == 0 {
		endKey = append(startKey, endKey...)
	}

	if startKey.Equal(endKey) {
		endKey = append(endKey, spanSymbols[m.rnd.Intn(len(spanSymbols))])
	}

	if endKey.Compare(startKey) < 0 {
		startKey, endKey = endKey, startKey
	}
	if endKey.Equal(startKey) {
		panic(roachpb.Span{Key: roachpb.Key(startKey), EndKey: roachpb.Key(endKey)}.String())
	}
	return roachpb.Span{Key: roachpb.Key(startKey), EndKey: roachpb.Key(endKey)}
}

const maxHistory = 64

// captureHistoryFrontier is a Frontier that captures history
// of forward calls in order to make it easier to reproduce fuzz test failures.
// See TestForwardInvertedSpan.
type SpanFrontier = Frontier
type captureHistoryFrontier struct {
	SpanFrontier
	history []string
}

func (f *captureHistoryFrontier) Forward(span roachpb.Span, ts hlc.Timestamp) (bool, error) {
	f.history = append(f.history, fmt.Sprintf(`advance(t, f, mkspan(%q, %q), %d)`, span.Key, span.EndKey, ts.WallTime))
	if len(f.history) > maxHistory {
		f.history = append([]string{}, f.history[1:]...)
	}
	return f.SpanFrontier.Forward(span, ts)
}

func (f *captureHistoryFrontier) History() string {
	return strings.Join(f.history, "\n")
}

func fuzzFrontier(f *testing.F) {
	seed := randutil.NewPseudoSeed()
	rnd := rand.New(rand.NewSource(seed))

	spanMaker, initialSpan := newSpanMaker(6, rnd)
	const corpusSize = 2 << 10
	for i := 0; i < corpusSize; i++ {
		s := spanMaker.rndSpan()
		// Add fuzz corpus.  Note: timestamps added could be negative, which
		// is of course is not a valid timestamp, but makes it so much fun to test.
		f.Add([]byte(s.Key), []byte(s.EndKey), rnd.Intn(corpusSize)-rnd.Intn(corpusSize))
	}

	mkFrontier := func() Frontier {
		sf, err := MakeFrontier(initialSpan)
		if err != nil {
			f.Fatal(err)
		}
		return sf
	}

	sf := &captureHistoryFrontier{SpanFrontier: mkFrontier()}

	f.Fuzz(func(t *testing.T, startKey, endKey []byte, walltime int) {
		// NB: copy start and end keys: fuzzer mutates inputs.
		var sp roachpb.Span
		sp.Key = append(sp.Key, startKey...)
		sp.EndKey = append(sp.EndKey, endKey...)

		if err := forwardWithErrorCheck(sf, sp, int64(walltime)); err != nil {
			t.Fatalf("err=%+v f=%s History:\n%s", err, sf, sf.History())
		}

		startKey, endKey, err := checkContiguousFrontier(sf)
		if err != nil {
			t.Fatalf("err=%s\nHistory:\n%s", err, sf.History())
		}
		// At the end of iteration, we should have record start/end key equal to the initial span.
		if !initialSpan.Key.Equal(startKey) || !initialSpan.EndKey.Equal(endKey) {
			t.Fatalf("expected to see entire %s sf, saw [%s-%s)", initialSpan, startKey, endKey)
		}
	})
}

func FuzzBtreeFrontier(f *testing.F) {
	defer enableBtreeFrontier(true)()
	fuzzFrontier(f)
}

func FuzzLLRBFrontier(f *testing.F) {
	defer enableBtreeFrontier(false)()
	fuzzFrontier(f)
}

func BenchmarkFrontier(b *testing.B) {
	disableSanityChecksForBenchmark = true
	defer func() {
		disableSanityChecksForBenchmark = false
	}()

	b.StopTimer()
	// To produce repeatable runs, run benchmark with COCKROACH_RANDOM_SEED env to override
	// the seed value and -test.benchtime=Nx to set explicit number of iterations.
	rnd, rndSeed := randutil.NewPseudoRand()
	spanMaker, initialSpan := newSpanMaker(4, rnd)
	const corpusSize = 2 << 14
	corpus := make([]roachpb.Span, corpusSize)
	for i := 0; i < corpusSize; i++ {
		corpus[i] = spanMaker.rndSpan()
	}
	b.StartTimer()

	benchForward := func(b *testing.B, f Frontier, rnd *rand.Rand) {
		if log.V(1) {
			log.Infof(context.Background(), "N=%d NumEntries=%d (seed: %d)", b.N, f.Len(), rndSeed)
		}
		var wall int64 = 10

		for i := 0; i < b.N; i++ {
			if i%10 == 0 {
				wall += rnd.Int63n(10)
			}

			delta := rnd.Int63n(10) - rnd.Int63n(3)
			rndSpan := corpus[rnd.Intn(corpusSize)]
			if _, err := f.Forward(rndSpan, hlc.Timestamp{WallTime: wall + delta}); err != nil {
				b.Fatalf("%+v", err)
			}
		}
		if log.V(1) {
			log.Infof(context.Background(), "Final frontier has %d entries", f.Len())
		}
	}

	for _, enableBtree := range []bool{false, true} {
		b.Run(fmt.Sprintf("btree=%t/rnd", enableBtree), func(b *testing.B) {
			defer enableBtreeFrontier(enableBtree)()

			b.StopTimer()
			// Reset rnd so that we get the same inputs for both benchmarks.
			rnd := rand.New(rand.NewSource(rndSeed))
			f, err := MakeFrontier(initialSpan)
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			b.ReportAllocs()

			benchForward(b, f, rnd)
		})

		// Bench a case where frontier tracks multiple disjoint spans.
		for _, numRanges := range []int{128, 1024, 4096, 8192, 16384} {
			b.Run(fmt.Sprintf("btree=%t/r=%d", enableBtree, numRanges), func(b *testing.B) {
				defer enableBtreeFrontier(enableBtree)()

				b.StopTimer()
				// Reset rnd so that we get the same inputs for both benchmarks.
				rnd := rand.New(rand.NewSource(rndSeed))
				f, err := MakeFrontier()
				if err != nil {
					b.Fatal(err)
				}
				var sg roachpb.SpanGroup
				for sg.Len() < numRanges {
					startKey := roachpb.Key(spanMaker.rndKey())
					sg.Add(roachpb.Span{Key: startKey, EndKey: startKey.Next()})
				}

				if err := sg.ForEach(func(span roachpb.Span) error {
					return f.AddSpansAt(hlc.Timestamp{}, span)
				}); err != nil {
					b.Fatal(err)
				}

				b.StartTimer()
				b.ReportAllocs()

				benchForward(b, f, rnd)
			})
		}
	}
}
