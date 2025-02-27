// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package span

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/errors"
)

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
		if s.Equal(frontierSpan) && ts.Equal(frontierTS) {
			sawFrontierSpan = true
		}

		if prev.s.Key == nil && prev.s.EndKey == nil {
			prev.s = s
			prev.ts = ts
			startKey = s.Key
			endKey = s.EndKey
			return ContinueMatch
		}

		if s.Key.Equal(prev.s.EndKey) {
			// Contiguous spans with the same timestamps are expected to be merged.
			if ts.Equal(prev.ts) {
				retErr = errors.Newf("expected ranges with equal timestamp to be merged, found %s and %s: %s", prev.s, s, f)
				return StopMatch
			}
		} else {
			// We expect frontier entries to be contiguous.
			retErr = errors.Newf("expected contiguous entries, found gap between %s and %s: %s", prev.s, s, f)
			return StopMatch
		}

		endKey = s.EndKey
		prev.s = s
		prev.ts = ts
		return ContinueMatch
	})

	if !sawFrontierSpan {
		return startKey, endKey, errors.Newf("expected to find frontier span %s@%s: %s", frontierSpan, frontierTS, f)
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
	f.history = append(f.history,
		fmt.Sprintf(`advanceFrontier(t, f, makeSpan(%q, %q), %d)`, span.Key, span.EndKey, ts.WallTime))
	if len(f.history) > maxHistory {
		f.history = append([]string{}, f.history[1:]...)
	}
	return f.SpanFrontier.Forward(span, ts)
}

func (f *captureHistoryFrontier) History() string {
	return strings.Join(f.history, "\n")
}
