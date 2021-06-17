// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import "github.com/cockroachdb/cockroach/pkg/util/interval"

// A SpanGroup is a specialization of interval.RangeGroup which deals
// with key spans. The zero-value of a SpanGroup can be used immediately.
//
// A SpanGroup does not support concurrent use.
type SpanGroup struct {
	rg interval.RangeGroup
}

func (g *SpanGroup) checkInit() {
	if g.rg == nil {
		g.rg = interval.NewRangeTree()
	}
}

// Add will attempt to add the provided Spans to the SpanGroup,
// returning whether the addition increased the span of the group
// or not.
func (g *SpanGroup) Add(spans ...Span) bool {
	if len(spans) == 0 {
		return false
	}
	ret := false
	g.checkInit()
	for _, span := range spans {
		ret = g.rg.Add(s2r(span)) || ret
	}
	return ret
}

// Sub will attempt to subtract the provided Spans from the SpanGroup,
// returning whether the subtraction increased the span of the group
// or not.
func (g *SpanGroup) Sub(spans ...Span) bool {
	if len(spans) == 0 {
		return false
	}
	ret := false
	g.checkInit()
	for _, span := range spans {
		ret = g.rg.Sub(s2r(span)) || ret
	}
	return ret
}

// Contains returns whether or not the provided Key is contained
// within the group of Spans in the SpanGroup.
func (g *SpanGroup) Contains(k Key) bool {
	if g.rg == nil {
		return false
	}
	return g.rg.Encloses(interval.Range{
		Start: interval.Comparable(k),
		// Use the next key since range-ends are exclusive.
		End: interval.Comparable(k.Next()),
	})
}

// Len returns the number of Spans currently within the SpanGroup.
// This will always be equal to or less than the number of spans added,
// as spans that overlap will merge to produce a single larger span.
func (g *SpanGroup) Len() int {
	if g.rg == nil {
		return 0
	}
	return g.rg.Len()
}

var _ = (*SpanGroup).Len

// Slice will return the contents of the SpanGroup as a slice of Spans.
func (g *SpanGroup) Slice() []Span {
	rg := g.rg
	if rg == nil {
		return nil
	}
	ret := make([]Span, 0, rg.Len())
	it := rg.Iterator()
	for {
		rng, next := it.Next()
		if !next {
			break
		}
		ret = append(ret, r2s(rng))
	}
	return ret
}

// s2r converts a Span to an interval.Range.  Since the Key and
// interval.Comparable types are both just aliases of []byte,
// we don't have to perform any other conversion.
func s2r(s Span) interval.Range {
	// Per docs on Span, if the span represents only a single key,
	// the EndKey value may be empty.  We'll handle this case by
	// ensuring we always have an exclusive end key value.
	var end = s.EndKey
	if len(end) == 0 || s.Key.Equal(s.EndKey) {
		end = s.Key.Next()
	}
	return interval.Range{
		Start: interval.Comparable(s.Key),
		End:   interval.Comparable(end),
	}
}

// r2s converts a Range to a Span
func r2s(r interval.Range) Span {
	return Span{
		Key:    Key(r.Start),
		EndKey: Key(r.End),
	}
}
