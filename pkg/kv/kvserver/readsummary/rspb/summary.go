// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rspb

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

const assertEnabled = buildutil.CrdbTestBuild

// FromTimestamp constructs a read summary from the provided timestamp, treating
// the argument as the low water mark of each segment in the summary.
func FromTimestamp(ts hlc.Timestamp) ReadSummary {
	seg := Segment{LowWater: ts}
	return ReadSummary{
		Local:  seg,
		Global: seg,
	}
}

// Clone performs a deep-copy of the receiver.
func (c *ReadSummary) Clone() *ReadSummary {
	res := new(ReadSummary)
	res.Local = c.Local.Clone()
	res.Global = c.Global.Clone()
	return res
}

// Merge combines two read summaries, resulting in a single summary that
// reflects the combination of all reads in each original summary. The merge
// operation is commutative and idempotent.
func (c *ReadSummary) Merge(o ReadSummary) {
	c.Local.Merge(o.Local)
	c.Global.Merge(o.Global)
}

// Compress compresses the read summary to fit within the provided size budget.
// It splits the budget evenly between the local and global segments.
func (c *ReadSummary) Compress(budget int64) {
	c.Local.Compress(budget / 2)
	c.Global.Compress(budget / 2)
}

// AddReadSpan adds a read span to the segment. The span must be sorted after
// all existing spans in the segment and must not overlap with any existing
// spans.
func (c *Segment) AddReadSpan(s ReadSpan) {
	if assertEnabled {
		if len(s.EndKey) != 0 {
			if bytes.Compare(s.Key, s.EndKey) >= 0 {
				panic(fmt.Sprintf("inverted span: %v", s))
			}
		}
		if len(c.ReadSpans) > 0 {
			last := c.ReadSpans[len(c.ReadSpans)-1]
			if bytes.Compare(last.Key, s.Key) >= 0 {
				panic(fmt.Sprintf("out of order spans: %v %v", last, s))
			}
			if len(last.EndKey) != 0 && bytes.Compare(last.EndKey, s.Key) > 0 {
				panic(fmt.Sprintf("overlapping spans: %v %v", last, s))
			}
		}
	}
	if s.Timestamp.LessEq(c.LowWater) {
		return // ignore
	}
	c.ReadSpans = append(c.ReadSpans, s)
}

// Clone performs a deep-copy of the receiver.
func (c *Segment) Clone() Segment {
	res := *c
	if len(c.ReadSpans) != 0 {
		res.ReadSpans = make([]ReadSpan, len(c.ReadSpans))
		copy(res.ReadSpans, c.ReadSpans)
	}
	return res
}

// Merge combines two segments, resulting in a single segment that reflects the
// combination of all reads in each original segment. The merge operation is
// commutative and idempotent.
func (c *Segment) Merge(o Segment) {
	// Forward the low water mark.
	if c.LowWater != o.LowWater {
		if c.LowWater.Less(o.LowWater) {
			// c.LowWater < o.LowWater, filter c.ReadSpans.
			c.ReadSpans = filterSpans(c.ReadSpans, o.LowWater)
			c.LowWater = o.LowWater
		} else {
			// c.LowWater > o.LowWater, filter c.ReadSpans.
			o.ReadSpans = filterSpans(o.ReadSpans, c.LowWater)
		}
	}
	// Merge the read spans.
	if len(o.ReadSpans) == 0 {
		// Fast-path #1: nothing to merge.
		return
	}
	if len(c.ReadSpans) == 0 {
		// Fast-path #2: copy o.ReadSpans directly.
		c.ReadSpans = o.ReadSpans
		return
	}
	// General case: merge the (individually sorted) c.ReadSpans and o.ReadSpans.
	c.ReadSpans = mergeSpans[readSpanVal, ReadSpan, *ReadSpan](c.ReadSpans, o.ReadSpans)
}

// filterSpans filters the read spans in spans by the provided timestamp. Any
// read span equal to or less than minTs is discarded. May mutate spans in the
// process.
func filterSpans(spans []ReadSpan, minTs hlc.Timestamp) []ReadSpan {
	res := spans
	cpy := false // don't copy unless necessary
	for i, s := range spans {
		if s.Timestamp.LessEq(minTs) {
			// Filter out span.
			if !cpy {
				cpy = true
				res = spans[:i]
			}
		} else if cpy {
			res = append(res, s)
		}
	}
	return res
}

// Compress compresses the segment to fit within the provided size budget,
// provide in bytes. The compression is performed in-place and may mutate the
// segment.
//
// Compression is performed by truncating the read spans in the segment while
// advancing the low water mark to account for the loss in resolution.
func (c *Segment) Compress(budget int64) {
	sizeBefore := int64(c.Size())
	if sizeBefore <= budget {
		return // already under budget
	}

	// Sort the read spans by decreasing timestamp.
	spans := c.ReadSpans
	sort.Sort(readSpansByDecrTime(spans))

	// Determine the number of spans to truncate, from oldest to newest.
	c.ReadSpans = nil
	sizeNoSpans := int64(c.Size())
	spanAllowance := budget - sizeNoSpans // may be negative
	truncIdx := 0
	for truncIdx < len(spans) {
		span := spans[truncIdx]
		spanSize := int64(span.Size())
		if spanSize > spanAllowance {
			break
		}
		spanAllowance -= spanSize
		truncIdx++
	}
	if truncIdx < len(spans) {
		// If we're truncating, the low water mark is advanced to the timestamp of
		// the first span that is being truncated. Due to the sort by timestamp, is
		// the maximum timestamp of any span that is being truncated.
		c.LowWater.Forward(spans[truncIdx].Timestamp)
	}
	if truncIdx > 0 {
		// If we're keeping any spans, perform the truncation.
		for i := truncIdx; i < len(spans); i++ {
			spans[i] = ReadSpan{} // zero out for GC
		}
		spans = spans[:truncIdx]

		// Then re-sort the spans by start key. The spans were previously
		// non-overlapping, so they will be non-overlapping after the sort.
		sort.Sort(readSpansByKey(spans))
		c.ReadSpans = spans
	}
}

// readSpansByDecrTime implements sorting of a slice of ReadSpans by timestamp,
// with the largest timestamps sorting first.
type readSpansByDecrTime []ReadSpan

func (s readSpansByDecrTime) Len() int           { return len(s) }
func (s readSpansByDecrTime) Less(i, j int) bool { return s[j].Timestamp.Less(s[i].Timestamp) }
func (s readSpansByDecrTime) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// readSpansByKey implements sorting of a slice of ReadSpans by start key.
type readSpansByKey []ReadSpan

func (s readSpansByKey) Len() int           { return len(s) }
func (s readSpansByKey) Less(i, j int) bool { return bytes.Compare(s[i].Key, s[j].Key) < 0 }
func (s readSpansByKey) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
