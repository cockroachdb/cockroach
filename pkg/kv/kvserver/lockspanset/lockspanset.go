// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lockspanset

import (
	"fmt"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

const NumLockStrength = lock.MaxStrength + 1

type LockSpanSet struct {
	spans [NumLockStrength][]roachpb.Span
}

var lockSpanSetPool = sync.Pool{
	New: func() interface{} { return new(LockSpanSet) },
}

// New creates a new empty LockSpanSet.
func New() *LockSpanSet {
	return lockSpanSetPool.Get().(*LockSpanSet)
}

// GetSpans returns a slice of spans with the given access.
func (l *LockSpanSet) GetSpans(access lock.Strength) []roachpb.Span {
	return l.spans[access]
}

// Add adds the supplied span to the lock span set to be accessed with the given
// lock strength.
func (l *LockSpanSet) Add(access lock.Strength, span roachpb.Span) {
	l.spans[access] = append(l.spans[access], span)
}

// SortAndDeDup sorts the spans in the SpanSet and removes any duplicates.
func (l *LockSpanSet) SortAndDeDup() {
	for st := lock.Strength(0); st < NumLockStrength; st++ {
		l.spans[st], _ /* distinct */ = roachpb.MergeSpans(&l.spans[st])
	}
}

// Release releases the LockSpanSet and its underlying slices. The receiver
// should not be used after being released.
func (l *LockSpanSet) Release() {
	for st := lock.Strength(0); st < NumLockStrength; st++ {
		// Recycle slice if capacity below threshold.
		const maxRecycleCap = 8
		var recycle []roachpb.Span
		if sl := l.spans[st]; cap(sl) <= maxRecycleCap {
			for i := range sl {
				sl[i] = roachpb.Span{}
			}
			recycle = sl[:0]
		}
		l.spans[st] = recycle
	}
	lockSpanSetPool.Put(l)
}

// Empty returns whether the set contains any spans across all lock strengths.
func (l *LockSpanSet) Empty() bool {
	return l.Len() == 0
}

// String prints a string representation of the LockSpanSet.
func (l *LockSpanSet) String() string {
	var buf strings.Builder
	for st := lock.Strength(0); st < NumLockStrength; st++ {
		for _, span := range l.GetSpans(st) {
			fmt.Fprintf(&buf, "%s: %s\n",
				st, span)
		}
	}
	return buf.String()
}

// Len returns the total number of spans tracked across all strengths.
func (l *LockSpanSet) Len() int {
	var count int
	for st := lock.Strength(0); st < NumLockStrength; st++ {
		count += len(l.GetSpans(st))
	}
	return count
}
