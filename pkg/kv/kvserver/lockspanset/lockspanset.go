// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lockspanset

import (
	"fmt"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

type LockSpanSet struct {
	spans [lock.NumLockStrength][]roachpb.Span
}

var lockSpanSetPool = sync.Pool{
	New: func() interface{} { return new(LockSpanSet) },
}

// New creates a new empty LockSpanSet.
func New() *LockSpanSet {
	return lockSpanSetPool.Get().(*LockSpanSet)
}

// GetSpans returns a slice of spans with the given strength.
func (l *LockSpanSet) GetSpans(str lock.Strength) []roachpb.Span {
	return l.spans[str]
}

// Add adds the supplied span to the LockSpanSet to be accessed with the given
// lock strength.
func (l *LockSpanSet) Add(str lock.Strength, span roachpb.Span) {
	l.spans[str] = append(l.spans[str], span)
}

// SortAndDeDup sorts the spans in the LockSpanSet and removes any duplicates.
func (l *LockSpanSet) SortAndDeDup() {
	for st := range l.spans {
		l.spans[st], _ /* distinct */ = roachpb.MergeSpans(&l.spans[st])
	}
}

// Release releases the LockSpanSet and its underlying slices. The receiver
// should not be used after being released.
func (l *LockSpanSet) Release() {
	for st := range l.spans {
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
	for st, spans := range l.spans {
		for _, span := range spans {
			fmt.Fprintf(&buf, "%s: %s\n",
				lock.Strength(st), span)
		}
	}
	return buf.String()
}

// Len returns the total number of spans tracked across all strengths.
func (l *LockSpanSet) Len() int {
	var count int
	for _, spans := range l.spans {
		count += len(spans)
	}
	return count
}

// Copy copies the LockSpanSet.
func (l *LockSpanSet) Copy() *LockSpanSet {
	n := New()
	for st := range l.spans {
		n.spans[st] = append(n.spans[st], l.spans[st]...)
	}
	return n
}

// Reserve space for N additional spans.
func (l *LockSpanSet) Reserve(str lock.Strength, n int) {
	existing := l.spans[str]
	if n <= cap(existing)-len(existing) {
		return
	}
	l.spans[str] = make([]roachpb.Span, len(existing), n+len(existing))
	copy(l.spans[str], existing)
}

// Validate returns an error if any spans that have been added to the set are
// invalid.
func (l *LockSpanSet) Validate() error {
	for _, spans := range l.spans {
		for _, span := range spans {
			if !span.Valid() {
				return errors.Errorf("invalid span %s %s", span.Key, span.EndKey)
			}
		}
	}
	return nil
}
