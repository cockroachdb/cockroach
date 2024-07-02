// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package gapcheck

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/huandu/skiplist"
)

type span struct {
	start, end int // Both inclusive.
}

func (s span) contains(n int) bool {
	return n >= s.start && n <= s.end
}

// GapChecker checks for gaps in a sequence of ints.
type GapChecker struct {
	spans *skiplist.SkipList
}

// NewGapChecker returns a new GapChecker.
func NewGapChecker() *GapChecker {
	spans := skiplist.New(skiplist.GreaterThanFunc(func(k1, k2 any) int {
		s1, s2 := k1.(span), k2.(span)
		if s1.start > s2.start {
			return 1
		} else if s1.start < s2.start {
			return -1
		}
		return 0
	}))
	spans.SetRandSource(rand.NewSource(timeutil.Now().UnixNano()))

	return &GapChecker{spans: spans}
}

func (c *GapChecker) Add(n int) {
	gte := c.spans.Find(span{n, n})
	if gte == nil {
		// We might be inside the last span, check that.
		last := c.spans.Back()
		if last != nil && last.Key().(span).contains(n) {
			return
		}
		// See if we can extend the last span.
		if last != nil && last.Key().(span).end == n-1 {
			c.spans.Remove(last.Key())
			c.spans.Set(span{last.Key().(span).start, n}, nil)
			return
		}

		// Else insert new span at end.
		c.spans.Set(span{n, n}, nil)
		return
	}

	// There's a span that starts at or after n.
	s := gte.Key().(span)
	if s.contains(n) || gte.Prev() != nil && gte.Prev().Key().(span).contains(n) {
		return
	}

	if n == s.start-1 {
		// Extend start backwards.
		c.spans.Remove(s) // Remove old span.
		new := c.spans.Set(span{s.start - 1, s.end}, nil)
		// If previous span ends at n-1, merge them.
		if prev := new.Prev(); prev != nil && prev.Key().(span).end == n-1 {
			c.spans.Remove(prev.Key())
			c.spans.Remove(new.Key())
			c.spans.Set(span{prev.Key().(span).start, s.end}, nil)
		}
		return
	}
	if prev := gte.Prev(); prev != nil && prev.Key().(span).end == n-1 {
		// Extend previous span.
		c.spans.Remove(prev.Key())
		c.spans.Set(span{prev.Key().(span).start, n}, nil)
		return
	}

	c.spans.Set(span{n, n}, nil)
}

func (c *GapChecker) Check() error {
	if c.spans.Len() < 2 {
		return nil
	}

	gaps := make([]span, 0, c.spans.Len()-1)
	for e := c.spans.Front(); e != nil; e = e.Next() {
		if e == c.spans.Front() {
			continue
		}
		s := span{e.Prev().Key().(span).end + 1, e.Key().(span).start - 1}
		if s.start > s.end {
			panic(fmt.Errorf("(looking at %v) gap with start > end: %+v\n%s", e.Key(), s, c.String()))
		}
		gaps = append(gaps, s)
	}

	return fmt.Errorf("gap(s) found: %+v; full sequence: %s", gaps, c.String())
}

func (c *GapChecker) String() string {
	b := strings.Builder{}
	b.WriteString("GapChecker{")
	for e := c.spans.Front(); e != nil; e = e.Next() {
		b.WriteString(fmt.Sprintf("%+v", e.Key().(span)))
	}
	b.WriteString("}")
	return b.String()
}
