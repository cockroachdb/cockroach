package gapcheck

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/huandu/skiplist"
)

type span struct {
	start, end int // both inclusive
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
	spans.SetRandSource(rand.NewSource(time.Now().UnixNano()))

	return &GapChecker{spans: spans}
}

func (c *GapChecker) Add(n int) {
	gte := c.spans.Find(span{n, n})
	if gte == nil {
		// see if we can extend the last span.
		if last := c.spans.Back(); last != nil && last.Key().(span).end == n-1 {
			c.spans.Remove(last.Key())
			c.spans.Set(span{last.Key().(span).start, n}, nil)
			return
		}

		// else insert new span at end.
		c.spans.Set(span{n, n}, nil)
		return
	}

	// so there's a span that starts at or after n.
	s := gte.Key().(span)
	if s.contains(n) {
		return
	}

	if n == s.start-1 {
		// extend start backwards.
		c.spans.Remove(s) // remove old span.
		new := c.spans.Set(span{s.start - 1, s.end}, nil)
		// if previous span ends at n-1, merge them.
		if prev := new.Prev(); prev != nil && prev.Key().(span).end == n-1 {
			c.spans.Remove(prev.Key())
			c.spans.Remove(new.Key())
			c.spans.Set(span{prev.Key().(span).start, s.end}, nil)
		}
		return
	}
	if prev := gte.Prev(); prev != nil && prev.Key().(span).end == n-1 {
		// extend previous span.
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
		gaps = append(gaps, span{e.Prev().Key().(span).end + 1, e.Key().(span).start - 1})
	}

	return fmt.Errorf("gap(s) found: %+v", gaps)
}

func (c *GapChecker) Print() {
	a := make([]span, 0, c.spans.Len())
	for e := c.spans.Front(); e != nil; e = e.Next() {
		a = append(a, e.Key().(span))
	}
	fmt.Printf("%+v\n", a)
}
