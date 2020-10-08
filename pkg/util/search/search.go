// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package search

import "github.com/cockroachdb/errors"

// A Predicate is a funcation that returns whether a given search value "passes"
// or not. It assumes that that within the search domain of [min, max) provided
// to a Searcher, f(i) == true implies f(i-1) == true and f(i) == false implies
// f(i+1) == false. A Predicate can be called multiple times, so it should be
// a pure function.
type Predicate func(int) (bool, error)

// A Searcher searches to find the largest value that passes a given predicate
// function.
type Searcher interface {
	// Search runs the predicate function multiple times while searching for the
	// largest value that passes the provided predicate function. It is only
	// valid to call Search once for a given Searcher instance.
	Search(pred Predicate) (res int, err error)

	// The following two methods are un-exported and are used by
	// searchWithSearcher to provide a default implementation of Search.

	// current returns the current value of the Searcher.
	current() int

	// step updates the Searcher with the results of a single search step.
	step(pass bool) (found bool)
}

// Used to provide a default implementation of Searcher.Search.
func searchWithSearcher(s Searcher, pred Predicate) (int, error) {
	for {
		pass, err := pred(s.current())
		if err != nil {
			return 0, err
		}
		found := s.step(pass)
		if found {
			return s.current(), nil
		}
	}
}

type searchSpace struct {
	min int // max known passing
	max int // min known failing
}

func (ss *searchSpace) bound(pass bool, cur, prec int) (bool, int) {
	if prec < 1 {
		panic(errors.Errorf("precision must be >= 1; found %d", prec))
	}
	if pass {
		if cur >= ss.max {
			panic(errors.Errorf("passed at index above max; max=%v, cur=%v", ss.max, cur))
		}
		ss.min = cur
	} else {
		if cur <= ss.min {
			panic(errors.Errorf("failed at index below min; min=%v, cur=%v", ss.min, cur))
		}
		ss.max = cur
	}
	if ss.max-ss.min <= prec {
		return true, mid(ss.min, ss.max)
	}
	return false, 0
}

type binarySearcher struct {
	ss   searchSpace
	cur  int
	prec int
}

// NewBinarySearcher returns a Searcher implementing a binary search strategy.
// Running the search predicate at min is assumed to pass and running the
// predicate at max is assumed to fail.
//
// While searching, it will result in a worst case and average case of O(log n)
// calls to the predicate function.
func NewBinarySearcher(min, max, prec int) Searcher {
	if min >= max {
		panic(errors.Errorf("min must be less than max; min=%v, max=%v", min, max))
	}
	if prec < 1 {
		panic(errors.Errorf("precision must be >= 1; prec=%v", prec))
	}
	return &binarySearcher{
		ss: searchSpace{
			min: min,
			max: max,
		},
		cur:  mid(min, max),
		prec: prec,
	}
}

func (bs *binarySearcher) Search(pred Predicate) (int, error) {
	return searchWithSearcher(bs, pred)
}

func (bs *binarySearcher) current() int { return bs.cur }

func (bs *binarySearcher) step(pass bool) (found bool) {
	found, val := bs.ss.bound(pass, bs.cur, bs.prec)
	if found {
		bs.cur = val
		return true
	}

	bs.cur = mid(bs.ss.min, bs.ss.max)
	return false
}

type lineSearcher struct {
	ss        searchSpace
	cur       int
	stepSize  int
	firstStep bool
	overshot  bool
	prec      int
}

// NewLineSearcher returns a Searcher implementing a line search strategy with
// an adaptive step size. Running the search predicate at min is assumed to pass
// and running the predicate at max is assumed to fail. The strategy will begin
// searching at the provided start index and with the specified step size.
//
// While searching, it will result in a worst case of O(log n) calls to the
// predicate function. However, the average efficiency is dependent on the
// distance between the starting value and the desired value. If the initial
// guess is fairly accurate, the search strategy is expected to perform better
// (i.e. result in fewer steps) than performing a binary search with no a priori
// knowledge.
func NewLineSearcher(min, max, start, stepSize, prec int) Searcher {
	if min >= max {
		panic(errors.Errorf("min must be less than max; min=%v, max=%v", min, max))
	}
	if start < min || start > max {
		panic(errors.Errorf("start must be between (min, max); start=%v, min=%v, max=%v",
			start, min, max))
	}
	if stepSize < 1 {
		panic(errors.Errorf("stepSize must be >= 1; stepSize=%v", stepSize))
	}
	if prec < 1 {
		panic(errors.Errorf("precision must be >= 1; prec=%v", prec))
	}
	return &lineSearcher{
		ss: searchSpace{
			min: min,
			max: max,
		},
		cur:       start,
		stepSize:  stepSize,
		firstStep: true,
		prec:      prec,
	}
}

func (ls *lineSearcher) Search(pred Predicate) (int, error) {
	return searchWithSearcher(ls, pred)
}

func (ls *lineSearcher) current() int { return ls.cur }

func (ls *lineSearcher) step(pass bool) (found bool) {
	found, val := ls.ss.bound(pass, ls.cur, ls.prec)
	if found {
		ls.cur = val
		return true
	}

	neg := 1
	if !pass {
		neg = -1
	}
	if ls.firstStep {
		// First step. Determine initial direction.
		ls.firstStep = false
		ls.stepSize = neg * ls.stepSize
	} else if neg*ls.stepSize < 0 {
		// Overshot. Reverse and decrease step size.
		ls.overshot = true
		ls.stepSize = -ls.stepSize / 2
	} else {
		// Undershot.
		if ls.overshot {
			// Already overshot. Continue decreasing step size.
			ls.stepSize /= 2
		} else {
			// Haven't yet overshot. Increase step size.
			ls.stepSize *= 2
		}
	}

	// Don't exceed bounds.
	minStep := ls.ss.min - ls.cur + 1
	maxStep := ls.ss.max - ls.cur - 1
	ls.stepSize = max(min(ls.stepSize, maxStep), minStep)
	ls.cur = ls.cur + ls.stepSize
	return false
}

func mid(a, b int) int {
	return (a + b) / 2
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
