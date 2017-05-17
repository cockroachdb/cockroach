// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package interval

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrInvertedRange is returned if an interval is used where the start value is greater
// than the end value.
var ErrInvertedRange = errors.New("interval: inverted range")

// ErrEmptyRange is returned if an interval is used where the start value is equal
// to the end value.
var ErrEmptyRange = errors.New("interval: empty range")

func rangeError(r Range) error {
	switch r.Start.Compare(r.End) {
	case 1:
		return ErrInvertedRange
	case 0:
		return ErrEmptyRange
	default:
		return nil
	}
}

// A Range is a type that describes the basic characteristics of an interval.
type Range struct {
	Start, End Comparable
}

// String implements the Stringer interface.
func (r Range) String() string {
	return fmt.Sprintf("{%x-%x}", r.Start, r.End)
}

// Overlapper specifies the overlapping relationship.
type Overlapper interface {
	// Overlap checks whether two ranges overlap.
	Overlap(Range, Range) bool
}

type inclusiveOverlapper struct{}

// Overlap checks where a and b overlap in the inclusive way.
func (overlapper inclusiveOverlapper) Overlap(a Range, b Range) bool {
	return a.Start.Compare(b.End) <= 0 && b.Start.Compare(a.End) <= 0
}

// InclusiveOverlapper defines overlapping as a pair of ranges that share a segment of the keyspace
// in the inclusive way. "inclusive" means that both start and end keys treated as inclusive values.
var InclusiveOverlapper = inclusiveOverlapper{}

type exclusiveOverlapper struct{}

// Overlap checks where a and b overlap in the exclusive way.
func (overlapper exclusiveOverlapper) Overlap(a Range, b Range) bool {
	return a.Start.Compare(b.End) < 0 && b.Start.Compare(a.End) < 0
}

// ExclusiveOverlapper defines overlapping as a pair of ranges that share a segment of the keyspace
// in the exclusive. "exclusive" means that the start keys are treated as inclusive and the end keys
// are treated as exclusive.
var ExclusiveOverlapper = exclusiveOverlapper{}

// An Interface is a type that can be inserted into an interval tree.
type Interface interface {
	Range() Range
	// Returns a unique ID for the element.
	// TODO(nvanbenschoten): Should this be changed to an int64?
	ID() uintptr
}

var _ = Compare

// Compare returns a value indicating the sort order relationship between a and b. The comparison is
// performed lexicographically on (a.Range().Start, a.ID()) and (b.Range().Start, b.ID()) tuples
// where Range().Start is more significant that ID().
//
// Given c = Compare(a, b):
//
//  c == -1  if (a.Range().Start, a.ID()) < (b.Range().Start, b.ID());
//  c == 0 if (a.Range().Start, a.ID()) == (b.Range().Start, b.ID()); and
//  c == 1 if (a.Range().Start, a.ID()) > (b.Range().Start, b.ID()).
//
// "c == 0" is equivalent to "Equal(a, b) == true".
func Compare(a, b Interface) int {
	startCmp := a.Range().Start.Compare(b.Range().Start)
	if startCmp != 0 {
		return startCmp
	}
	aID := a.ID()
	bID := b.ID()
	if aID < bID {
		return -1
	} else if aID > bID {
		return 1
	} else {
		return 0
	}
}

var _ = Equal

// Equal returns a boolean indicating whethter the given Interfaces are equal to each other. If
// "Equal(a, b) == true", "a.Range().End == b.Range().End" must hold. Otherwise, the interval tree
// behavior is undefined. "Equal(a, b) == true" is equivalent to "Compare(a, b) == 0". But the
// former has measurably better performance than the latter. So Equal should be used when only
// equality state is needed.
func Equal(a, b Interface) bool {
	return a.Range().Start.Equal(b.Range().Start) && a.ID() == b.ID()
}

// A Comparable is a type that describes the ends of a Range.
type Comparable []byte

// Compare returns a value indicating the sort order relationship between the
// receiver and the parameter.
//
// Given c = a.Compare(b):
//  c == -1 if a < b;
//  c == 0 if a == b; and
//  c == 1 if a > b.
//
func (c Comparable) Compare(o Comparable) int {
	return bytes.Compare(c, o)
}

var _ = Comparable(nil).Equal

// Equal returns a boolean indicating if the given comparables are equal to
// each other. Note that this has measurably better performance than
// Compare() == 0, so it should be used when only equality state is needed.
func (c Comparable) Equal(o Comparable) bool {
	return bytes.Equal(c, o)
}

// An Operation is a function that operates on an Interface. If done is returned true, the
// Operation is indicating that no further work needs to be done and so the DoMatching function
// should traverse no further.
type Operation func(Interface) (done bool)

type Tree interface {
	AdjustRanges()
	Len() int
	Get(r Range) []Interface
	GetWithOverlapper(r Range, overlapper Overlapper) []Interface
	Insert(e Interface, fast bool) error
	Delete(e Interface, fast bool) error
	Do(fn Operation) bool
	DoMatching(fn Operation, r Range) bool
	Iterator() TreeIterator
}

type TreeIterator interface {
	Next() (Interface, bool)
}

// NewTree creates a new interval tree with the given overlapper function.
func NewTree(overlapper Overlapper) Tree {
	return newLLBRTree(overlapper)
}
