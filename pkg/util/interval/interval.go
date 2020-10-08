// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This code originated in the github.com/biogo/store/interval package.

// Package interval provides two implementations for an interval tree. One is
// based on an augmented Left-Leaning Red Black tree. The other is based on an
// augmented BTree.
package interval

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
)

// ErrInvertedRange is returned if an interval is used where the start value is greater
// than the end value.
var ErrInvertedRange = errors.Newf("interval: inverted range")

// ErrEmptyRange is returned if an interval is used where the start value is equal
// to the end value.
var ErrEmptyRange = errors.Newf("interval: empty range")

// ErrNilRange is returned if an interval is used where both the start value and
// the end value are nil. This is a specialization of ErrEmptyRange.
var ErrNilRange = errors.Newf("interval: nil range")

func rangeError(r Range) error {
	switch r.Start.Compare(r.End) {
	case 1:
		return ErrInvertedRange
	case 0:
		if len(r.Start) == 0 && len(r.End) == 0 {
			return ErrNilRange
		}
		return ErrEmptyRange
	default:
		return nil
	}
}

// A Range is a type that describes the basic characteristics of an interval.
type Range struct {
	Start, End Comparable
}

// Equal returns whether the two ranges are equal.
func (r Range) Equal(other Range) bool {
	return r.Start.Equal(other.Start) && r.End.Equal(other.End)
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
	return overlapsInclusive(a, b)
}

func overlapsInclusive(a Range, b Range) bool {
	return a.Start.Compare(b.End) <= 0 && b.Start.Compare(a.End) <= 0
}

// InclusiveOverlapper defines overlapping as a pair of ranges that share a segment of the keyspace
// in the inclusive way. "inclusive" means that both start and end keys treated as inclusive values.
var InclusiveOverlapper = inclusiveOverlapper{}

type exclusiveOverlapper struct{}

// Overlap checks where a and b overlap in the exclusive way.
func (overlapper exclusiveOverlapper) Overlap(a Range, b Range) bool {
	return overlapsExclusive(a, b)
}

func overlapsExclusive(a Range, b Range) bool {
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

// Equal returns a boolean indicating whether the given Interfaces are equal to each other. If
// "Equal(a, b) == true", "a.Range().End == b.Range().End" must hold. Otherwise, the interval tree
// behavior is undefined. "Equal(a, b) == true" is equivalent to "Compare(a, b) == 0". But the
// former has measurably better performance than the latter. So Equal should be used when only
// equality state is needed.
func Equal(a, b Interface) bool {
	return a.ID() == b.ID() && a.Range().Start.Equal(b.Range().Start)
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

// Tree is an interval tree. For all the functions which have a fast argument,
// fast being true means a fast operation which does not adjust node ranges. If
// fast is false, node ranges are adjusted.
type Tree interface {
	// AdjustRanges fixes range fields for all nodes in the tree. This must be
	// called before Get, Do or DoMatching* is used if fast insertion or deletion
	// has been performed.
	AdjustRanges()
	// Len returns the number of intervals stored in the Tree.
	Len() int
	// Get returns a slice of Interfaces that overlap r in the tree. The slice is
	// sorted nondecreasingly by interval start.
	Get(r Range) []Interface
	// GetWithOverlapper returns a slice of Interfaces that overlap r in the tree
	// using the provided overlapper function. The slice is sorted nondecreasingly
	// by interval start.
	GetWithOverlapper(r Range, overlapper Overlapper) []Interface
	// Insert inserts the Interface e into the tree. Insertions may replace an
	// existing Interface which is equal to the Interface e.
	Insert(e Interface, fast bool) error
	// Delete deletes the Interface e if it exists in the tree. The deleted
	// Interface is equal to the Interface e.
	Delete(e Interface, fast bool) error
	// Do performs fn on all intervals stored in the tree. The traversal is done
	// in the nondecreasing order of interval start. A boolean is returned
	// indicating whether the traversal was interrupted by an Operation returning
	// true. If fn alters stored intervals' sort relationships, future tree
	// operation behaviors are undefined.
	Do(fn Operation) bool
	// DoMatching performs fn on all intervals stored in the tree that overlaps r.
	// The traversal is done in the nondecreasing order of interval start. A
	// boolean is returned indicating whether the traversal was interrupted by an
	// Operation returning true. If fn alters stored intervals' sort
	// relationships, future tree operation behaviors are undefined.
	DoMatching(fn Operation, r Range) bool
	// Iterator creates an iterator to iterate over all intervals stored in the
	// tree, in-order.
	Iterator() TreeIterator
	// Clear this tree.
	Clear()
	// Clone clones the tree, returning a copy.
	Clone() Tree
}

// TreeIterator iterates over all intervals stored in the interval tree, in-order.
type TreeIterator interface {
	// Next returns the current interval stored in the interval tree	and moves
	// the iterator to the next interval. The method returns false if no intervals
	// remain in the interval tree.
	Next() (Interface, bool)
}

var useBTreeImpl = envutil.EnvOrDefaultBool("COCKROACH_INTERVAL_BTREE", false)

// NewTree creates a new interval tree with the given overlapper function. It
// uses the augmented Left-Leaning Red Black tree implementation.
func NewTree(overlapper Overlapper) Tree {
	if useBTreeImpl {
		return newBTree(overlapper)
	}
	return newLLRBTree(overlapper)
}
