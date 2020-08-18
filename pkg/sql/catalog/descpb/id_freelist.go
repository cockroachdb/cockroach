// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

import (
	"fmt"
	"strings"
)

func singleton(elem uint32) IDRange {
	return IDRange{Start: elem, End: elem + 1}
}

func (r *IDRange) empty() bool {
	return r.Start == r.End
}

func (r *IDRange) popFront() uint32 {
	val := r.Start
	r.Start++
	return val
}

// compareElem returns 0 if the element is within the range, -1 if it is less
// than the start of the range, and +1 if it is larger than the end of the range.
func (r *IDRange) compareElem(elem uint32) int {
	if elem < r.Start {
		return -1
	}
	if elem >= r.Start && elem < r.End {
		return 0
	}
	return 1
}

func collapse(prev IDRange, elem uint32, next IDRange) []IDRange {
	switch {
	case prev.End == elem && elem+1 == next.Start:
		// In this case we can collapse the ranges into a single range.
		return []IDRange{{Start: prev.Start, End: next.End}}
	case prev.End == elem:
		// In this case we can only collapse the first range.
		return []IDRange{{Start: prev.Start, End: elem + 1}, next}
	case next.Start == elem+1:
		// In this case we can only collapse the second range.
		return []IDRange{prev, {Start: elem, End: next.End}}
	default:
		// In the default case we can't do anything.
		return []IDRange{prev, singleton(elem), next}
	}
}

func (fl *IDFreeList) AddElement(elem uint32) {
	r := fl.Ranges
	if len(r) == 0 {
		fl.Ranges = []IDRange{{Start: elem, End: elem + 1}}
		return
	}

	// Handle if the element should be inserted at the front or the end.
	first := &r[0]
	if first.compareElem(elem) < 0 {
		if first.Start == elem+1 {
			first.Start--
		} else {
			newRanges := make([]IDRange, 0, len(fl.Ranges)+1)
			fl.Ranges = append(append(newRanges, singleton(elem)), fl.Ranges...)
		}
		return
	}

	last := &r[len(r)-1]
	if last.compareElem(elem) > 0 {
		if last.End == elem {
			last.End++
		} else {
			fl.Ranges = append(fl.Ranges, singleton(elem))
		}
		return
	}

	for i := 0; i < len(r)-1; i++ {
		cur, next := r[i], r[i+1]
		switch cur.compareElem(elem) {
		case -1:
			panic("unreachable")
		case 0:
			// We already have this element.
			return
		case 1:
			// We move on.
		}
		switch next.compareElem(elem) {
		case 0, 1:
			// Handle it next iteration?
			continue
		}

		// At this point, we know that the element fits in between cur and next.
		result := make([]IDRange, 0, len(r))
		result = append(result, r[:i]...)
		result = append(result, collapse(cur, elem, next)...)
		result = append(result, r[i+2:]...)
		fl.Ranges = result
		return
	}
}

func (fl *IDFreeList) GetNextFree() (uint32, bool) {
	r := fl.Ranges
	if len(r) == 0 {
		return 0, false
	}
	front := &r[0]
	next := front.popFront()
	if front.empty() {
		fl.Ranges = fl.Ranges[1:]
	}
	return next, true
}

func (fl *IDFreeList) DebugString() string {
	var b strings.Builder
	b.WriteString("[")
	for i, r := range fl.Ranges {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("(%d, %d)", r.Start, r.End))
	}
	b.WriteString("]")
	return b.String()
}
