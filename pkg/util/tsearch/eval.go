// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tsearch

import (
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

// EvalTSQuery runs the provided TSQuery against the provided TSVector,
// returning whether or not the query matches the vector.
func EvalTSQuery(q TSQuery, v TSVector) (bool, error) {
	evaluator := tsEvaluator{
		v: v,
		q: q,
	}
	return evaluator.eval()
}

type tsEvaluator struct {
	v TSVector
	q TSQuery
}

func (e *tsEvaluator) eval() (bool, error) {
	return e.evalNode(e.q.root)
}

func (e *tsEvaluator) evalNode(node *tsNode) (bool, error) {
	switch node.op {
	case invalid:
		prefixMatch := false
		if len(node.term.positions) > 0 && node.term.positions[0].weight == weightStar {
			prefixMatch = true
		}

		// To evaluate a term, we search the vector for a match.
		target := node.term.lexeme
		i := sort.Search(len(e.v), func(i int) bool {
			return e.v[i].lexeme >= target
		})
		if i < len(e.v) {
			t := e.v[i]
			if prefixMatch {
				return strings.HasPrefix(t.lexeme, target), nil
			}
			return t.lexeme == target, nil
		}
		return false, nil
	case and:
		l, err := e.evalNode(node.l)
		if err != nil || !l {
			return false, err
		}
		return e.evalNode(node.r)
	case or:
		l, err := e.evalNode(node.l)
		if err != nil {
			return false, err
		}
		if l {
			return true, nil
		}
		return e.evalNode(node.r)
	case not:
		ret, err := e.evalNode(node.l)
		return !ret, err
	case followedby:
		positions, err := e.evalWithinFollowedBy(node)
		return len(positions.positions) > 0, err
	}
	return false, errors.AssertionFailedf("invalid operator %d", node.op)
}

type tspositionset struct {
	positions []tsposition
	width     int
	invert    bool
}

type emitMode int

const (
	emitMatches emitMode = 1 << iota
	emitLeftUnmatched
	emitRightUnmatched
)

func (e *tsEvaluator) evalFollowedBy(
	lPositions, rPositions tspositionset, lOffset, rOffset int, emitMode emitMode,
) (tspositionset, error) {
	// Followed by makes sure that two terms are separated by exactly n words.
	// First, find all slots that match for the left expression.

	// Find the offsetted intersection of 2 sorted integer lists, using the
	// followedN as the offset.
	var ret tspositionset
	var lIdx, rIdx int
	// Loop through the two sorted position lists, until the position on the
	// right is as least as large as the position on the left.
	for {
		lExhausted := lIdx >= len(lPositions.positions)
		rExhausted := rIdx >= len(rPositions.positions)
		if lExhausted && rExhausted {
			break
		}
		var lPos, rPos int
		if !lExhausted {
			lPos = lPositions.positions[lIdx].position + lOffset
		} else {
			// Quit unless we're outputting all of the RHS, which we will if we have
			// a negative match on the LHS.
			if emitMode&emitRightUnmatched == 0 {
				break
			}
			lPos = math.MaxInt64
		}
		if !rExhausted {
			rPos = rPositions.positions[rIdx].position + rOffset
		} else {
			// Quit unless we're outputting all of the LHS, which we will if we have
			// a negative match on the RHS.
			if emitMode&emitLeftUnmatched == 0 {
				break
			}
			rPos = math.MaxInt64
		}

		if lPos < rPos {
			if emitMode&emitLeftUnmatched > 0 {
				ret.positions = append(ret.positions, tsposition{position: lPos})
			}
			lIdx++
		} else if lPos == rPos {
			if emitMode&emitMatches > 0 {
				ret.positions = append(ret.positions, tsposition{position: rPos})
			}
			lIdx++
			rIdx++
		} else {
			if emitMode&emitRightUnmatched > 0 {
				ret.positions = append(ret.positions, tsposition{position: rPos})
			}
			rIdx++
		}
	}
	return ret, nil
}

// evalWithinFollowedBy is the evaluator for subexpressions of a followed by
// operator. Instead of just returning true or false, and possibly short
// circuiting on boolean ops, we need to return all of the tspositions at which
// each arm of the followed by expression matches.
func (e *tsEvaluator) evalWithinFollowedBy(node *tsNode) (tspositionset, error) {
	switch node.op {
	case invalid:
		prefixMatch := false
		if len(node.term.positions) > 0 && node.term.positions[0].weight == weightStar {
			prefixMatch = true
		}

		// To evaluate a term, we search the vector for a match.
		target := node.term.lexeme
		i := sort.Search(len(e.v), func(i int) bool {
			return e.v[i].lexeme >= target
		})
		if i >= len(e.v) {
			// No match.
			return tspositionset{}, nil
		}
		var ret []tsposition
		if prefixMatch {
			for j := i; j < len(e.v); j++ {
				t := e.v[j]
				if !strings.HasPrefix(t.lexeme, target) {
					break
				}
				ret = append(ret, t.positions...)
			}
			ret = sortAndUniqTSPositions(ret)
			return tspositionset{positions: ret}, nil
		} else if e.v[i].lexeme != target {
			// No match.
			return tspositionset{}, nil
		}
		return tspositionset{positions: e.v[i].positions}, nil
	case or:
		var lOffset, rOffset, width int

		lPositions, err := e.evalWithinFollowedBy(node.l)
		if err != nil {
			return tspositionset{}, err
		}
		rPositions, err := e.evalWithinFollowedBy(node.r)
		if err != nil {
			return tspositionset{}, err
		}

		width = lPositions.width
		if rPositions.width > width {
			width = rPositions.width
		}
		lOffset = width - lPositions.width
		rOffset = width - rPositions.width

		mode := emitMatches | emitLeftUnmatched | emitRightUnmatched
		invertResults := false
		switch {
		case lPositions.invert && rPositions.invert:
			invertResults = true
			mode = emitMatches
		case lPositions.invert:
			invertResults = true
			mode = emitLeftUnmatched
		case rPositions.invert:
			invertResults = true
			mode = emitRightUnmatched
		}
		ret, err := e.evalFollowedBy(lPositions, rPositions, lOffset, rOffset, mode)
		if invertResults {
			ret.invert = true
		}
		ret.width = width
		return ret, err
	case not:
		ret, err := e.evalWithinFollowedBy(node.l)
		if err != nil {
			return tspositionset{}, err
		}
		ret.invert = !ret.invert
		return ret, nil
	case followedby:
		// Followed by and and have similar handling.
		fallthrough
	case and:
		var lOffset, rOffset, width int

		lPositions, err := e.evalWithinFollowedBy(node.l)
		if err != nil {
			return tspositionset{}, err
		}
		rPositions, err := e.evalWithinFollowedBy(node.r)
		if err != nil {
			return tspositionset{}, err
		}
		if node.op == followedby {
			lOffset = node.followedN + rPositions.width
			width = lOffset + lPositions.width
		} else {
			width = lPositions.width
			if rPositions.width > width {
				width = rPositions.width
			}
			lOffset = width - lPositions.width
			rOffset = width - rPositions.width
		}

		mode := emitMatches
		invertResults := false
		switch {
		case lPositions.invert && rPositions.invert:
			invertResults = true
			mode |= emitLeftUnmatched | emitRightUnmatched
		case lPositions.invert:
			mode = emitRightUnmatched
		case rPositions.invert:
			mode = emitLeftUnmatched
		}
		ret, err := e.evalFollowedBy(lPositions, rPositions, lOffset, rOffset, mode)
		if invertResults {
			ret.invert = true
		}
		ret.width = width
		return ret, err
	}
	return tspositionset{}, errors.AssertionFailedf("invalid operator %d", node.op)
}

// sortAndUniqTSPositions sorts and uniquifies the input tsposition list by
// their position attributes.
func sortAndUniqTSPositions(pos []tsposition) []tsposition {
	if len(pos) <= 1 {
		return pos
	}
	sort.Slice(pos, func(i, j int) bool {
		return pos[i].position < pos[j].position
	})
	// Then distinct: (wouldn't it be nice if Go had generics?)
	lastUniqueIdx := 0
	for j := 1; j < len(pos); j++ {
		if pos[j].position != pos[lastUniqueIdx].position {
			// We found a unique entry, at index i. The last unique entry in the array
			// was at lastUniqueIdx, so set the entry after that one to our new unique
			// entry, and bump lastUniqueIdx for the next loop iteration.
			lastUniqueIdx++
			pos[lastUniqueIdx] = pos[j]
		}
	}
	pos = pos[:lastUniqueIdx+1]
	return pos
}
