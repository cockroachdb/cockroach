// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import "bytes"

// MultiplicityValue is a bitfield that describes whether a join duplicates
// and/or filters rows from a particular input.
type MultiplicityValue uint8

const (
	// MultiplicityIndeterminateVal indicates that no guarantees can be made about
	// the effect the join will have on its input rows.
	MultiplicityIndeterminateVal MultiplicityValue = 0

	// MultiplicityNotDuplicatedVal indicates that the join will not include input
	// rows in its output more than once.
	MultiplicityNotDuplicatedVal MultiplicityValue = 1 << (iota - 1)

	// MultiplicityPreservedVal indicates that the join will include all input
	// rows in its output.
	MultiplicityPreservedVal
)

// JoinMultiplicity answers queries about how a join will affect the rows from
// its inputs. Left and right input rows can be duplicated and/or filtered by
// the join. As an example:
//
//   CREATE TABLE xy (x INT PRIMARY KEY, y INT);
//   CREATE TABLE uv (u INT PRIMARY KEY, v INT);
//   SELECT * FROM xy FULL JOIN uv ON x=u;
//
// 1. Are rows from xy or uv being duplicated by the join?
// 2. Are any rows being filtered from the join output?
//
// A JoinMultiplicity constructed for the join is able to answer either of the
// above questions by checking one of the MultiplicityValue bit flags. The
// not-duplicated and preserved flags are always unset for a join unless it can
// be statically proven that no rows from the given input will be duplicated or
// filtered respectively. As an example, take the following query:
//
//   SELECT * FROM xy INNER JOIN uv ON y = v;
//
// At execution time, it may be that every row from xy will be included in the
// join output exactly once. However, since this cannot be proven before
// runtime, the duplicated and filtered flags must be set.
//
// After initial construction by multiplicity_builder.go, JoinMultiplicity
// should be considered immutable.
type JoinMultiplicity struct {
	// LeftMultiplicity and RightMultiplicity describe how the left and right
	// input rows respectively will be affected by the join operator.
	// As an example, using the query from above:
	//
	//  SELECT * FROM xy FULL JOIN uv ON x=u;
	//
	// MultiplicityNotDuplicatedVal: both LeftMultiplicity and RightMultiplicity
	// would set the not-duplicated flag because the equality is between key
	// columns, which means that no row can match more than once.
	//
	// MultiplicityPreservedVal: both fields would set the preserved flag because
	// the FullJoin will add back any rows that don't match on the filter
	// conditions.
	LeftMultiplicity  MultiplicityValue
	RightMultiplicity MultiplicityValue
}

// JoinDoesNotDuplicateLeftRows returns true when rows from the left input will
// not be included in the join output more than once.
func (mp *JoinMultiplicity) JoinDoesNotDuplicateLeftRows() bool {
	return mp.LeftMultiplicity&MultiplicityNotDuplicatedVal != 0
}

// JoinDoesNotDuplicateRightRows returns true when rows from the right input
// will not be included in the join output more than once.
func (mp *JoinMultiplicity) JoinDoesNotDuplicateRightRows() bool {
	return mp.RightMultiplicity&MultiplicityNotDuplicatedVal != 0
}

// JoinPreservesLeftRows returns true when all rows from the left input are
// guaranteed to be included in the join output.
func (mp *JoinMultiplicity) JoinPreservesLeftRows() bool {
	return mp.LeftMultiplicity&MultiplicityPreservedVal != 0
}

// JoinPreservesRightRows returns true when all rows from the right input are
// guaranteed to be included in the join output.
func (mp *JoinMultiplicity) JoinPreservesRightRows() bool {
	return mp.RightMultiplicity&MultiplicityPreservedVal != 0
}

// String returns a formatted string containing flags for the left and right
// inputs that indicate how many times any given input row can be guaranteed to
// show up in the join output.
func (mp *JoinMultiplicity) String() string {
	if !mp.isInteresting() {
		return ""
	}

	var buf bytes.Buffer
	const zeroOrMore = "zero-or-more"
	const zeroOrOne = "zero-or-one"
	const oneOrMore = "one-or-more"
	const exactlyOne = "exactly-one"

	isFirstFlag := true

	writeFlag := func(name string) {
		if !isFirstFlag {
			buf.WriteString(", ")
		}
		buf.WriteString(name)
		isFirstFlag = false
	}

	outputFlag := func(doesNotDuplicateRows, preservesRows bool) {
		if doesNotDuplicateRows {
			if preservesRows {
				writeFlag(exactlyOne)
			} else {
				writeFlag(zeroOrOne)
			}
		} else {
			if preservesRows {
				writeFlag(oneOrMore)
			} else {
				writeFlag(zeroOrMore)
			}
		}
	}

	buf.WriteString("left-rows(")
	outputFlag(mp.JoinDoesNotDuplicateLeftRows(), mp.JoinPreservesLeftRows())

	isFirstFlag = true
	buf.WriteString("), right-rows(")
	outputFlag(mp.JoinDoesNotDuplicateRightRows(), mp.JoinPreservesRightRows())
	buf.WriteString(")")

	return buf.String()
}

// isInteresting returns true when rows from either of the inputs are guaranteed
// not to be duplicated or filtered.
func (mp *JoinMultiplicity) isInteresting() bool {
	return mp.JoinDoesNotDuplicateLeftRows() || mp.JoinDoesNotDuplicateRightRows() ||
		mp.JoinPreservesLeftRows() || mp.JoinPreservesRightRows()
}
