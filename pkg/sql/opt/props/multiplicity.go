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

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

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
// its inputs. Left and right input rows can be duplicated, filtered and/or
// null-extended by the join. As an example:
//
//   CREATE TABLE xy (x INT PRIMARY KEY, y INT);
//   CREATE TABLE uv (u INT PRIMARY KEY, v INT);
//   SELECT * FROM xy FULL JOIN uv ON x=u;
//
// 1. Are rows from xy or uv being duplicated by the join?
// 2. Are any rows being filtered from the join output?
// 3. Are rows from either side being null-extended by the join?
//
// A JoinMultiplicity constructed for the join is able to answer any of the
// above questions by checking one of the MultiplicityValue bit flags. The
// duplicated, filtered and null-extended flags are always set for a join unless
// it can be statically proven that no rows from the given input will be
// duplicated, filtered, or null-extended respectively. As an example, take the
// following query:
//
//   SELECT * FROM xy INNER JOIN uv ON y = v;
//
// At execution time, it may be that every row from xy will be included in the
// join output exactly once. However, since this cannot be proven before
// runtime, the duplicated and filtered flags must be set.
//
// When it is stored in the Relational of an operator other than a join,
// JoinMultiplicity is simply used to bubble up unfiltered output columns.
//
// After initial construction by multiplicity_builder.go, JoinMultiplicity
// should be considered immutable.
type JoinMultiplicity struct {
	// UnfilteredCols contains all columns from the operator's input(s) that are
	// guaranteed to never have been filtered. Row duplication is allowed and
	// other columns from the same base table need not be included. This allows
	// the validity of properties from the base table to be verified (for example,
	// a foreign-key relation).
	//
	// UnfilteredCols can be populated for non-join as well as join operators
	// because the UnfilteredCols fields of a join's inputs are used in the
	// construction of its JoinMultiplicity.
	//
	// UnfilteredCols should only be used by MultiplicityBuilder to aid in
	// initializing the other fields. Other callers should only use the property
	// methods (e.g. JoinFiltersMatchAllLeftRows).
	UnfilteredCols opt.ColSet

	// LeftMultiplicity and RightMultiplicity describe how the left and right
	// input rows respectively will be affected by the join operator.
	// As an example, using the query from above:
	//
	//  SELECT * FROM xy FULL JOIN uv ON x=u;
	//
	// Duplicated: neither LeftMultiplicity nor RightMultiplicity would set the
	// duplicated flag because the equality is between key columns, which means
	// that no row can match more than once.
	//
	// Filtered: neither field would set the filtered flag because the FullJoin
	// will add back any rows that don't match on the filter conditions.
	//
	// Null-extended: since the FullJoin will add back null-extended rows on both
	// sides, both LeftMultiplicity and RightMultiplicity will set the
	// null-extended flag.
	LeftMultiplicity  MultiplicityValue
	RightMultiplicity MultiplicityValue
}

// JoinWillNotDuplicateLeftRows returns true when rows from the left input will
// not be included in the join output more than once.
func (mp *JoinMultiplicity) JoinWillNotDuplicateLeftRows() bool {
	return mp.LeftMultiplicity&MultiplicityNotDuplicatedVal != 0
}

// JoinWillNotDuplicateRightRows returns true when rows from the right input
// will not be included in the join output more than once.
func (mp *JoinMultiplicity) JoinWillNotDuplicateRightRows() bool {
	return mp.RightMultiplicity&MultiplicityNotDuplicatedVal != 0
}

// JoinPreservesLeftRows returns true when rows from the left input are
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
	const oneOrZero = "one-or-zero"
	const oneOrMore = "one-or-more"
	const exactlyOne = "exactly-one"

	isFirstFlag := true
	buf.WriteString("left-rows(")

	writeFlag := func(name string) {
		if !isFirstFlag {
			buf.WriteString(", ")
		}
		buf.WriteString(name)
		isFirstFlag = false
	}

	if mp.JoinWillNotDuplicateLeftRows() {
		if mp.JoinPreservesLeftRows() {
			writeFlag(exactlyOne)
		} else {
			writeFlag(oneOrZero)
		}
	} else {
		if mp.JoinPreservesLeftRows() {
			writeFlag(oneOrMore)
		} else {
			writeFlag(zeroOrMore)
		}
	}

	isFirstFlag = true
	buf.WriteString("), right-rows(")

	if mp.JoinWillNotDuplicateRightRows() {
		if mp.JoinPreservesRightRows() {
			writeFlag(exactlyOne)
		} else {
			writeFlag(oneOrZero)
		}
	} else {
		if mp.JoinPreservesRightRows() {
			writeFlag(oneOrMore)
		} else {
			writeFlag(zeroOrMore)
		}
	}

	buf.WriteString(")")

	return buf.String()
}

// isInteresting returns true when rows from either of the inputs are guaranteed
// not to be duplicated or filtered.
func (mp *JoinMultiplicity) isInteresting() bool {
	return mp.JoinWillNotDuplicateLeftRows() || mp.JoinWillNotDuplicateRightRows() ||
		mp.JoinPreservesLeftRows() || mp.JoinPreservesRightRows()
}
