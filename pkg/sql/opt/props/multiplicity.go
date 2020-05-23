// Copyright 2018 The Cockroach Authors.
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

// MultiplicityValue is an enum that describes whether a join filters,
// duplicates and/or null-extends rows from its input.
type MultiplicityValue uint8

const (
	// UnchangedMultiplicityVal indicates that every input row will be included in
	// the join output exactly once.
	UnchangedMultiplicityVal MultiplicityValue = 0

	// DuplicatedMultiplicityVal indicates that the join may include input rows in
	// its output more than once.
	DuplicatedMultiplicityVal MultiplicityValue = 1 << (iota - 1)

	// FilteredMultiplicityVal indicates that the join may not include all input
	// rows in its output.
	FilteredMultiplicityVal

	// NullExtendedMultiplicityVal indicates that the join may null-extend the
	// input.
	NullExtendedMultiplicityVal
)

// Multiplicity answers queries about how a join will affect the rows from its
// inputs. Left and right input rows can be duplicated, filtered and/or
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
// A Multiplicity constructed for the join is able to answer any of the above
// questions by checking one of the MultiplicityValue bit flags. The duplicated,
// filtered and null-extended flags are always set for a join unless it can be
// statically proven that no rows from the given input will be duplicated,
// filtered, or null-extended respectively. As an example, take the following
// query:
//
//   SELECT * FROM xy INNER JOIN uv ON y = v;
//
// At execution time, it may be that every row from xy will be included in the
// join output exactly once. However, since this cannot be proven before
// runtime, the duplicated and filtered flags must be set.
//
// After initial construction by multiplicity_builder.go, Multiplicity should
// be considered immutable.
type Multiplicity struct {
	// UnfilteredCols contains all columns from the operator's input(s) that are
	// guaranteed to never have been filtered. Row duplication is allowed and
	// other columns from the same base table need not be included. This allows
	// the validity of properties from the base table to be verified (for example,
	// a foreign-key relation).
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

// JoinCanDuplicateLeftRows returns true when rows from the left input may be
// included in the join output more than once.
func (mp *Multiplicity) JoinCanDuplicateLeftRows() bool {
	return mp.LeftMultiplicity&DuplicatedMultiplicityVal != 0
}

// JoinCanDuplicateRightRows returns true when rows from the right input may be
// included in the join output more than once.
func (mp *Multiplicity) JoinCanDuplicateRightRows() bool {
	return mp.RightMultiplicity&DuplicatedMultiplicityVal != 0
}

// JoinCanFilterLeftRows returns true when rows from the left input may not be
// included in the join output.
func (mp *Multiplicity) JoinCanFilterLeftRows() bool {
	return mp.LeftMultiplicity&FilteredMultiplicityVal != 0
}

// JoinCanFilterRightRows returns true when rows from the right input may not be
// included in the join output.
func (mp *Multiplicity) JoinCanFilterRightRows() bool {
	return mp.RightMultiplicity&FilteredMultiplicityVal != 0
}

// JoinCanNullExtendLeftRows returns true if the join output may null-extend
// columns from the left input
func (mp *Multiplicity) JoinCanNullExtendLeftRows() bool {
	return mp.LeftMultiplicity&NullExtendedMultiplicityVal != 0
}

// JoinCanNullExtendRightRows returns true if the join output may null-extend
// columns from the right input.
func (mp *Multiplicity) JoinCanNullExtendRightRows() bool {
	return mp.RightMultiplicity&NullExtendedMultiplicityVal != 0
}

// String returns a formatted string containing flags for the left and right
// inputs that indicate whether rows can be duplicated or filtered.
// Null-extension is not included because the flag will always be set for an
// outer join and will never be set for an inner join, so it isn't very
// interesting.
func (mp *Multiplicity) String() string {
	var buf bytes.Buffer
	noDup := "no-dup"
	canDup := "can-dup"
	allPreserved := "all-preserved"
	canDiscard := "can-discard"

	isFirstFlag := true
	buf.WriteString("left-rows(")

	writeFlag := func(name string) {
		if !isFirstFlag {
			buf.WriteString(", ")
		}
		buf.WriteString(name)
		isFirstFlag = false
	}

	if mp.JoinCanDuplicateLeftRows() {
		writeFlag(canDup)
	} else {
		writeFlag(noDup)
	}
	if mp.JoinCanFilterLeftRows() {
		writeFlag(canDiscard)
	} else {
		writeFlag(allPreserved)
	}

	isFirstFlag = true
	buf.WriteString("), right-rows(")

	if mp.JoinCanDuplicateRightRows() {
		writeFlag(canDup)
	} else {
		writeFlag(noDup)
	}
	if mp.JoinCanFilterRightRows() {
		writeFlag(canDiscard)
	} else {
		writeFlag(allPreserved)
	}

	buf.WriteString(")")

	return buf.String()
}
