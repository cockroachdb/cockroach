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
	"github.com/cockroachdb/errors"
)

// MultiplicityValue is a bit field that describes whether a join's filters
// match all input rows at least once, as well as whether the filters match all
// input rows at most once.
type MultiplicityValue uint8

const (
	// MultiplicityIndeterminateVal indicates that no guarantees can be made about
	// the number of times any given input row will be matched by the join
	// filters.
	MultiplicityIndeterminateVal MultiplicityValue = 0

	// MultiplicityNotDuplicatedVal indicates that the join will not match any
	// input rows more than once.
	MultiplicityNotDuplicatedVal MultiplicityValue = 1 << (iota - 1)

	// MultiplicityPreservedVal indicates that the join filters will match all
	// left rows at least once.
	MultiplicityPreservedVal
)

// JoinMultiplicity stores properties that allow guarantees to be made about how
// a join will affect rows from its inputs. Logical join operators can be
// modeled as an inner join followed by some combination of additional
// operations. For example, outer joins can be modeled as inner joins with the
// unmatched rows added (null-extended) to the output. A semi-join can be
// modeled as a combination of inner join, project and distinct operators. And
// of course, an inner join can simply be modeled as an inner join.
//
// JoinMultiplicity stores properties for the hypothetical 'inner join' that is
// a part of any logical join operator. These properties are used to answer
// the aforementioned queries about how a join will affect the rows from its
// inputs. Left and right input rows can be duplicated and/or filtered by the
// join. As an example:
//
//   CREATE TABLE xy (x INT PRIMARY KEY, y INT);
//   CREATE TABLE uv (u INT PRIMARY KEY, v INT);
//   SELECT * FROM xy FULL JOIN uv ON x=u;
//
// 1. Are rows from xy or uv being duplicated by the join?
// 2. Are any rows being filtered from the join output?
//
// A JoinMultiplicity constructed for the join is able to answer questions of
// this type by calling a function like JoinDoesNotDuplicateLeftRows with the
// corresponding join operator type. The operator type is used to derive the
// final multiplicity properties from the inner join properties. This means that
// the properties stored in the JoinMultiplicity can be different from the
// output of the functions that return the final multiplicity properties; for
// example, a left join may preserve left rows even if its ON condition doesn't
// match all left rows.
//
// The not-duplicated and preserved flags are always unset for a join unless it
// can be statically proven that no rows from the given input will be duplicated
// or filtered respectively. As an example, take the following query:
//
//   SELECT * FROM xy INNER JOIN uv ON y = v;
//
// At execution time, it may be that every row from xy will be included in the
// join output exactly once. However, since this cannot be proven before
// runtime, neither the MultiplicityNotDuplicated nor the MultiplicityPreserved
// flags can be set.
//
// After initial construction by multiplicity_builder.go, JoinMultiplicity
// should be considered immutable.
type JoinMultiplicity struct {
	// LeftMultiplicity and RightMultiplicity describe how the left and right
	// input rows respectively would be affected by an inner join with the same
	// ON condition. As an example, using the tables from above:
	//
	//  SELECT * FROM xy FULL JOIN uv ON x=u;
	//
	// MultiplicityNotDuplicatedVal: both LeftMultiplicity and RightMultiplicity
	// would set the not-duplicated flag because the equality is between key
	// columns, which means that no row can match more than once.
	//
	// MultiplicityPreservedVal: neither field would set the preserved flag
	// because there is no relation between the x and u columns that would
	// guarantee that every row has a match on the equality condition. However,
	// because the full join would add back unmatched rows, JoinPreservesLeftRows
	// and JoinPreservesRightRows would return true.
	LeftMultiplicity  MultiplicityValue
	RightMultiplicity MultiplicityValue
}

// JoinFiltersDoNotDuplicateLeftRows returns true when the join filters are
// guaranteed to not return true for rows from the left input more than once.
func (mp *JoinMultiplicity) JoinFiltersDoNotDuplicateLeftRows() bool {
	return mp.LeftMultiplicity&MultiplicityNotDuplicatedVal != 0
}

// JoinFiltersDoNotDuplicateRightRows returns true when the join filters are
// guaranteed to not return true for rows from the right input more than once.
func (mp *JoinMultiplicity) JoinFiltersDoNotDuplicateRightRows() bool {
	return mp.RightMultiplicity&MultiplicityNotDuplicatedVal != 0
}

// JoinFiltersMatchAllLeftRows returns true when the join filters are guaranteed
// to return true for rows from the left input at least once.
func (mp *JoinMultiplicity) JoinFiltersMatchAllLeftRows() bool {
	return mp.LeftMultiplicity&MultiplicityPreservedVal != 0
}

// JoinFiltersMatchAllRightRows returns true when the join filters are
// guaranteed to return true for rows from the right input at least once.
func (mp *JoinMultiplicity) JoinFiltersMatchAllRightRows() bool {
	return mp.RightMultiplicity&MultiplicityPreservedVal != 0
}

// JoinDoesNotDuplicateLeftRows returns true when rows from the left input will
// not be included in the join output more than once.
func (mp *JoinMultiplicity) JoinDoesNotDuplicateLeftRows(op opt.Operator) bool {
	switch op {
	case opt.InnerJoinOp, opt.LeftJoinOp, opt.FullJoinOp:
		break

	case opt.SemiJoinOp:
		return true

	default:
		panic(errors.AssertionFailedf("unsupported operator: %v", op))
	}
	return mp.JoinFiltersDoNotDuplicateLeftRows()
}

// JoinDoesNotDuplicateRightRows returns true when rows from the right input
// will not be included in the join output more than once.
func (mp *JoinMultiplicity) JoinDoesNotDuplicateRightRows(op opt.Operator) bool {
	switch op {
	case opt.InnerJoinOp, opt.LeftJoinOp, opt.FullJoinOp:
		break

	case opt.SemiJoinOp:
		panic(errors.AssertionFailedf("right rows are not included in the output of a %v", op))

	default:
		panic(errors.AssertionFailedf("unsupported operator: %v", op))
	}
	return mp.JoinFiltersDoNotDuplicateRightRows()
}

// JoinPreservesLeftRows returns true when all rows from the left input are
// guaranteed to be included in the join output.
func (mp *JoinMultiplicity) JoinPreservesLeftRows(op opt.Operator) bool {
	switch op {
	case opt.InnerJoinOp, opt.SemiJoinOp:
		break

	case opt.LeftJoinOp, opt.FullJoinOp:
		return true

	default:
		panic(errors.AssertionFailedf("unsupported operator: %v", op))
	}
	return mp.JoinFiltersMatchAllLeftRows()
}

// JoinPreservesRightRows returns true when all rows from the right input are
// guaranteed to be included in the join output.
func (mp *JoinMultiplicity) JoinPreservesRightRows(op opt.Operator) bool {
	switch op {
	case opt.InnerJoinOp, opt.LeftJoinOp:
		break

	case opt.FullJoinOp:
		return true

	case opt.SemiJoinOp:
		panic(errors.AssertionFailedf("right rows are not included in the output of a %v", op))

	default:
		panic(errors.AssertionFailedf("unsupported operator: %v", op))
	}
	return mp.JoinFiltersMatchAllRightRows()
}

// Format returns a formatted string containing flags that describe the
// multiplicity properties of the join, taking into account the join type.
func (mp *JoinMultiplicity) Format(op opt.Operator) string {
	switch op {
	case opt.InnerJoinOp, opt.LeftJoinOp, opt.FullJoinOp, opt.SemiJoinOp:

	default:
		panic(errors.AssertionFailedf("unsupported operator: %v", op))
	}

	if !mp.isInteresting(op) {
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
	outputFlag(mp.JoinDoesNotDuplicateLeftRows(op), mp.JoinPreservesLeftRows(op))
	buf.WriteString(")")

	if op != opt.SemiJoinOp {
		isFirstFlag = true
		buf.WriteString(", right-rows(")
		outputFlag(mp.JoinDoesNotDuplicateRightRows(op), mp.JoinPreservesRightRows(op))
		buf.WriteString(")")
	}

	return buf.String()
}

// String returns a formatted string containing flags for the left and right
// inputs that indicate how many times any given input row is guaranteed to
// match on the join filters.
func (mp *JoinMultiplicity) String() string {
	return mp.Format(opt.InnerJoinOp)
}

// isInteresting returns true when the multiplicity properties of an operator
// differ from the default for that operator. For example, left rows being
// preserved is interesting for an inner join or semi join, but not for a left
// join or full join.
func (mp *JoinMultiplicity) isInteresting(op opt.Operator) bool {
	switch op {
	case opt.InnerJoinOp:
		return mp.JoinFiltersDoNotDuplicateLeftRows() || mp.JoinFiltersDoNotDuplicateRightRows() ||
			mp.JoinFiltersMatchAllLeftRows() || mp.JoinFiltersMatchAllRightRows()

	case opt.LeftJoinOp:
		return mp.JoinFiltersDoNotDuplicateLeftRows() || mp.JoinFiltersDoNotDuplicateRightRows() ||
			mp.JoinFiltersMatchAllRightRows()

	case opt.FullJoinOp:
		return mp.JoinFiltersDoNotDuplicateLeftRows() || mp.JoinFiltersDoNotDuplicateRightRows()

	case opt.SemiJoinOp:
		return mp.JoinFiltersMatchAllLeftRows()

	default:
		panic(errors.AssertionFailedf("invalid operator: %v", op))
	}
}
