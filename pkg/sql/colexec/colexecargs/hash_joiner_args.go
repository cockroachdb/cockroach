// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecargs

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// HashJoinerSpec is the specification for a hash join operator. The hash joiner
// performs a join on the left and right's equal columns and returns combined
// left and right output columns.
type HashJoinerSpec struct {
	JoinType descpb.JoinType
	// Left and Right are the specifications of the two input table sources to
	// the hash joiner.
	Left  hashJoinerSourceSpec
	Right hashJoinerSourceSpec

	// TrackBuildMatches indicates whether or not we need to track if a row from
	// the build table had a match (this is needed with RIGHT/FULL OUTER, RIGHT
	// SEMI, and RIGHT ANTI joins).
	TrackBuildMatches bool

	// RightDistinct indicates whether or not the build table equality column
	// tuples are distinct. If they are distinct, performance can be optimized.
	RightDistinct bool
}

type hashJoinerSourceSpec struct {
	// EqCols specify the indices of the source tables equality column during
	// the hash join.
	EqCols []uint32

	// SourceTypes specify the types of the input columns of the source table
	// for the hash joiner.
	SourceTypes []*types.T
}

// MakeHashJoinerSpec creates a specification for columnar hash join operator.
// leftEqCols and rightEqCols specify the equality columns while leftOutCols and
// rightOutCols specifies the output columns. leftTypes and rightTypes specify
// the input column types of the two sources. rightDistinct indicates whether
// the equality columns of the right source form a key.
func MakeHashJoinerSpec(
	joinType descpb.JoinType,
	leftEqCols []uint32,
	rightEqCols []uint32,
	leftTypes []*types.T,
	rightTypes []*types.T,
	rightDistinct bool,
) HashJoinerSpec {
	switch joinType {
	case descpb.LeftSemiJoin:
		// In a left semi join, we don't need to store anything but a single row
		// per build row, since all we care about is whether a row on the left
		// matches any row on the right.
		//
		// Note that this is *not* the case if we have an ON condition, since
		// we'll also need to make sure that a row on the left passes the ON
		// condition with the row on the right to emit it. However, we don't
		// support ON conditions just yet. When we do, we'll have a separate
		// case for that.
		rightDistinct = true
	case descpb.LeftAntiJoin,
		descpb.RightAntiJoin,
		descpb.RightSemiJoin,
		descpb.IntersectAllJoin,
		descpb.ExceptAllJoin:
		// LEFT/RIGHT ANTI, RIGHT SEMI, INTERSECT ALL, and EXCEPT ALL joins
		// currently rely on the fact that ht.ProbeScratch.HeadID is populated
		// in order to perform the matching. However, HeadID is only populated
		// when the right side is considered to be non-distinct, so we override
		// that information here. Note that it forces these joins to be slower
		// than they could have been if they utilized the actual distinctness
		// information.
		// TODO(yuzefovich): refactor these joins to take advantage of the
		// actual distinctness information.
		rightDistinct = false
	}
	var trackBuildMatches bool
	switch joinType {
	case descpb.RightOuterJoin, descpb.FullOuterJoin,
		descpb.RightSemiJoin, descpb.RightAntiJoin:
		trackBuildMatches = true
	}

	left := hashJoinerSourceSpec{
		EqCols:      leftEqCols,
		SourceTypes: leftTypes,
	}
	right := hashJoinerSourceSpec{
		EqCols:      rightEqCols,
		SourceTypes: rightTypes,
	}
	return HashJoinerSpec{
		JoinType:          joinType,
		Left:              left,
		Right:             right,
		TrackBuildMatches: trackBuildMatches,
		RightDistinct:     rightDistinct,
	}
}
