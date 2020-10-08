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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/stretchr/testify/require"
)

var bothIndeterminate = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityIndeterminateVal,
	RightMultiplicity: MultiplicityIndeterminateVal,
}

var bothNoDup = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityNotDuplicatedVal,
	RightMultiplicity: MultiplicityNotDuplicatedVal,
}

var bothPreserved = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityPreservedVal,
	RightMultiplicity: MultiplicityPreservedVal,
}

var bothNoDupBothPreserved = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityNotDuplicatedVal | MultiplicityPreservedVal,
	RightMultiplicity: MultiplicityNotDuplicatedVal | MultiplicityPreservedVal,
}

var leftIndeterminateRightPreserved = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityIndeterminateVal,
	RightMultiplicity: MultiplicityPreservedVal,
}

var leftIndeterminateRightNoDup = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityIndeterminateVal,
	RightMultiplicity: MultiplicityNotDuplicatedVal,
}

var rightIndeterminateLeftPreserved = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityPreservedVal,
	RightMultiplicity: MultiplicityIndeterminateVal,
}

var rightIndeterminateLeftNoDup = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityNotDuplicatedVal,
	RightMultiplicity: MultiplicityIndeterminateVal,
}

var bothNoDupLeftPreserved = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityNotDuplicatedVal | MultiplicityPreservedVal,
	RightMultiplicity: MultiplicityNotDuplicatedVal,
}

var bothPreservedLeftNoDup = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityPreservedVal | MultiplicityNotDuplicatedVal,
	RightMultiplicity: MultiplicityPreservedVal,
}

var bothNoDupRightPreserved = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityNotDuplicatedVal,
	RightMultiplicity: MultiplicityNotDuplicatedVal | MultiplicityPreservedVal,
}

var bothPreservedRightNoDup = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityPreservedVal,
	RightMultiplicity: MultiplicityPreservedVal | MultiplicityNotDuplicatedVal,
}

func TestJoinMultiplicity_JoinFiltersDoNotDuplicateLeftRows(t *testing.T) {
	require.Equal(t, false, bothIndeterminate.JoinFiltersDoNotDuplicateLeftRows())
	require.Equal(t, true, bothNoDup.JoinFiltersDoNotDuplicateLeftRows())
	require.Equal(t, false, bothPreserved.JoinFiltersDoNotDuplicateLeftRows())
	require.Equal(t, true, bothNoDupBothPreserved.JoinFiltersDoNotDuplicateLeftRows())
	require.Equal(t, false, leftIndeterminateRightPreserved.JoinFiltersDoNotDuplicateLeftRows())
	require.Equal(t, false, leftIndeterminateRightNoDup.JoinFiltersDoNotDuplicateLeftRows())
	require.Equal(t, false, rightIndeterminateLeftPreserved.JoinFiltersDoNotDuplicateLeftRows())
	require.Equal(t, true, rightIndeterminateLeftNoDup.JoinFiltersDoNotDuplicateLeftRows())
	require.Equal(t, true, bothNoDupLeftPreserved.JoinFiltersDoNotDuplicateLeftRows())
	require.Equal(t, true, bothPreservedLeftNoDup.JoinFiltersDoNotDuplicateLeftRows())
	require.Equal(t, true, bothNoDupRightPreserved.JoinFiltersDoNotDuplicateLeftRows())
	require.Equal(t, false, bothPreservedRightNoDup.JoinFiltersDoNotDuplicateLeftRows())
}

func TestJoinMultiplicity_JoinFiltersDoNotDuplicateRightRows(t *testing.T) {
	require.Equal(t, false, bothIndeterminate.JoinFiltersDoNotDuplicateRightRows())
	require.Equal(t, true, bothNoDup.JoinFiltersDoNotDuplicateRightRows())
	require.Equal(t, false, bothPreserved.JoinFiltersDoNotDuplicateRightRows())
	require.Equal(t, true, bothNoDupBothPreserved.JoinFiltersDoNotDuplicateRightRows())
	require.Equal(t, false, leftIndeterminateRightPreserved.JoinFiltersDoNotDuplicateRightRows())
	require.Equal(t, true, leftIndeterminateRightNoDup.JoinFiltersDoNotDuplicateRightRows())
	require.Equal(t, false, rightIndeterminateLeftPreserved.JoinFiltersDoNotDuplicateRightRows())
	require.Equal(t, false, rightIndeterminateLeftNoDup.JoinFiltersDoNotDuplicateRightRows())
	require.Equal(t, true, bothNoDupLeftPreserved.JoinFiltersDoNotDuplicateRightRows())
	require.Equal(t, false, bothPreservedLeftNoDup.JoinFiltersDoNotDuplicateRightRows())
	require.Equal(t, true, bothNoDupRightPreserved.JoinFiltersDoNotDuplicateRightRows())
	require.Equal(t, true, bothPreservedRightNoDup.JoinFiltersDoNotDuplicateRightRows())
}

func TestJoinMultiplicity_JoinFiltersMatchAllLeftRows(t *testing.T) {
	require.Equal(t, false, bothIndeterminate.JoinFiltersMatchAllLeftRows())
	require.Equal(t, false, bothNoDup.JoinFiltersMatchAllLeftRows())
	require.Equal(t, true, bothPreserved.JoinFiltersMatchAllLeftRows())
	require.Equal(t, true, bothNoDupBothPreserved.JoinFiltersMatchAllLeftRows())
	require.Equal(t, false, leftIndeterminateRightPreserved.JoinFiltersMatchAllLeftRows())
	require.Equal(t, false, leftIndeterminateRightNoDup.JoinFiltersMatchAllLeftRows())
	require.Equal(t, true, rightIndeterminateLeftPreserved.JoinFiltersMatchAllLeftRows())
	require.Equal(t, false, rightIndeterminateLeftNoDup.JoinFiltersMatchAllLeftRows())
	require.Equal(t, true, bothNoDupLeftPreserved.JoinFiltersMatchAllLeftRows())
	require.Equal(t, true, bothPreservedLeftNoDup.JoinFiltersMatchAllLeftRows())
	require.Equal(t, false, bothNoDupRightPreserved.JoinFiltersMatchAllLeftRows())
	require.Equal(t, true, bothPreservedRightNoDup.JoinFiltersMatchAllLeftRows())
}

func TestJoinMultiplicity_JoinFiltersMatchAllRightRows(t *testing.T) {
	require.Equal(t, false, bothIndeterminate.JoinFiltersMatchAllRightRows())
	require.Equal(t, false, bothNoDup.JoinFiltersMatchAllRightRows())
	require.Equal(t, true, bothPreserved.JoinFiltersMatchAllRightRows())
	require.Equal(t, true, bothNoDupBothPreserved.JoinFiltersMatchAllRightRows())
	require.Equal(t, true, leftIndeterminateRightPreserved.JoinFiltersMatchAllRightRows())
	require.Equal(t, false, leftIndeterminateRightNoDup.JoinFiltersMatchAllRightRows())
	require.Equal(t, false, rightIndeterminateLeftPreserved.JoinFiltersMatchAllRightRows())
	require.Equal(t, false, rightIndeterminateLeftNoDup.JoinFiltersMatchAllRightRows())
	require.Equal(t, false, bothNoDupLeftPreserved.JoinFiltersMatchAllRightRows())
	require.Equal(t, true, bothPreservedLeftNoDup.JoinFiltersMatchAllRightRows())
	require.Equal(t, true, bothNoDupRightPreserved.JoinFiltersMatchAllRightRows())
	require.Equal(t, true, bothPreservedRightNoDup.JoinFiltersMatchAllRightRows())
}

func TestJoinMultiplicity_JoinDoesNotDuplicateLeftRows(t *testing.T) {
	require.Equal(t, false, bothIndeterminate.JoinDoesNotDuplicateLeftRows(opt.InnerJoinOp))
	require.Equal(t, false, bothIndeterminate.JoinDoesNotDuplicateLeftRows(opt.LeftJoinOp))
	require.Equal(t, false, bothIndeterminate.JoinDoesNotDuplicateLeftRows(opt.FullJoinOp))
	require.Equal(t, true, bothIndeterminate.JoinDoesNotDuplicateLeftRows(opt.SemiJoinOp))
	require.Equal(
		t, true, rightIndeterminateLeftNoDup.JoinDoesNotDuplicateLeftRows(opt.InnerJoinOp))
	require.Equal(
		t, true, rightIndeterminateLeftNoDup.JoinDoesNotDuplicateLeftRows(opt.LeftJoinOp))
	require.Equal(
		t, true, rightIndeterminateLeftNoDup.JoinDoesNotDuplicateLeftRows(opt.FullJoinOp))
	require.Equal(
		t, true, rightIndeterminateLeftNoDup.JoinDoesNotDuplicateLeftRows(opt.SemiJoinOp))
}

func TestJoinMultiplicity_JoinDoesNotDuplicateRightRows(t *testing.T) {
	require.Equal(t, false, bothIndeterminate.JoinDoesNotDuplicateRightRows(opt.InnerJoinOp))
	require.Equal(t, false, bothIndeterminate.JoinDoesNotDuplicateRightRows(opt.LeftJoinOp))
	require.Equal(t, false, bothIndeterminate.JoinDoesNotDuplicateRightRows(opt.FullJoinOp))
	require.Equal(
		t, true, leftIndeterminateRightNoDup.JoinDoesNotDuplicateRightRows(opt.InnerJoinOp))
	require.Equal(
		t, true, leftIndeterminateRightNoDup.JoinDoesNotDuplicateRightRows(opt.LeftJoinOp))
	require.Equal(
		t, true, leftIndeterminateRightNoDup.JoinDoesNotDuplicateRightRows(opt.FullJoinOp))
}

func TestJoinMultiplicity_JoinPreservesLeftRows(t *testing.T) {
	require.Equal(t, false, bothIndeterminate.JoinPreservesLeftRows(opt.InnerJoinOp))
	require.Equal(t, true, bothIndeterminate.JoinPreservesLeftRows(opt.LeftJoinOp))
	require.Equal(t, true, bothIndeterminate.JoinPreservesLeftRows(opt.FullJoinOp))
	require.Equal(t, false, bothIndeterminate.JoinPreservesLeftRows(opt.SemiJoinOp))
	require.Equal(
		t, true, rightIndeterminateLeftPreserved.JoinPreservesLeftRows(opt.InnerJoinOp))
	require.Equal(
		t, true, rightIndeterminateLeftPreserved.JoinPreservesLeftRows(opt.LeftJoinOp))
	require.Equal(
		t, true, rightIndeterminateLeftPreserved.JoinPreservesLeftRows(opt.FullJoinOp))
	require.Equal(
		t, true, rightIndeterminateLeftPreserved.JoinPreservesLeftRows(opt.SemiJoinOp))
}

func TestJoinMultiplicity_JoinPreservesRightRows(t *testing.T) {
	require.Equal(t, false, bothIndeterminate.JoinPreservesRightRows(opt.InnerJoinOp))
	require.Equal(t, false, bothIndeterminate.JoinPreservesRightRows(opt.LeftJoinOp))
	require.Equal(t, true, bothIndeterminate.JoinPreservesRightRows(opt.FullJoinOp))
	require.Equal(
		t, true, leftIndeterminateRightPreserved.JoinPreservesRightRows(opt.InnerJoinOp))
	require.Equal(
		t, true, leftIndeterminateRightPreserved.JoinPreservesRightRows(opt.LeftJoinOp))
	require.Equal(
		t, true, leftIndeterminateRightPreserved.JoinPreservesRightRows(opt.FullJoinOp))
}
