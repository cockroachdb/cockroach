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

func TestJoinMultiplicity_JoinDoesNotDuplicateLeftRows(t *testing.T) {
	require.Equal(t, false, bothIndeterminate.JoinDoesNotDuplicateLeftRows())
	require.Equal(t, true, bothNoDup.JoinDoesNotDuplicateLeftRows())
	require.Equal(t, false, bothPreserved.JoinDoesNotDuplicateLeftRows())
	require.Equal(t, true, bothNoDupBothPreserved.JoinDoesNotDuplicateLeftRows())
	require.Equal(t, false, leftIndeterminateRightPreserved.JoinDoesNotDuplicateLeftRows())
	require.Equal(t, false, leftIndeterminateRightNoDup.JoinDoesNotDuplicateLeftRows())
	require.Equal(t, false, rightIndeterminateLeftPreserved.JoinDoesNotDuplicateLeftRows())
	require.Equal(t, true, rightIndeterminateLeftNoDup.JoinDoesNotDuplicateLeftRows())
	require.Equal(t, true, bothNoDupLeftPreserved.JoinDoesNotDuplicateLeftRows())
	require.Equal(t, true, bothPreservedLeftNoDup.JoinDoesNotDuplicateLeftRows())
	require.Equal(t, true, bothNoDupRightPreserved.JoinDoesNotDuplicateLeftRows())
	require.Equal(t, false, bothPreservedRightNoDup.JoinDoesNotDuplicateLeftRows())
}

func TestJoinMultiplicity_JoinDoesNotDuplicateRightRows(t *testing.T) {
	require.Equal(t, false, bothIndeterminate.JoinDoesNotDuplicateRightRows())
	require.Equal(t, true, bothNoDup.JoinDoesNotDuplicateRightRows())
	require.Equal(t, false, bothPreserved.JoinDoesNotDuplicateRightRows())
	require.Equal(t, true, bothNoDupBothPreserved.JoinDoesNotDuplicateRightRows())
	require.Equal(t, false, leftIndeterminateRightPreserved.JoinDoesNotDuplicateRightRows())
	require.Equal(t, true, leftIndeterminateRightNoDup.JoinDoesNotDuplicateRightRows())
	require.Equal(t, false, rightIndeterminateLeftPreserved.JoinDoesNotDuplicateRightRows())
	require.Equal(t, false, rightIndeterminateLeftNoDup.JoinDoesNotDuplicateRightRows())
	require.Equal(t, true, bothNoDupLeftPreserved.JoinDoesNotDuplicateRightRows())
	require.Equal(t, false, bothPreservedLeftNoDup.JoinDoesNotDuplicateRightRows())
	require.Equal(t, true, bothNoDupRightPreserved.JoinDoesNotDuplicateRightRows())
	require.Equal(t, true, bothPreservedRightNoDup.JoinDoesNotDuplicateRightRows())
}

func TestJoinMultiplicity_JoinPreservesLeftRows(t *testing.T) {
	require.Equal(t, false, bothIndeterminate.JoinPreservesLeftRows())
	require.Equal(t, false, bothNoDup.JoinPreservesLeftRows())
	require.Equal(t, true, bothPreserved.JoinPreservesLeftRows())
	require.Equal(t, true, bothNoDupBothPreserved.JoinPreservesLeftRows())
	require.Equal(t, false, leftIndeterminateRightPreserved.JoinPreservesLeftRows())
	require.Equal(t, false, leftIndeterminateRightNoDup.JoinPreservesLeftRows())
	require.Equal(t, true, rightIndeterminateLeftPreserved.JoinPreservesLeftRows())
	require.Equal(t, false, rightIndeterminateLeftNoDup.JoinPreservesLeftRows())
	require.Equal(t, true, bothNoDupLeftPreserved.JoinPreservesLeftRows())
	require.Equal(t, true, bothPreservedLeftNoDup.JoinPreservesLeftRows())
	require.Equal(t, false, bothNoDupRightPreserved.JoinPreservesLeftRows())
	require.Equal(t, true, bothPreservedRightNoDup.JoinPreservesLeftRows())
}

func TestJoinMultiplicity_JoinPreservesRightRows(t *testing.T) {
	require.Equal(t, false, bothIndeterminate.JoinPreservesRightRows())
	require.Equal(t, false, bothNoDup.JoinPreservesRightRows())
	require.Equal(t, true, bothPreserved.JoinPreservesRightRows())
	require.Equal(t, true, bothNoDupBothPreserved.JoinPreservesRightRows())
	require.Equal(t, true, leftIndeterminateRightPreserved.JoinPreservesRightRows())
	require.Equal(t, false, leftIndeterminateRightNoDup.JoinPreservesRightRows())
	require.Equal(t, false, rightIndeterminateLeftPreserved.JoinPreservesRightRows())
	require.Equal(t, false, rightIndeterminateLeftNoDup.JoinPreservesRightRows())
	require.Equal(t, false, bothNoDupLeftPreserved.JoinPreservesRightRows())
	require.Equal(t, true, bothPreservedLeftNoDup.JoinPreservesRightRows())
	require.Equal(t, true, bothNoDupRightPreserved.JoinPreservesRightRows())
	require.Equal(t, true, bothPreservedRightNoDup.JoinPreservesRightRows())
}
