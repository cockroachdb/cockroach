// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetAttribute(t *testing.T) {
	seqElem := SequenceDependency{
		TableID:    1,
		ColumnID:   2,
		SequenceID: 3,
	}
	seqElemDiff := SequenceDependency{
		TableID:    1,
		ColumnID:   4,
		SequenceID: 3,
	}

	// Sanity: Validate basic string conversion, equality,
	// and inequality.
	expectedStr := `SequenceDependency:{DescID: 3, DepID: 1, ColumnID: 2}`
	require.Equal(t, expectedStr, seqElem.GetAttributes().String(), "Attribute string conversion is broken.")
	require.True(t, seqElem.GetAttributes().Equal(seqElem.GetAttributes()))
	require.False(t, seqElem.GetAttributes().Equal(seqElemDiff.GetAttributes()))

	// Sanity: Validate type references, then check if type comparisons
	// work.
	typeBackRef := TypeReference{TypeID: 3, DescID: 1}
	expectedStr = `TypeReference:{DescID: 3, DepID: 1}`
	require.Equal(t, expectedStr, typeBackRef.GetAttributes().String(), "Attribute string conversion is broken.")
	require.False(t, seqElem.GetAttributes().Equal(typeBackRef.GetAttributes()))
	require.False(t, typeBackRef.GetAttributes().Equal(seqElem.GetAttributes()))

	// Sanity: Validate attribute fetching for both types.
	require.Equal(t, "1", typeBackRef.GetAttributes().Get(AttributeDepID).String())
	require.Equal(t, "3", typeBackRef.GetAttributes().Get(AttributeDescID).String())
	require.Equal(t, "TypeReference", typeBackRef.GetAttributes().Get(AttributeType).String())
	require.Equal(t, "4", seqElemDiff.GetAttributes().Get(AttributeColumnID).String())
}
