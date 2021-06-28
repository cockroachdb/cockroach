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
	seqElem := &SequenceDependency{
		TableID:    1,
		ColumnID:   2,
		SequenceID: 3,
	}
	seqElemDiff := &SequenceDependency{
		TableID:    1,
		ColumnID:   4,
		SequenceID: 3,
	}

	// Sanity: Validate basic string conversion, equality,
	// and inequality.
	expectedStr := `SequenceDependency:{DescID: 3, ReferencedDescID: 1, ColumnID: 2}`
	require.Equal(t, expectedStr, AttributesString(seqElem), "Attribute string conversion is broken.")
	require.True(t, EqualElements(seqElem, seqElem))
	require.False(t, EqualElements(seqElem, seqElemDiff))

	// Sanity: Validate type references, then check if type comparisons
	// work.
	typeBackRef := &TypeReference{DescID: 1, TypeID: 3}
	expectedStr = `TypeReference:{DescID: 1, ReferencedDescID: 3}`
	require.Equal(t, expectedStr, AttributesString(typeBackRef), "Attribute string conversion is broken.")
	require.False(t, EqualElements(seqElem, typeBackRef))
	require.False(t, EqualElements(typeBackRef, seqElem))

	// Sanity: Validate attribute fetching for both types.
	require.Equal(t, "3", typeBackRef.getAttribute(AttributeReferencedDescID).String())
	require.Equal(t, "1", typeBackRef.getAttribute(AttributeDescID).String())
	require.Equal(t, "TypeReference", typeBackRef.getAttribute(AttributeType).String())
	require.Equal(t, "4", seqElemDiff.getAttribute(AttributeColumnID).String())
}

func BenchmarkCompareElements(b *testing.B) {
	var elements = []Element{
		&Column{},
		&PrimaryIndex{},
		&SecondaryIndex{},
		&SequenceDependency{},
		&UniqueConstraint{},
		&CheckConstraint{},
		&Sequence{},
		&DefaultExpression{},
		&View{},
		&TypeReference{},
		&Table{},
		&OutboundForeignKey{},
		&InboundForeignKey{},
		&RelationDependedOnBy{},
		&SequenceOwnedBy{},
	}
	for i := 0; i < int(float64(b.N)/float64(len(elements)*len(elements))); i++ {
		for _, a := range elements {
			for _, b := range elements {
				CompareElements(a, b)
			}
		}
	}
}
