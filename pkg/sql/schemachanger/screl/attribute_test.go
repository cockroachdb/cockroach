// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package screl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/stretchr/testify/require"
)

func TestGetAttribute(t *testing.T) {
	seqElem := &scpb.SequenceDependency{
		TableID:    1,
		ColumnID:   2,
		SequenceID: 3,
	}
	seqElemDiff := &scpb.SequenceDependency{
		TableID:    1,
		ColumnID:   4,
		SequenceID: 3,
	}

	// Sanity: Validate basic string conversion, equality,
	// and inequality.
	expectedStr := `SequenceDependency:{DescID: 3, ColumnID: 2, ReferencedDescID: 1}`
	require.Equal(t, expectedStr, ElementString(seqElem), "Attribute string conversion is broken.")
	require.True(t, EqualElements(seqElem, seqElem))
	require.False(t, EqualElements(seqElem, seqElemDiff))

	// Sanity: Validate type references, then check if type comparisons
	// work.
	viewDependsOnType := &scpb.ViewDependsOnType{TableID: 1, TypeID: 3}
	expectedStr = `ViewDependsOnType:{DescID: 1, ReferencedDescID: 3}`
	require.Equal(t, expectedStr, ElementString(viewDependsOnType), "Attribute string conversion is broken.")
	require.False(t, EqualElements(seqElem, viewDependsOnType))
	require.False(t, EqualElements(viewDependsOnType, seqElem))
}

func BenchmarkCompareElements(b *testing.B) {
	var elements = []scpb.Element{
		&scpb.Column{},
		&scpb.PrimaryIndex{},
		&scpb.SecondaryIndex{},
		&scpb.SequenceDependency{},
		&scpb.UniqueConstraint{},
		&scpb.CheckConstraint{},
		&scpb.Sequence{},
		&scpb.DefaultExpression{},
		&scpb.DefaultExprTypeReference{},
		&scpb.ComputedExprTypeReference{},
		&scpb.OnUpdateExprTypeReference{},
		&scpb.View{},
		&scpb.ViewDependsOnType{},
		&scpb.Table{},
		&scpb.ForeignKey{},
		&scpb.ForeignKeyBackReference{},
		&scpb.RelationDependedOnBy{},
		&scpb.SequenceOwnedBy{},
	}
	for i := 0; i < int(float64(b.N)/float64(len(elements)*len(elements))); i++ {
		for _, a := range elements {
			for _, b := range elements {
				CompareElements(a, b)
			}
		}
	}
}
