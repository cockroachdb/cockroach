// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package screl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/stretchr/testify/require"
)

func TestGetAttribute(t *testing.T) {
	cn := &scpb.ColumnName{
		TableID:  1,
		ColumnID: 2,
		Name:     "foo",
	}
	cnDiff := &scpb.ColumnName{
		TableID:  1,
		ColumnID: 4,
		Name:     "foo",
	}

	// Sanity: Validate basic string conversion, equality,
	// and inequality.
	expectedStr := `ColumnName:{DescID: 1, Name: foo, ColumnID: 2}`
	require.Equal(t, expectedStr, ElementString(cn), "Attribute string conversion is broken.")
	require.True(t, EqualElementKeys(cn, cn))
	require.False(t, EqualElementKeys(cn, cnDiff))

	// Sanity: Validate type references, then check if type comparisons
	// work.
	so := &scpb.SequenceOwner{TableID: 1, ColumnID: 2, SequenceID: 3}
	expectedStr = `SequenceOwner:{DescID: 1, ColumnID: 2, ReferencedDescID: 3}`
	require.Equal(t, expectedStr, ElementString(so), "Attribute string conversion is broken.")
	require.False(t, EqualElementKeys(so, cn))
	require.False(t, EqualElementKeys(so, cnDiff))
}

func BenchmarkCompareElements(b *testing.B) {
	var elements = []scpb.Element{
		&scpb.Column{},
		&scpb.PrimaryIndex{},
		&scpb.SecondaryIndex{},
		&scpb.CheckConstraint{},
		&scpb.Sequence{},
		&scpb.View{},
		&scpb.Table{},
	}
	for i := 0; i < int(float64(b.N)/float64(len(elements)*len(elements))); i++ {
		for _, a := range elements {
			for _, b := range elements {
				Schema.CompareOn(equalityAttrs, a, b)
			}
		}
	}
}
