// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scattr

import (
	"reflect"
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
	expectedStr := `SequenceDependency: {DescID: 3, ReferencedDescID: 1, ColumnID: 2}`
	require.Equal(t, expectedStr, ToString(seqElem), "Attr string conversion is broken.")
	require.True(t, Equal(seqElem, seqElem))
	require.False(t, Equal(seqElem, seqElemDiff))

	// Sanity: Validate type references, then check if type comparisons
	// work.
	typeBackRef := &scpb.TypeReference{DescID: 1, TypeID: 3}
	expectedStr = `TypeReference: {DescID: 1, ReferencedDescID: 3}`
	require.Equal(t, expectedStr, ToString(typeBackRef), "Attr string conversion is broken.")
	require.False(t, Equal(seqElem, typeBackRef))
	require.False(t, Equal(typeBackRef, seqElem))

	// Sanity: Validate attribute fetching for both types.
	require.Equal(t, "3", Get(ReferencedDescID, typeBackRef).String())
	require.Equal(t, "1", Get(DescID, typeBackRef).String())
	require.Equal(t, "TypeReference", Get(Type, typeBackRef).String())
	require.Equal(t, "4", Get(ColumnID, seqElemDiff).String())
}

func BenchmarkCompareElements(b *testing.B) {
	var elements = []scpb.Container{
		&scpb.Column{},
		&scpb.PrimaryIndex{},
		&scpb.SecondaryIndex{},
		&scpb.SequenceDependency{},
		&scpb.UniqueConstraint{},
		&scpb.CheckConstraint{},
		&scpb.Sequence{},
		&scpb.DefaultExpression{},
		&scpb.View{},
		&scpb.TypeReference{},
		&scpb.Table{},
		&scpb.OutboundForeignKey{},
		&scpb.InboundForeignKey{},
		&scpb.RelationDependedOnBy{},
		&scpb.SequenceOwnedBy{},
		&scpb.Node{
			Target: scpb.NewTarget(scpb.Target_ADD, &scpb.Sequence{}),
		},
		scpb.NewTarget(scpb.Target_ADD, &scpb.Table{}),
	}
	for i := 0; i < int(float64(b.N)/float64(len(elements)*len(elements))); i++ {
		for _, a := range elements {
			for _, b := range elements {
				Compare(a, b)
			}
		}
	}
}

// TestElementAttributeValueTypesMatch ensure that for all elements which
// have a given Attr, that the values all have the same type.
func TestElementAttributeValueTypesMatch(t *testing.T) {
	typ := reflect.TypeOf((*scpb.ElementProto)(nil)).Elem()
	attributeMap := make(map[Attr]reflect.Type)
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		elem := reflect.New(f.Type.Elem()).Interface().(scpb.Element)
		for i := 0; i < numAttributes; i++ {
			attr := attributeOrder[i]
			av := Get(attr, elem)
			if av == nil {
				continue
			}
			avt := reflect.TypeOf(av)
			if exp, ok := attributeMap[attr]; ok {
				require.Equalf(t, exp, avt, "%v", attr)
			} else {
				attributeMap[attr] = avt
			}
		}
	}
}

// TestAllElementsHaveDescID ensures that all element types do carry an
// DescID.
func TestAllElementsHaveDescID(t *testing.T) {
	typ := reflect.TypeOf((*scpb.ElementProto)(nil)).Elem()
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		elem := reflect.New(f.Type.Elem()).Interface().(scpb.Element)
		require.NotNilf(t, Get(DescID, elem), "%s", f.Type.Elem())
	}
}
