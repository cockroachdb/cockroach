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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

// TestAllElementsHaveDescID ensures that all element types have a DescID.
func TestAllElementsHaveDescID(t *testing.T) {
	typ := reflect.TypeOf((*scpb.ElementProto)(nil)).Elem()
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		elem := reflect.New(f.Type.Elem()).Interface().(scpb.Element)
		require.Equal(t, descpb.ID(0), GetDescID(elem))
	}
}

func TestAllDescIDs(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    scpb.Element
		expected []catid.DescID
	}{
		{
			name: "schema parent",
			input: &scpb.SchemaParent{
				SchemaID:         1,
				ParentDatabaseID: 2,
			},
			expected: []catid.DescID{1, 2},
		},
		{
			name: "default expr",
			input: &scpb.ColumnDefaultExpression{
				TableID:  1,
				ColumnID: 10,
				Expression: scpb.Expression{
					Expr:            "foo",
					UsesTypeIDs:     []catid.DescID{2, 3},
					UsesSequenceIDs: []catid.DescID{4, 5},
				},
			},
			expected: []catid.DescID{1, 2, 3, 4, 5},
		},
		{
			name: "udf column",
			input: &scpb.ColumnType{
				TableID:  1,
				ColumnID: 10,
				TypeT: scpb.TypeT{
					Type:          types.Any,
					ClosedTypeIDs: []catid.DescID{2, 3},
				},
			},
			expected: []catid.DescID{1, 2, 3},
		},
		{
			name: "computed column",
			input: &scpb.ColumnType{
				TableID:  1,
				ColumnID: 10,
				TypeT: scpb.TypeT{
					Type:          types.Any,
					ClosedTypeIDs: []catid.DescID{2, 3},
				},
				ComputeExpr: &scpb.Expression{
					Expr:            "foo",
					UsesTypeIDs:     []catid.DescID{3, 4},
					UsesSequenceIDs: []catid.DescID{5, 6},
				},
			},
			expected: []catid.DescID{1, 2, 3, 4, 5, 6},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ElementsMatch(t, tc.expected, AllDescIDs(tc.input).Ordered())
		})
	}
}
