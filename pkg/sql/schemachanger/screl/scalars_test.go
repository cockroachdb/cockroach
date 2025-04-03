// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package screl

import (
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	types "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

// TestAllElementsHaveDescID ensures that all element types have a DescID.
func TestAllElementsHaveDescID(t *testing.T) {
	forEachNewElementType(t, func(elem scpb.Element) {
		require.Equalf(t, descpb.ID(0), GetDescID(elem), "elem %T", elem)
	})
}

func TestAllElementsHaveMinVersion(t *testing.T) {
	forEachNewElementType(t, func(elem scpb.Element) {
		// If `elem` does not have a min version, the following function call will panic.
		VersionSupportsElementUse(elem, clusterversion.ClusterVersion{})
	})
}

// ForEachElement executes a function for each element type.
func forEachNewElementType(t *testing.T, fn func(element scpb.Element)) {
	require.NoError(t,
		scpb.ForEachElementType(func(e scpb.Element) error {
			newElem := reflect.New(reflect.TypeOf(e).Elem())
			fn(newElem.Interface().(scpb.Element))
			return nil
		}))
}

func TestAllDescIDsAndContainsDescID(t *testing.T) {
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
					Type:          types.AnyElement,
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
					Type:          types.AnyElement,
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
			for _, id := range tc.expected {
				require.Truef(t, ContainsDescID(tc.input, id), "contains %d", id)
			}
			require.False(t, ContainsDescID(tc.input, 0))
			require.False(t, ContainsDescID(tc.input, math.MaxUint32))
		})
	}
}
