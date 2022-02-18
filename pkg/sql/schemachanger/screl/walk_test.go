// Copyright 2022 The Cockroach Authors.
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
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

// TestWalk test that walk works for all known elements at the time of writing.
// It also verifies that it never barfs on all on the elements. Given the
// constrained nature of its use, it's pretty confidence inspiring that it works
// as advertised.
func TestWalk(t *testing.T) {
	// Sanity check that we don't panic or anything dumb on all the elements.
	t.Run("all elements work", func(t *testing.T) {
		typ := reflect.TypeOf((*scpb.ElementProto)(nil)).Elem()
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			elem := reflect.New(f.Type.Elem()).Interface().(scpb.Element)
			require.NoError(t, WalkDescIDs(elem, func(id *catid.DescID) error { return nil }))
			require.NoError(t, WalkTypes(elem, func(id *types.T) error { return nil }))
			require.NoError(t, WalkExpressions(elem, func(id *catpb.Expression) error { return nil }))
		}
	})

	t.Run("errors propagate", func(t *testing.T) {
		require.EqualError(t, WalkDescIDs(&scpb.Column{}, func(id *catid.DescID) error {
			return errors.New("boom")
		}), "boom")
	})

	// Check that the values are sane.
	type testCase struct {
		elem           scpb.Element
		expIDs         []*catid.DescID
		expTypes       []*types.T
		expExpressions []*catpb.Expression
	}
	for _, tc := range []testCase{
		func() testCase {
			v := scpb.ColumnDefaultExpression{Expression: scpb.Expression{UsesSequenceIDs: []catid.DescID{1, 2}}}
			return testCase{
				elem:           &v,
				expIDs:         []*catid.DescID{&v.TableID, &v.UsesSequenceIDs[0], &v.UsesSequenceIDs[1]},
				expExpressions: []*catpb.Expression{&v.Expression.Expr},
			}
		}(),
		func() testCase {
			v := scpb.ColumnType{ComputeExpr: &scpb.Expression{}, TypeT: scpb.TypeT{Type: types.Timestamp}}
			return testCase{
				elem:           &v,
				expIDs:         []*catid.DescID{&v.TableID},
				expTypes:       []*types.T{types.Timestamp},
				expExpressions: []*catpb.Expression{&v.ComputeExpr.Expr},
			}
		}(),
		func() testCase {
			v := scpb.PrimaryIndex{}
			return testCase{
				elem:   &v,
				expIDs: []*catid.DescID{&v.TableID},
			}
		}(),
		func() testCase {
			v := scpb.SecondaryIndex{}
			return testCase{
				elem:   &v,
				expIDs: []*catid.DescID{&v.TableID},
			}
		}(),
		func() testCase {
			v := scpb.UniqueWithoutIndexConstraint{}
			return testCase{
				elem:   &v,
				expIDs: []*catid.DescID{&v.TableID},
			}
		}(),
		func() testCase {
			v := scpb.CheckConstraint{}
			return testCase{
				elem:           &v,
				expIDs:         []*catid.DescID{&v.TableID},
				expExpressions: []*catpb.Expression{&v.Expr},
			}
		}(),
		func() testCase {
			v := scpb.Sequence{}
			return testCase{
				elem:   &v,
				expIDs: []*catid.DescID{&v.SequenceID},
			}
		}(),
		func() testCase {
			v := scpb.View{}
			return testCase{
				elem:   &v,
				expIDs: []*catid.DescID{&v.ViewID},
			}
		}(),
		func() testCase {
			v := scpb.Table{}
			return testCase{
				elem:   &v,
				expIDs: []*catid.DescID{&v.TableID},
			}
		}(),
		func() testCase {
			v := scpb.ForeignKeyConstraint{}
			return testCase{
				elem:   &v,
				expIDs: []*catid.DescID{&v.TableID, &v.ReferencedTableID},
			}
		}(),
		func() testCase {
			v := scpb.SequenceOwner{}
			return testCase{
				elem:   &v,
				expIDs: []*catid.DescID{&v.SequenceID, &v.TableID},
			}
		}(),
	} {
		t.Run(fmt.Sprintf("%T", tc.elem), func(t *testing.T) {
			t.Run("DescIDs", func(t *testing.T) {
				var got []*catid.DescID
				require.NoError(t, WalkDescIDs(tc.elem, func(id *catid.DescID) error {
					got = append(got, id)
					return nil
				}))
				require.Equal(t, tc.expIDs, got)
			})
			t.Run("Types", func(t *testing.T) {
				var got []*types.T
				require.NoError(t, WalkTypes(tc.elem, func(t *types.T) error {
					got = append(got, t)
					return nil
				}))
				require.Equal(t, tc.expTypes, got)
			})
			t.Run("Expressions", func(t *testing.T) {
				var got []*catpb.Expression
				require.NoError(t, WalkExpressions(tc.elem, func(t *catpb.Expression) error {
					got = append(got, t)
					return nil
				}))
				require.Equal(t, tc.expExpressions, got)
			})
		})
	}
}
