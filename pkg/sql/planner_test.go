// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestTypeAsString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := planner{}
	testData := []struct {
		expr        tree.Expr
		expected    string
		expectedErr bool
	}{
		{expr: tree.NewDString("foo"), expected: "foo"},
		{
			expr: &tree.BinaryExpr{
				Operator: tree.Concat, Left: tree.NewDString("foo"), Right: tree.NewDString("bar")},
			expected: "foobar",
		},
		{expr: tree.NewDInt(3), expectedErr: true},
	}

	t.Run("TypeAsString", func(t *testing.T) {
		for _, td := range testData {
			fn, err := p.TypeAsString(td.expr, "test")
			if err != nil {
				if !td.expectedErr {
					t.Fatalf("expected no error; got %v", err)
				}
				continue
			} else if td.expectedErr {
				t.Fatal("expected error; got none")
			}
			s, err := fn()
			if err != nil {
				t.Fatal(err)
			}
			if s != td.expected {
				t.Fatalf("expected %s; got %s", td.expected, s)
			}
		}
	})

	t.Run("TypeAsStringArray", func(t *testing.T) {
		for _, td := range testData {
			fn, err := p.TypeAsStringArray([]tree.Expr{td.expr, td.expr}, "test")
			if err != nil {
				if !td.expectedErr {
					t.Fatalf("expected no error; got %v", err)
				}
				continue
			} else if td.expectedErr {
				t.Fatal("expected error; got none")
			}
			a, err := fn()
			if err != nil {
				t.Fatal(err)
			}
			expected := []string{td.expected, td.expected}
			if !reflect.DeepEqual(a, expected) {
				t.Fatalf("expected %s; got %s", expected, a)
			}
		}
	})
}
