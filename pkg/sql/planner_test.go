// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"testing"

	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestTypeAsString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := planner{}
	testData := []struct {
		expr        parser.Expr
		expected    string
		expectedErr bool
	}{
		{expr: parser.NewDString("foo"), expected: "foo"},
		{
			expr: &parser.BinaryExpr{
				Operator: parser.Concat, Left: parser.NewDString("foo"), Right: parser.NewDString("bar")},
			expected: "foobar",
		},
		{expr: parser.NewDInt(3), expectedErr: true},
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
			fn, err := p.TypeAsStringArray([]parser.Expr{td.expr, td.expr}, "test")
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
