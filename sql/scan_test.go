// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// Test that we can resolve the qnames in an expression that has already been
// resolved.
func TestRetryResolveQNames(t *testing.T) {
	defer leaktest.AfterTest(t)

	expr, err := parser.ParseExprTraditional(`COUNT(a)`)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		s := &scanNode{}
		s.desc = testTableDesc()
		s.visibleCols = s.desc.Columns
		if err := s.desc.AllocateIDs(); err != nil {
			t.Fatal(err)
		}

		_, err = s.resolveQNames(expr)
		if err != nil {
			t.Fatal(err)
		}
		if len(s.qvals) != 1 {
			t.Fatalf("%d: expected 1 qvalue, but found %d", i, len(s.qvals))
		}
		if _, ok := s.qvals[ColumnID(1)]; !ok {
			t.Fatalf("%d: unable to find qvalue for column ID 1", i)
		}
	}
}
