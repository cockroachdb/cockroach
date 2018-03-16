// Copyright 2018 The Cockroach Authors.
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
// This file implements data structures used by index constraints generation.

package constraint

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestSpans(t *testing.T) {
	keyCtx := testKeyContext(1)
	var s Spans
	check := func(exp string) {
		if actual := s.String(); actual != exp {
			t.Errorf("expected %s, got %s", exp, actual)
		}
	}
	add := func(x int) {
		k := MakeKey(tree.NewDInt(tree.DInt(x)))
		var span Span
		span.Set(keyCtx, k, IncludeBoundary, k, IncludeBoundary)
		s.Append(&span)
	}
	check("")
	add(1)
	check("[/1 - /1]")
	add(2)
	check("[/1 - /1] [/2 - /2]")
	add(3)
	check("[/1 - /1] [/2 - /2] [/3 - /3]")
	s.Truncate(2)
	check("[/1 - /1] [/2 - /2]")
	s.Truncate(1)
	check("[/1 - /1]")
	s.Truncate(0)
	check("")

	add(3)
	add(1)
	add(2)
	add(1)
	add(4)
	add(3)
	s.SortAndDedup(keyCtx)
	check("[/1 - /1] [/2 - /2] [/3 - /3] [/4 - /4]")
	s.SortAndDedup(keyCtx)
	check("[/1 - /1] [/2 - /2] [/3 - /3] [/4 - /4]")
	add(4)
	add(5)
	add(5)
	s.SortAndDedup(keyCtx)
	check("[/1 - /1] [/2 - /2] [/3 - /3] [/4 - /4] [/5 - /5]")
}
