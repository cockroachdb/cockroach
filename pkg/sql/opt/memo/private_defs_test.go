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

package memo_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
)

func TestScanCanProvideOrdering(t *testing.T) {
	cat := testcat.New()
	_, err := cat.ExecuteDDL(
		"CREATE TABLE a (" +
			"k INT PRIMARY KEY, " +
			"i INT, " +
			"s STRING, " +
			"f FLOAT, " +
			"INDEX (i, k), " +
			"INDEX (s DESC) STORING(f))",
	)
	if err != nil {
		t.Fatal(err)
	}

	md := opt.NewMetadata()
	a := md.AddTable(cat.Table("a"))

	// INDEX (i, k)
	altIndex1 := 1
	// INDEX (s DESC) STORING(f)
	altIndex2 := 2

	k := opt.OrderingColumn(md.TableColumn(a, 0))
	i := opt.OrderingColumn(md.TableColumn(a, 1))
	s := opt.OrderingColumn(md.TableColumn(a, 2))
	f := opt.OrderingColumn(md.TableColumn(a, 3))

	testcases := []struct {
		index    int
		ordering opt.Ordering
		expected bool
	}{
		// Ordering is longer than index.
		{index: altIndex1, ordering: opt.Ordering{i, k, s}, expected: true},
		{index: altIndex1, ordering: opt.Ordering{k, i, s}, expected: false},

		// Index is longer than ordering.
		{index: altIndex1, ordering: opt.Ordering{i}, expected: true},
		{index: altIndex1, ordering: opt.Ordering{k}, expected: false},

		// Index contains descending column.
		{index: altIndex2, ordering: opt.Ordering{-s}, expected: true},
		{index: altIndex2, ordering: opt.Ordering{s}, expected: false},

		// Index contains storing column.
		{index: altIndex2, ordering: opt.Ordering{-s, k, f}, expected: true},
		{index: altIndex2, ordering: opt.Ordering{-s, k, i}, expected: false},
	}

	for _, tc := range testcases {
		var oc props.OrderingChoice
		for _, col := range tc.ordering {
			var colChoice props.OrderingColumnChoice
			colChoice.Descending = col.Descending()
			colChoice.Group.Add(int(col.ID()))
			oc.AppendCol(&colChoice)
		}

		def := &memo.ScanOpDef{Table: a, Index: tc.index}
		if def.CanProvideOrdering(md, &oc) != tc.expected {
			t.Errorf("expected %v, got %v", tc.expected, !tc.expected)
		}
	}
}
