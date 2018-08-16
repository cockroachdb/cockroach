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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestScanCanProvideOrdering(t *testing.T) {
	cat := testcat.New()
	_, err := cat.ExecuteDDL(
		"CREATE TABLE a (" +
			"k INT PRIMARY KEY, " +
			"i INT, " +
			"s STRING, " +
			"f FLOAT, " +
			"INDEX (i, k) STORING(f), " +
			"UNIQUE INDEX (s DESC, f))",
	)
	if err != nil {
		t.Fatal(err)
	}

	var md opt.Metadata
	a := md.AddTable(cat.Table(tree.NewUnqualifiedTableName("a")))

	// PRIMARY KEY (k)
	primary := 0
	// INDEX (i, k) STORING(f)
	altIndex1 := 1
	// INDEX (s DESC, f)
	altIndex2 := 2

	testcases := []struct {
		index    int
		ordering string
		expected string // "no", "fwd", or "rev".
	}{
		{index: primary, ordering: "", expected: "fwd"},
		{index: primary, ordering: "+1", expected: "fwd"},
		{index: primary, ordering: "-1", expected: "rev"},
		{index: primary, ordering: "+1,+2", expected: "no"},
		{index: primary, ordering: "-1,-2", expected: "no"},
		{index: altIndex1, ordering: "", expected: "fwd"},
		{index: altIndex1, ordering: "+1 opt(2)", expected: "fwd"},
		{index: altIndex1, ordering: "-1 opt(2)", expected: "rev"},
		{index: altIndex1, ordering: "-2,+1", expected: "no"},
		{index: altIndex2, ordering: "", expected: "fwd"},
		{index: altIndex2, ordering: "-3,+(4|1)", expected: "fwd"},
		{index: altIndex2, ordering: "-3,+4,+1", expected: "fwd"},
		{index: altIndex2, ordering: "+3,-4,-1", expected: "rev"},
		{index: altIndex2, ordering: "+3,+4,+1", expected: "no"},
	}

	for _, tc := range testcases {
		def := &memo.ScanOpDef{Table: a, Index: tc.index}
		required := props.ParseOrderingChoice(tc.ordering)
		ok, reverse := def.CanProvideOrdering(&md, &required)
		res := "no"
		if ok {
			if reverse {
				res = "rev"
			} else {
				res = "fwd"
			}
		}

		if res != tc.expected {
			t.Errorf("index: %d, required: %s, expected %v, got %v", tc.index, tc.ordering, tc.expected, res)
		}
	}
}
