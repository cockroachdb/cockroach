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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
)

func TestCanProvideOrdering(t *testing.T) {
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

	test := func(def *memo.ScanOpDef, ordering opt.Ordering, expected bool) {
		t.Helper()
		if def.CanProvideOrdering(md, ordering) != expected {
			t.Errorf("expected %v, got %v", expected, !expected)
		}
	}

	// Ordering is longer than index.
	def := &memo.ScanOpDef{Table: a, Index: altIndex1}
	test(def, opt.Ordering{i, k, s}, true)
	test(def, opt.Ordering{k, i, s}, false)

	// Index is longer than ordering.
	test(def, opt.Ordering{i}, true)
	test(def, opt.Ordering{k}, false)

	// Index contains descending column.
	def = &memo.ScanOpDef{Table: a, Index: altIndex2}
	test(def, opt.Ordering{-s}, true)
	test(def, opt.Ordering{s}, false)

	// Index contains storing column.
	test(def, opt.Ordering{-s, k, f}, true)
	test(def, opt.Ordering{-s, k, i}, false)
}
