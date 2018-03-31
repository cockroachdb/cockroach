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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
)

func TestAltIndexHasCols(t *testing.T) {
	cat := createDefsCatalog(t)
	md := opt.NewMetadata()
	a := md.AddTable(cat.Table("a"))

	// INDEX (i, k)
	altIndex1 := 1
	// INDEX (s DESC) STORING(f)
	altIndex2 := 2

	k := int(md.TableColumn(a, 0))
	i := int(md.TableColumn(a, 1))
	s := int(md.TableColumn(a, 2))

	test := func(def *memo.ScanOpDef, altIndex int, expected bool) {
		t.Helper()
		if def.AltIndexHasCols(md, altIndex) != expected {
			t.Errorf("expected %v, got %v", expected, !expected)
		}
	}

	def := &memo.ScanOpDef{Table: a, Index: opt.PrimaryIndex, Cols: util.MakeFastIntSet(k, i)}
	test(def, altIndex1, true)
	test(def, altIndex2, false)

	def = &memo.ScanOpDef{Table: a, Index: opt.PrimaryIndex, Cols: util.MakeFastIntSet(k)}
	test(def, altIndex1, true)
	test(def, altIndex2, true)

	def = &memo.ScanOpDef{Table: a, Index: opt.PrimaryIndex, Cols: util.MakeFastIntSet(s)}
	test(def, altIndex1, false)
	test(def, altIndex2, true)

	def = &memo.ScanOpDef{Table: a, Index: opt.PrimaryIndex, Cols: util.MakeFastIntSet(k, i, s)}
	test(def, altIndex1, false)
	test(def, altIndex2, false)
}

func TestCanProvideOrdering(t *testing.T) {
	cat := createDefsCatalog(t)
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

	test := func(def *memo.ScanOpDef, ordering memo.Ordering, expected bool) {
		t.Helper()
		if def.CanProvideOrdering(md, ordering) != expected {
			t.Errorf("expected %v, got %v", expected, !expected)
		}
	}

	// Ordering is longer than index.
	def := &memo.ScanOpDef{Table: a, Index: altIndex1}
	test(def, memo.Ordering{i, k, s}, true)
	test(def, memo.Ordering{k, i, s}, false)

	// Index is longer than ordering.
	test(def, memo.Ordering{i}, true)
	test(def, memo.Ordering{k}, false)

	// Index contains descending column.
	def = &memo.ScanOpDef{Table: a, Index: altIndex2}
	test(def, memo.Ordering{-s}, true)
	test(def, memo.Ordering{s}, false)

	// Index contains storing column.
	test(def, memo.Ordering{-s, k, f}, true)
	test(def, memo.Ordering{-s, k, i}, false)
}

func createDefsCatalog(t *testing.T) *testutils.TestCatalog {
	cat := testutils.NewTestCatalog()
	testutils.ExecuteTestDDL(
		t,
		"CREATE TABLE a ("+
			"k INT PRIMARY KEY, "+
			"i INT, "+
			"s STRING, "+
			"f FLOAT, "+
			"INDEX (i, k), "+
			"INDEX (s DESC) STORING(f))",
		cat,
	)
	return cat
}
