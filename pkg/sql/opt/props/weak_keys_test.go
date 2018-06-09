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

package props_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/util"
)

func TestMetadataWeakKeys(t *testing.T) {
	test := func(weakKeys props.WeakKeys, expected string) {
		t.Helper()
		actual := fmt.Sprintf("%v", weakKeys)
		if actual != expected {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	}

	testContains := func(weakKeys props.WeakKeys, cs opt.ColSet, expected bool) {
		t.Helper()
		actual := weakKeys.ContainsSubsetOf(cs)
		if actual != expected {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	}

	md := opt.NewMetadata()

	// Create table with the following interesting indexes:
	//   1. Primary key index with multiple columns.
	//   2. Single column index.
	//   3. Storing values (should have no impact).
	//   4. Non-unique index (should always be superset of primary key).
	//   5. Unique index that has subset of cols of another unique index, but
	//      which is defined afterwards (triggers removal of previous weak key).
	cat := testcat.New()
	_, err := cat.ExecuteDDL(
		"CREATE TABLE a (" +
			"k INT, " +
			"i INT, " +
			"d DECIMAL, " +
			"f FLOAT, " +
			"s STRING, " +
			"PRIMARY KEY (k, i), " +
			"UNIQUE INDEX (f) STORING (s, i)," +
			"UNIQUE INDEX (d DESC, i, s)," +
			"UNIQUE INDEX (d, i DESC) STORING (f)," +
			"INDEX (s DESC, i))")
	if err != nil {
		t.Fatal(err)
	}
	a := md.AddTable(cat.Table("a"))

	wk := makeTableWeakKeys(md, a)
	test(wk, "[(1,2) (4) (2,3)]")

	// Test ContainsSubsetOf method.
	testContains(wk, util.MakeFastIntSet(1, 2), true)
	testContains(wk, util.MakeFastIntSet(1, 2, 3), true)
	testContains(wk, util.MakeFastIntSet(4), true)
	testContains(wk, util.MakeFastIntSet(4, 3, 2, 1), true)
	testContains(wk, util.MakeFastIntSet(1), false)
	testContains(wk, util.MakeFastIntSet(1, 3), false)
	testContains(wk, util.MakeFastIntSet(5), false)

	// Add additional weak keys to additionally verify Add method.
	wk.Add(util.MakeFastIntSet(1, 2, 3))
	test(wk, "[(1,2) (4) (2,3)]")

	wk.Add(util.MakeFastIntSet(2, 1))
	test(wk, "[(1,2) (4) (2,3)]")

	wk.Add(util.MakeFastIntSet(2))
	test(wk, "[(4) (2)]")

	// Test Combine method.
	// Combine weak keys with themselves.
	wk = makeTableWeakKeys(md, a).Combine(makeTableWeakKeys(md, a))
	test(wk, "[(1,2) (4) (2,3)]")

	var wk2 props.WeakKeys

	// Combine set with empty set.
	wk = wk.Combine(wk2)
	test(wk, "[(1,2) (4) (2,3)]")

	// Combine empty set with another set.
	wk = wk2.Combine(wk)
	test(wk, "[(1,2) (4) (2,3)]")

	// Combine new key not in the existing set.
	wk2.Add(util.MakeFastIntSet(5, 1))
	wk = wk.Combine(wk2)
	test(wk, "[(1,2) (4) (2,3) (1,5)]")

	// Combine weak keys that overlap with existing keys.
	wk2.Add(util.MakeFastIntSet(2))
	wk2.Add(util.MakeFastIntSet(6))

	wk = wk.Combine(wk2)
	test(wk, "[(4) (1,5) (2) (6)]")
}

func makeTableWeakKeys(md *opt.Metadata, tabID opt.TableID) props.WeakKeys {
	tab := md.Table(tabID)
	weakKeys := make(props.WeakKeys, 0, tab.IndexCount())
	for idx := 0; idx < tab.IndexCount(); idx++ {
		var cs opt.ColSet
		index := tab.Index(idx)
		for col := 0; col < index.UniqueColumnCount(); col++ {
			cs.Add(int(md.TableColumn(tabID, index.Column(col).Ordinal)))
		}
		weakKeys.Add(cs)
	}
	return weakKeys
}
