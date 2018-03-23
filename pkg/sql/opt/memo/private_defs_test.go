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

func TestScanOpDef(t *testing.T) {
	cat := createDefsCatalog(t)
	md := opt.NewMetadata()
	a := md.AddTable(cat.Table("a"))
	altIndex1 := 1
	altIndex2 := 2
	k := int(md.TableColumn(a, 0))
	i := int(md.TableColumn(a, 1))
	s := int(md.TableColumn(a, 2))

	testHasCols := func(def *memo.ScanOpDef, altIndex int, expected bool) {
		t.Helper()
		if def.AltIndexHasCols(md, altIndex) != expected {
			t.Errorf("expected %v, got %v", expected, !expected)
		}
	}

	def := &memo.ScanOpDef{Table: a, Index: opt.PrimaryIndex, Cols: util.MakeFastIntSet(k, i)}
	testHasCols(def, altIndex1, true)
	testHasCols(def, altIndex2, false)

	def = &memo.ScanOpDef{Table: a, Index: opt.PrimaryIndex, Cols: util.MakeFastIntSet(k)}
	testHasCols(def, altIndex1, true)
	testHasCols(def, altIndex2, true)

	def = &memo.ScanOpDef{Table: a, Index: opt.PrimaryIndex, Cols: util.MakeFastIntSet(s)}
	testHasCols(def, altIndex1, false)
	testHasCols(def, altIndex2, true)

	def = &memo.ScanOpDef{Table: a, Index: opt.PrimaryIndex, Cols: util.MakeFastIntSet(k, i, s)}
	testHasCols(def, altIndex1, false)
	testHasCols(def, altIndex2, false)
}

func createDefsCatalog(t *testing.T) *testutils.TestCatalog {
	cat := testutils.NewTestCatalog()
	testutils.ExecuteTestDDL(
		t,
		"CREATE TABLE a (k INT PRIMARY KEY, i INT, s STRING, INDEX (i, k), INDEX(s DESC))",
		cat,
	)
	return cat
}
