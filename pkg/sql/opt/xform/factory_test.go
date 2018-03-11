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

package xform

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// TestSimplifyFilters tests factory.TestSimplifyFilters. It's hard to fully
// test using SQL, as And operator rules simplify the expression before the
// Filters operator is created.
func TestSimplifyFilters(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	f := newFactory(&evalCtx, 0)

	cat := createFiltersCatalog(t)
	a := f.Metadata().AddTable(cat.Table("a"))
	ax := f.Metadata().TableColumn(a, 0)

	variable := f.ConstructVariable(f.InternPrivate(ax))
	constant := f.ConstructConst(f.InternPrivate(tree.NewDInt(1)))
	eq := f.ConstructEq(variable, constant)

	// Filters expression evaluates to False if any operand is False.
	conditions := []opt.GroupID{eq, f.ConstructFalse(), eq}
	result := f.simplifyFilters(f.InternList(conditions))
	if f.mem.lookupNormExpr(result).op != opt.FalseOp {
		t.Fatalf("result should have been False")
	}

	// Filters expression evaluates to False if any operand is Null.
	conditions = []opt.GroupID{f.ConstructNull(f.InternPrivate(types.Unknown)), eq, eq}
	result = f.simplifyFilters(f.InternList(conditions))
	if f.mem.lookupNormExpr(result).op != opt.FalseOp {
		t.Fatalf("result should have been False")
	}

	// Filters operator skips True operands.
	conditions = []opt.GroupID{eq, f.ConstructTrue(), eq, f.ConstructTrue()}
	result = f.simplifyFilters(f.InternList(conditions))
	if f.mem.lookupNormExpr(result).asFilters().conditions().Length != 2 {
		t.Fatalf("result should have filtered True operators")
	}

	// Filters operator flattens nested And operands.
	conditions = []opt.GroupID{eq, eq}
	and := f.ConstructAnd(f.InternList(conditions))
	conditions = []opt.GroupID{and, eq, and}
	result = f.simplifyFilters(f.InternList(conditions))
	if f.mem.lookupNormExpr(result).asFilters().conditions().Length != 5 {
		t.Fatalf("result should have flattened And operators")
	}
}

func createFiltersCatalog(t *testing.T) *testutils.TestCatalog {
	cat := testutils.NewTestCatalog()
	testutils.ExecuteTestDDL(t, "CREATE TABLE a (x INT PRIMARY KEY, y INT)", cat)
	return cat
}
