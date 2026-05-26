// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func TestGetJoinMultiplicity(t *testing.T) {
	ob := makeOpBuilder(t)

	const createTableStatements = `
	CREATE TABLE xy (
		x INT PRIMARY KEY,
		y INT
	);
	
	CREATE TABLE uv (
		u INT PRIMARY KEY,
		v INT
	);
	
	CREATE TABLE fk_tab (
		r1 INT NOT NULL REFERENCES xy(x),
		r2 INT REFERENCES xy(x),
		r3 INT NOT NULL REFERENCES xy(x)
	);
	
	CREATE TABLE abc (
		a INT,
		b INT,
		c INT,
		PRIMARY KEY (a, b, c)
	);
	
	CREATE TABLE not_null_multi_col_fk_tab (
		r1 INT NOT NULL,
		r2 INT NOT NULL,
		r3 INT NOT NULL,
		FOREIGN KEY (r1, r2, r3) REFERENCES abc(a, b, c)
	);
	
	CREATE TABLE one_null_multi_col_fk_tab (
		r1 INT NOT NULL,
		r2 INT,
		r3 INT NOT NULL,
		FOREIGN KEY (r1, r2, r3) REFERENCES abc(a, b, c)
	);
	`
	ob.createTables(createTableStatements)

	xyScan, xyCols := ob.xyScan()
	xyScan2, xyCols2 := ob.xyScan()
	xyScanFiltered, xyColsFiltered := ob.makeFilteredScan("xy")
	uvScan, uvCols := ob.uvScan()
	fkScan, fkCols := ob.fkScan()
	abcScan, abcCols := ob.abcScan()
	notNullMultiColFKScan, notNullMultiColFKCols := ob.notNullMultiColFKScan()
	oneNullMultiColFKScan, oneNullMultiColFKCols := ob.oneNullMultiColFKScan()
	xyScanSkipLocked, xyColsSkipLocked := ob.xyScanSkipLocked()
	fkScanSkipLocked, fkColsSkipLocked := ob.fkScanSkipLocked()

	testCases := []struct {
		joinOp   opt.Operator
		left     RelExpr
		right    RelExpr
		on       FiltersExpr
		expected string
	}{
		{ // 0
			// SELECT * FROM fk_tab INNER JOIN xy ON True;
			joinOp:   opt.InnerJoinOp,
			left:     fkScan,
			right:    xyScan,
			on:       TrueFilter,
			expected: "left-rows(one-or-more), right-rows(zero-or-more)",
		},
		{ // 1
			// SELECT * FROM xy LEFT JOIN fk_tab ON True;
			joinOp:   opt.LeftJoinOp,
			left:     xyScan,
			right:    fkScan,
			on:       TrueFilter,
			expected: "left-rows(one-or-more), right-rows(one-or-more)",
		},
		{ // 2
			// SELECT * FROM xy LEFT JOIN uv ON x = u;
			joinOp:   opt.LeftJoinOp,
			left:     xyScan,
			right:    uvScan,
			on:       ob.makeFilters(ob.makeEquality(xyCols[0], uvCols[0])),
			expected: "left-rows(exactly-one), right-rows(zero-or-one)",
		},
		{ // 3
			// SELECT * FROM uv FULL JOIN xy ON True;
			joinOp:   opt.FullJoinOp,
			left:     uvScan,
			right:    xyScan,
			on:       TrueFilter,
			expected: "",
		},
		{ // 4
			// SELECT * FROM fk_tab INNER JOIN xy ON r1 = x;
			joinOp:   opt.InnerJoinOp,
			left:     fkScan,
			right:    xyScan,
			on:       ob.makeFilters(ob.makeEquality(fkCols[0], xyCols[0])),
			expected: "left-rows(exactly-one), right-rows(zero-or-more)",
		},
		{ // 5
			// SELECT * FROM fk_tab INNER JOIN xy ON r2 = x;
			joinOp:   opt.InnerJoinOp,
			left:     fkScan,
			right:    xyScan,
			on:       ob.makeFilters(ob.makeEquality(fkCols[1], xyCols[0])),
			expected: "left-rows(zero-or-one), right-rows(zero-or-more)",
		},
		{ // 6
			// SELECT * FROM fk_tab INNER JOIN xy ON r1 = x AND r3 = x;
			joinOp: opt.InnerJoinOp,
			left:   fkScan,
			right:  xyScan,
			on: ob.makeFilters(
				ob.makeEquality(fkCols[0], xyCols[0]),
				ob.makeEquality(fkCols[2], xyCols[0]),
			),
			expected: "left-rows(zero-or-one), right-rows(zero-or-more)",
		},
		{ // 7
			// SELECT * FROM xy INNER JOIN xy AS xy2 ON True;
			joinOp:   opt.InnerJoinOp,
			left:     xyScan,
			right:    xyScan2,
			on:       TrueFilter,
			expected: "",
		},
		{ // 8
			// SELECT * FROM xy INNER JOIN xy AS xy2 ON xy.x = xy2.x;
			joinOp:   opt.InnerJoinOp,
			left:     xyScan,
			right:    xyScan2,
			on:       ob.makeFilters(ob.makeEquality(xyCols[0], xyCols2[0])),
			expected: "left-rows(exactly-one), right-rows(exactly-one)",
		},
		{ // 9
			// SELECT * FROM xy INNER JOIN uv ON x = u;
			joinOp:   opt.InnerJoinOp,
			left:     xyScan,
			right:    uvScan,
			on:       ob.makeFilters(ob.makeEquality(xyCols[0], uvCols[0])),
			expected: "left-rows(zero-or-one), right-rows(zero-or-one)",
		},
		{ // 10
			// SELECT *
			// FROM not_null_multi_col_fk_tab
			// INNER JOIN abc
			// ON (r1, r2, r3) = (a, b, c);
			joinOp: opt.InnerJoinOp,
			left:   notNullMultiColFKScan,
			right:  abcScan,
			on: ob.makeFilters(
				ob.makeEquality(notNullMultiColFKCols[0], abcCols[0]),
				ob.makeEquality(notNullMultiColFKCols[1], abcCols[1]),
				ob.makeEquality(notNullMultiColFKCols[2], abcCols[2]),
			),
			expected: "left-rows(exactly-one), right-rows(zero-or-more)",
		},
		{ // 11
			// SELECT *
			// FROM one_null_multi_col_fk_tab
			// INNER JOIN abc
			// ON (r1, r2, r3) = (a, b, c);
			joinOp: opt.InnerJoinOp,
			left:   oneNullMultiColFKScan,
			right:  abcScan,
			on: ob.makeFilters(
				ob.makeEquality(oneNullMultiColFKCols[0], abcCols[0]),
				ob.makeEquality(oneNullMultiColFKCols[1], abcCols[1]),
				ob.makeEquality(oneNullMultiColFKCols[2], abcCols[2]),
			),
			expected: "left-rows(zero-or-one), right-rows(zero-or-more)",
		},
		{ // 12
			// SELECT * FROM not_null_multi_col_fk_tab INNER JOIN abc ON r1 = a;
			joinOp:   opt.InnerJoinOp,
			left:     notNullMultiColFKScan,
			right:    abcScan,
			on:       ob.makeFilters(ob.makeEquality(notNullMultiColFKCols[0], abcCols[0])),
			expected: "left-rows(one-or-more), right-rows(zero-or-more)",
		},
		{ // 13
			// SELECT * FROM one_null_multi_col_fk_tab INNER JOIN abc ON r1 = a;
			joinOp:   opt.InnerJoinOp,
			left:     oneNullMultiColFKScan,
			right:    abcScan,
			on:       ob.makeFilters(ob.makeEquality(oneNullMultiColFKCols[0], abcCols[0])),
			expected: "",
		},
		{ // 14
			// SELECT * FROM not_null_multi_col_fk_tab INNER JOIN abc ON r2 = a;
			joinOp:   opt.InnerJoinOp,
			left:     notNullMultiColFKScan,
			right:    abcScan,
			on:       ob.makeFilters(ob.makeEquality(notNullMultiColFKCols[1], abcCols[0])),
			expected: "",
		},
		{ // 15
			// SELECT *
			// FROM fk_tab
			// INNER JOIN (SELECT * FROM xy INNER JOIN xy AS xy2 ON xy.x = xy2.x)
			// ON True;
			joinOp: opt.InnerJoinOp,
			left:   fkScan,
			right: ob.makeInnerJoin(
				xyScan,
				xyScan2,
				ob.makeFilters(ob.makeEquality(xyCols[0], xyCols2[0])),
			),
			on:       TrueFilter,
			expected: "left-rows(one-or-more), right-rows(zero-or-more)",
		},
		{ // 16
			// SELECT *
			// FROM fk_tab
			// INNER JOIN (SELECT * FROM xy LEFT JOIN uv ON x = u)
			// ON r1 = x;
			joinOp: opt.InnerJoinOp,
			left:   fkScan,
			right: ob.makeLeftJoin(
				xyScan,
				uvScan,
				ob.makeFilters(ob.makeEquality(xyCols[0], uvCols[0])),
			),
			on:       ob.makeFilters(ob.makeEquality(fkCols[0], xyCols[0])),
			expected: "left-rows(exactly-one), right-rows(zero-or-more)",
		},
		{ // 17
			// SELECT *
			// FROM fk_tab
			// INNER JOIN (SELECT * FROM xy INNER JOIN uv ON x = u)
			// ON r1 = x;
			joinOp: opt.InnerJoinOp,
			left:   fkScan,
			right: ob.makeInnerJoin(
				xyScan,
				uvScan,
				ob.makeFilters(ob.makeEquality(xyCols[0], uvCols[0])),
			),
			on:       ob.makeFilters(ob.makeEquality(fkCols[0], xyCols[0])),
			expected: "left-rows(zero-or-one), right-rows(zero-or-more)",
		},
		{ // 18
			// SELECT * FROM fk_tab WHERE EXISTS (SELECT * FROM xy WHERE x = r1);
			joinOp:   opt.SemiJoinOp,
			left:     fkScan,
			right:    xyScan,
			on:       ob.makeFilters(ob.makeEquality(fkCols[0], xyCols[0])),
			expected: "left-rows(exactly-one)",
		},
		{ // 19
			// SELECT *
			// FROM fk_tab
			// INNER JOIN (SELECT * FROM xy AS xy1 INNER JOIN xy xy2 ON xy1.x = xy2.x)
			// ON r1 = xy1.x;
			joinOp: opt.InnerJoinOp,
			left:   fkScan,
			right: ob.makeInnerJoin(
				xyScan,
				xyScan2,
				ob.makeFilters(ob.makeEquality(xyCols[0], xyCols2[0])),
			),
			on:       ob.makeFilters(ob.makeEquality(fkCols[0], xyCols[0])),
			expected: "left-rows(exactly-one), right-rows(zero-or-more)",
		},
		{ // 20
			// SELECT *
			// FROM fk_tab
			// INNER JOIN (SELECT * FROM xy AS xy1 INNER JOIN xy xy2 ON xy1.x = xy2.x)
			// ON r1 = xy2.x;
			joinOp: opt.InnerJoinOp,
			left:   fkScan,
			right: ob.makeInnerJoin(
				xyScan,
				xyScan2,
				ob.makeFilters(ob.makeEquality(xyCols[0], xyCols2[0])),
			),
			on:       ob.makeFilters(ob.makeEquality(fkCols[0], xyCols2[0])),
			expected: "left-rows(exactly-one), right-rows(zero-or-more)",
		},
		{ // 21
			// SELECT * FROM fk_tab INNER JOIN xy ON r1 = x WHERE r1 = 5 AND x = 5;
			joinOp:   opt.InnerJoinOp,
			left:     ob.makeSelect(fkScan, ob.makeFilters(ob.makeConstEquality(fkCols[0], 5))),
			right:    ob.makeSelect(xyScan, ob.makeFilters(ob.makeConstEquality(xyCols[0], 5))),
			on:       ob.makeFilters(ob.makeEquality(fkCols[0], xyCols[0])),
			expected: "left-rows(exactly-one), right-rows(zero-or-more)",
		},
		{ // 22
			// SELECT * FROM xy INNER JOIN xy AS xy2 ON xy.x = xy2.x WHERE xy.x = 5 AND xy2.x = 5;
			joinOp:   opt.InnerJoinOp,
			left:     ob.makeSelect(xyScan, ob.makeFilters(ob.makeConstEquality(xyCols[0], 5))),
			right:    ob.makeSelect(xyScan2, ob.makeFilters(ob.makeConstEquality(xyCols2[0], 5))),
			on:       ob.makeFilters(ob.makeEquality(xyCols[0], xyCols2[0])),
			expected: "left-rows(exactly-one), right-rows(exactly-one)",
		},
		{ // 23
			// SELECT * FROM fk_tab INNER JOIN xy ON r1 = x WHERE r1 = 5 AND x >= 5;
			joinOp:   opt.InnerJoinOp,
			left:     ob.makeSelect(fkScan, ob.makeFilters(ob.makeConstEquality(fkCols[0], 5))),
			right:    ob.makeSelect(xyScan, ob.makeFilters(ob.makeConstInequality(xyCols[0], 5))),
			on:       ob.makeFilters(ob.makeEquality(fkCols[0], xyCols[0])),
			expected: "left-rows(zero-or-one), right-rows(zero-or-more)",
		},
		{ // 24
			// SELECT * FROM xy INNER JOIN xy AS xy2 ON xy.x = xy2.x WHERE xy.x = 5 AND xy2.x >= 5;
			joinOp:   opt.InnerJoinOp,
			left:     ob.makeSelect(xyScan, ob.makeFilters(ob.makeConstEquality(xyCols[0], 5))),
			right:    ob.makeSelect(xyScan2, ob.makeFilters(ob.makeConstInequality(xyCols2[0], 5))),
			on:       ob.makeFilters(ob.makeEquality(xyCols[0], xyCols2[0])),
			expected: "left-rows(zero-or-one), right-rows(zero-or-one)",
		},
		{ // 25
			// SELECT * FROM fk_tab INNER JOIN xy ON r1 = x WHERE r1 = 5 AND y = 5;
			joinOp:   opt.InnerJoinOp,
			left:     ob.makeSelect(fkScan, ob.makeFilters(ob.makeConstEquality(fkCols[0], 5))),
			right:    ob.makeSelect(xyScan, ob.makeFilters(ob.makeConstEquality(xyCols[1], 5))),
			on:       ob.makeFilters(ob.makeEquality(fkCols[0], xyCols[0])),
			expected: "left-rows(zero-or-one), right-rows(zero-or-more)",
		},
		{ // 26
			// SELECT * FROM xy INNER JOIN xy AS xy2 ON xy.x = xy2.x WHERE xy.x = 5 AND xy2.y = 5;
			joinOp:   opt.InnerJoinOp,
			left:     ob.makeSelect(xyScan, ob.makeFilters(ob.makeConstEquality(xyCols[0], 5))),
			right:    ob.makeSelect(xyScan2, ob.makeFilters(ob.makeConstEquality(xyCols2[1], 5))),
			on:       ob.makeFilters(ob.makeEquality(xyCols[0], xyCols2[0])),
			expected: "left-rows(zero-or-one), right-rows(zero-or-one)",
		},
		{ // 27
			// SELECT * FROM fk_tab INNER JOIN (
			// 	 SELECT * FROM xy WHERE x = 5 LIMIT 10
			// ) AS xy2 ON r1 = x
			// WHERE xy.x = 5;
			joinOp: opt.InnerJoinOp,
			left:   ob.makeSelect(fkScan, ob.makeFilters(ob.makeConstEquality(fkCols[0], 5))),
			right: ob.makeSelect(xyScanFiltered, ob.makeFilters(
				ob.makeConstEquality(xyColsFiltered[0], 5),
			)),
			on:       ob.makeFilters(ob.makeEquality(fkCols[0], xyColsFiltered[0])),
			expected: "left-rows(zero-or-one), right-rows(zero-or-more)",
		},
		{ // 28
			// SELECT * FROM xy INNER JOIN (
			// 	 SELECT * FROM xy WHERE x = 5 LIMIT 10
			// ) AS xy2 ON xy.x = xy2.x
			// WHERE xy.x = 5;
			joinOp: opt.InnerJoinOp,
			left:   ob.makeSelect(xyScan, ob.makeFilters(ob.makeConstEquality(xyCols[0], 5))),
			right: ob.makeSelect(xyScanFiltered, ob.makeFilters(
				ob.makeConstEquality(xyColsFiltered[0], 5),
			)),
			on:       ob.makeFilters(ob.makeEquality(xyCols[0], xyColsFiltered[0])),
			expected: "left-rows(zero-or-one), right-rows(exactly-one)",
		},
		{ // 29
			// SELECT * FROM fk_tab INNER JOIN (
			// 	 SELECT * FROM xy WHERE x = 5 AND y = 2
			// ) AS xy2 ON r1 = x
			// WHERE xy.x = 5;
			joinOp: opt.InnerJoinOp,
			left:   ob.makeSelect(fkScan, ob.makeFilters(ob.makeConstEquality(fkCols[0], 5))),
			right: ob.makeSelect(xyScan, ob.makeFilters(
				ob.makeConstEquality(xyCols[0], 5),
				ob.makeConstEquality(xyCols[1], 2),
			)),
			on:       ob.makeFilters(ob.makeEquality(fkCols[0], xyCols[0])),
			expected: "left-rows(zero-or-one), right-rows(zero-or-more)",
		},
		{ // 30
			// SELECT * FROM xy INNER JOIN (
			// 	 SELECT * FROM xy WHERE x = 5 AND y = 2
			// ) AS xy2 ON xy.x = xy2.x
			// WHERE xy.x = 5;
			joinOp: opt.InnerJoinOp,
			left:   ob.makeSelect(xyScan, ob.makeFilters(ob.makeConstEquality(xyCols[0], 5))),
			right: ob.makeSelect(xyScan2, ob.makeFilters(
				ob.makeConstEquality(xyCols2[0], 5),
				ob.makeConstEquality(xyCols2[1], 2),
			)),
			on:       ob.makeFilters(ob.makeEquality(xyCols[0], xyCols2[0])),
			expected: "left-rows(zero-or-one), right-rows(exactly-one)",
		},
		{ // 31
			// SELECT * FROM xy INNER JOIN xy AS xy2 ON xy.x = xy2.x FOR UPDATE OF xy SKIP LOCKED;
			joinOp:   opt.InnerJoinOp,
			left:     xyScanSkipLocked,
			right:    xyScan2,
			on:       ob.makeFilters(ob.makeEquality(xyColsSkipLocked[0], xyCols2[0])),
			expected: "left-rows(exactly-one), right-rows(zero-or-one)",
		},
		{ // 32
			// SELECT * FROM xy AS xy2 INNER JOIN xy ON xy.x = xy2.x FOR UPDATE OF xy2 SKIP LOCKED;
			joinOp:   opt.InnerJoinOp,
			left:     xyScan2,
			right:    xyScanSkipLocked,
			on:       ob.makeFilters(ob.makeEquality(xyColsSkipLocked[0], xyCols2[0])),
			expected: "left-rows(zero-or-one), right-rows(exactly-one)",
		},
		{ // 33
			// SELECT * FROM xy INNER JOIN fk_tab ON r1 = x FOR UPDATE SKIP LOCKED;
			joinOp:   opt.InnerJoinOp,
			left:     xyScanSkipLocked,
			right:    fkScanSkipLocked,
			on:       ob.makeFilters(ob.makeEquality(fkColsSkipLocked[0], xyColsSkipLocked[0])),
			expected: "left-rows(zero-or-more), right-rows(zero-or-one)",
		},
		{ // 34
			// SELECT * FROM fk_tab INNER JOIN xy ON r1 = x FOR UPDATE OF fk_tab SKIP LOCKED;
			joinOp:   opt.InnerJoinOp,
			left:     fkScanSkipLocked,
			right:    xyScan,
			on:       ob.makeFilters(ob.makeEquality(fkColsSkipLocked[0], xyCols[0])),
			expected: "left-rows(exactly-one), right-rows(zero-or-more)",
		},
		{ // 35
			// SELECT * FROM fk_tab INNER JOIN xy ON r1 = x FOR UPDATE OF xy SKIP LOCKED;
			joinOp:   opt.InnerJoinOp,
			left:     fkScan,
			right:    xyScanSkipLocked,
			on:       ob.makeFilters(ob.makeEquality(fkCols[0], xyColsSkipLocked[0])),
			expected: "left-rows(zero-or-one), right-rows(zero-or-more)",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test case %d", i), func(t *testing.T) {
			join := ob.makeJoin(tc.joinOp, tc.left, tc.right, tc.on)
			joinWithMult, _ := join.(joinWithMultiplicity)
			multiplicity := joinWithMult.getMultiplicity()
			if multiplicity.Format(tc.joinOp) != tc.expected {
				t.Errorf("\nexpected: %s\nactual:   %s", tc.expected, multiplicity.Format(tc.joinOp))
			}
		})
	}
}

type testOpBuilder struct {
	t       *testing.T
	evalCtx *eval.Context
	mem     *Memo
	cat     *testcat.Catalog
}

func makeOpBuilder(t *testing.T) testOpBuilder {
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var mem Memo
	mem.Init(context.Background(), &evalCtx)
	ob := testOpBuilder{
		t:       t,
		evalCtx: &evalCtx,
		mem:     &mem,
		cat:     testcat.New(),
	}
	return ob
}

func (ob *testOpBuilder) createTable(createTableStmt string) {
	if _, err := ob.cat.ExecuteDDL(
		createTableStmt,
	); err != nil {
		ob.t.Fatal(err)
	}
}

func (ob *testOpBuilder) createTables(stmts string) {
	last := 0
	for {
		pos, ok := parser.SplitFirstStatement(stmts[last:])
		if !ok {
			break
		}
		ob.createTable(stmts[last : last+pos])
		last += pos
	}
}

func (ob *testOpBuilder) makeScan(tableName tree.Name) (scan RelExpr, vars []*VariableExpr) {
	return ob.makeScanImpl(tableName, false /* filtered */)
}

func (ob *testOpBuilder) makeFilteredScan(
	tableName tree.Name,
) (scan RelExpr, vars []*VariableExpr) {
	return ob.makeScanImpl(tableName, true /* filtered */)
}

func (ob *testOpBuilder) makeScanImpl(
	tableName tree.Name, filtered bool,
) (scan RelExpr, vars []*VariableExpr) {
	tn := tree.NewUnqualifiedTableName(tableName)
	tab := ob.cat.Table(tn)
	tabID := ob.mem.Metadata().AddTable(tab, tn)
	var cols opt.ColSet
	for i := 0; i < tab.ColumnCount(); i++ {
		col := tabID.ColumnID(i)
		cols.Add(col)
		newVar := ob.mem.MemoizeVariable(col)
		vars = append(vars, newVar)
	}
	sp := &ScanPrivate{Table: tabID, Cols: cols}
	if filtered {
		sp.HardLimit = 10
	}
	return ob.mem.MemoizeScan(sp), vars
}

func (ob *testOpBuilder) xyScan() (scan RelExpr, vars []*VariableExpr) {
	return ob.makeScan("xy")
}

func (ob *testOpBuilder) uvScan() (scan RelExpr, vars []*VariableExpr) {
	return ob.makeScan("uv")
}

func (ob *testOpBuilder) fkScan() (scan RelExpr, vars []*VariableExpr) {
	return ob.makeScan("fk_tab")
}

func (ob *testOpBuilder) abcScan() (scan RelExpr, vars []*VariableExpr) {
	return ob.makeScan("abc")
}

func (ob *testOpBuilder) notNullMultiColFKScan() (scan RelExpr, vars []*VariableExpr) {
	return ob.makeScan("not_null_multi_col_fk_tab")
}

func (ob *testOpBuilder) oneNullMultiColFKScan() (scan RelExpr, vars []*VariableExpr) {
	return ob.makeScan("one_null_multi_col_fk_tab")
}

func (ob *testOpBuilder) xyScanSkipLocked() (scan RelExpr, vars []*VariableExpr) {
	scan, vars = ob.xyScan()
	scan.(*ScanExpr).Locking = opt.Locking{
		Strength: tree.ForUpdate, WaitPolicy: tree.LockWaitSkipLocked,
	}
	return scan, vars
}

func (ob *testOpBuilder) fkScanSkipLocked() (scan RelExpr, vars []*VariableExpr) {
	scan, vars = ob.fkScan()
	scan.(*ScanExpr).Locking = opt.Locking{
		Strength: tree.ForUpdate, WaitPolicy: tree.LockWaitSkipLocked,
	}
	return scan, vars
}

func (ob *testOpBuilder) makeSelect(input RelExpr, filters FiltersExpr) RelExpr {
	return ob.mem.MemoizeSelect(input, filters)
}

func (ob *testOpBuilder) makeInnerJoin(left, right RelExpr, on FiltersExpr) RelExpr {
	return ob.mem.MemoizeInnerJoin(left, right, on, EmptyJoinPrivate)
}

func (ob *testOpBuilder) makeLeftJoin(left, right RelExpr, on FiltersExpr) RelExpr {
	return ob.mem.MemoizeLeftJoin(left, right, on, EmptyJoinPrivate)
}

func (ob *testOpBuilder) makeFullJoin(left, right RelExpr, on FiltersExpr) RelExpr {
	return ob.mem.MemoizeFullJoin(left, right, on, EmptyJoinPrivate)
}

func (ob *testOpBuilder) makeSemiJoin(left, right RelExpr, on FiltersExpr) RelExpr {
	return ob.mem.MemoizeSemiJoin(left, right, on, EmptyJoinPrivate)
}

func (ob *testOpBuilder) makeJoin(
	joinOp opt.Operator, left, right RelExpr, on FiltersExpr,
) RelExpr {
	switch joinOp {
	case opt.InnerJoinOp:
		return ob.makeInnerJoin(left, right, on)

	case opt.LeftJoinOp:
		return ob.makeLeftJoin(left, right, on)

	case opt.FullJoinOp:
		return ob.makeFullJoin(left, right, on)

	case opt.SemiJoinOp:
		return ob.makeSemiJoin(left, right, on)

	default:
		panic(errors.AssertionFailedf("invalid operator type: %v", joinOp.String()))
	}
}

func (ob *testOpBuilder) makeEquality(left, right *VariableExpr) opt.ScalarExpr {
	return ob.mem.MemoizeEq(left, right)
}

func (ob *testOpBuilder) makeConstEquality(v *VariableExpr, c int) opt.ScalarExpr {
	return ob.mem.MemoizeEq(v, ob.mem.MemoizeConst(tree.NewDInt(tree.DInt(c)), types.Int))
}

func (ob *testOpBuilder) makeConstInequality(v *VariableExpr, c int) opt.ScalarExpr {
	return ob.mem.MemoizeGe(v, ob.mem.MemoizeConst(tree.NewDInt(tree.DInt(c)), types.Int))
}

func (ob *testOpBuilder) makeFilters(conditions ...opt.ScalarExpr) (filters FiltersExpr) {
	for i := range conditions {
		filtersItem := FiltersItem{Condition: conditions[i]}
		filtersItem.PopulateProps(ob.mem)
		filters = append(filters, filtersItem)
	}
	return filters
}
