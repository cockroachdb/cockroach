// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlsmith

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var (
	alters       []statementWeight
	alterWeights []int
)

func init() {
	alters = []statementWeight{
		{1, makeRenameTable},
		{1, makeCreateTable},
		{1, makeDropTable},

		{1, makeAddColumn},
		{1, makeDropColumn},
		{1, makeRenameColumn},
		{1, makeAlterColumnType},

		{1, makeCreateIndex},
		{1, makeDropIndex},
		{1, makeRenameIndex},
	}
	alterWeights = func() []int {
		m := make([]int, len(alters))
		for i, s := range alters {
			m[i] = s.weight
		}
		return m
	}()
}

func makeAlter(s *scope) (tree.Statement, bool) {
	if s.canRecurse() {
		// Schema changes aren't visible immediately, so try to
		// sync the change from the last alter before trying the
		// next one. This is instead of running ReloadSchemas right
		// after the alter succeeds (which doesn't always pick
		// up the change). This has the added benefit of leaving
		// behind old column references for a bit, which should
		// test some additional logic.
		_ = s.schema.ReloadSchemas()
		for i := 0; i < retryCount; i++ {
			idx := s.schema.alters.Next()
			stmt, ok := alters[idx].fn(s)
			if ok {
				return stmt, ok
			}
		}
	}
	return nil, false
}

func makeCreateTable(s *scope) (tree.Statement, bool) {
	table := sqlbase.RandCreateTable(s.schema.rnd, 0)
	table.Table = tree.MakeUnqualifiedTableName(s.schema.name("tab"))
	return table, true
}

func makeDropTable(s *scope) (tree.Statement, bool) {
	_, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}

	return &tree.DropTable{
		Names:        tree.TableNames{*tableRef.TableName},
		DropBehavior: s.schema.randDropBehavior(),
	}, true
}

func makeRenameTable(s *scope) (tree.Statement, bool) {
	_, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}

	newName, err := tree.NewUnresolvedObjectName(
		1 /* numParts */, [3]string{string(s.schema.name("tab"))}, tree.NoAnnotation,
	)
	if err != nil {
		return nil, false
	}

	return &tree.RenameTable{
		Name:    tableRef.TableName.ToUnresolvedObjectName(),
		NewName: newName,
	}, true
}

func makeRenameColumn(s *scope) (tree.Statement, bool) {
	_, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	col := tableRef.Columns[s.schema.rnd.Intn(len(tableRef.Columns))]

	return &tree.RenameColumn{
		Table:   *tableRef.TableName,
		Name:    col.Name,
		NewName: s.schema.name("col"),
	}, true
}

func makeAlterColumnType(s *scope) (tree.Statement, bool) {
	_, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	typ := sqlbase.RandColumnType(s.schema.rnd)
	col := tableRef.Columns[s.schema.rnd.Intn(len(tableRef.Columns))]

	return &tree.AlterTable{
		Table: tableRef.TableName.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAlterColumnType{
				Column: col.Name,
				ToType: typ,
			},
		},
	}, true
}

func makeAddColumn(s *scope) (tree.Statement, bool) {
	_, tableRef, colRefs, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	colRefs.stripTableName()
	t := sqlbase.RandColumnType(s.schema.rnd)
	col, err := tree.NewColumnTableDef(s.schema.name("col"), t, false /* isSerial */, nil)
	if err != nil {
		return nil, false
	}
	col.Nullable.Nullability = s.schema.randNullability()
	if s.coin() {
		col.DefaultExpr.Expr = &tree.ParenExpr{Expr: makeScalar(s, t, nil)}
	} else if s.coin() {
		col.Computed.Computed = true
		col.Computed.Expr = &tree.ParenExpr{Expr: makeScalar(s, t, colRefs)}
	}
	for s.coin() {
		col.CheckExprs = append(col.CheckExprs, tree.ColumnTableDefCheckExpr{
			Expr: makeBoolExpr(s, colRefs),
		})
	}

	return &tree.AlterTable{
		Table: tableRef.TableName.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAddColumn{
				ColumnDef: col,
			},
		},
	}, true
}

func makeDropColumn(s *scope) (tree.Statement, bool) {
	_, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	col := tableRef.Columns[s.schema.rnd.Intn(len(tableRef.Columns))]

	return &tree.AlterTable{
		Table: tableRef.TableName.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableDropColumn{
				Column:       col.Name,
				DropBehavior: s.schema.randDropBehavior(),
			},
		},
	}, true
}

func makeCreateIndex(s *scope) (tree.Statement, bool) {
	_, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	var cols tree.IndexElemList
	seen := map[tree.Name]bool{}
	for len(cols) < 1 || s.coin() {
		col := tableRef.Columns[s.schema.rnd.Intn(len(tableRef.Columns))]
		if seen[col.Name] {
			continue
		}
		seen[col.Name] = true
		cols = append(cols, tree.IndexElem{
			Column:    col.Name,
			Direction: s.schema.randDirection(),
		})
	}
	var storing tree.NameList
	for s.coin() {
		col := tableRef.Columns[s.schema.rnd.Intn(len(tableRef.Columns))]
		if seen[col.Name] {
			continue
		}
		seen[col.Name] = true
		storing = append(storing, col.Name)
	}

	return &tree.CreateIndex{
		Name:    s.schema.name("idx"),
		Table:   *tableRef.TableName,
		Unique:  s.coin(),
		Columns: cols,
		Storing: storing,
	}, true
}

func makeDropIndex(s *scope) (tree.Statement, bool) {
	tin, _, ok := s.schema.getRandIndex()
	return &tree.DropIndex{
		IndexList:    tree.TableIndexNames{tin},
		DropBehavior: s.schema.randDropBehavior(),
	}, ok
}

func makeRenameIndex(s *scope) (tree.Statement, bool) {
	tin, _, ok := s.schema.getRandIndex()
	return &tree.RenameIndex{
		Index:   tin,
		NewName: tree.UnrestrictedName(s.schema.name("idx")),
	}, ok
}
