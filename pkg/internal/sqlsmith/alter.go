// Copyright 2019 The Cockroach Authors.
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

	return &tree.RenameTable{
		Name:    *tableRef.TableName,
		NewName: tree.MakeUnqualifiedTableName(s.schema.name("tab")),
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
	typ := getRandType()
	col := tableRef.Columns[s.schema.rnd.Intn(len(tableRef.Columns))]

	return &tree.AlterTable{
		Table: *tableRef.TableName,
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
	t := getRandType()
	col, err := tree.NewColumnTableDef(s.schema.name("col"), t, false /* isSerial */, nil)
	if err != nil {
		return nil, false
	}
	col.Nullable.Nullability = s.schema.randNullability()
	if coin() {
		col.DefaultExpr.Expr = &tree.ParenExpr{Expr: makeScalar(s, t, nil)}
	} else if coin() {
		col.Computed.Computed = true
		col.Computed.Expr = &tree.ParenExpr{Expr: makeScalar(s, t, colRefs)}
	}
	for coin() {
		col.CheckExprs = append(col.CheckExprs, tree.ColumnTableDefCheckExpr{
			Expr: makeBoolExpr(s, colRefs),
		})
	}

	return &tree.AlterTable{
		Table: *tableRef.TableName,
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
		Table: *tableRef.TableName,
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
	for len(cols) < 1 || coin() {
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
	for coin() {
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
		Unique:  coin(),
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
