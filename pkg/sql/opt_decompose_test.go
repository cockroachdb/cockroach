// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func testTableDesc(
	t *testing.T, intermediate func(desc *MutableTableDescriptor),
) *sqlbase.ImmutableTableDescriptor {
	mut := sqlbase.NewMutableCreatedTableDescriptor(sqlbase.TableDescriptor{
		Name:     "test",
		ID:       1001,
		ParentID: 1000,
		Columns: []sqlbase.ColumnDescriptor{
			{Name: "a", Type: *types.Int},
			{Name: "b", Type: *types.Int},
			{Name: "c", Type: *types.Bool},
			{Name: "d", Type: *types.Bool},
			{Name: "e", Type: *types.Bool},
			{Name: "f", Type: *types.Bool},
			{Name: "g", Type: *types.Bool},
			{Name: "h", Type: *types.Float},
			{Name: "i", Type: *types.String},
			{Name: "j", Type: *types.Int},
			{Name: "k", Type: *types.Bytes},
			{Name: "l", Type: *types.Decimal},
			{Name: "m", Type: *types.Decimal},
			{Name: "n", Type: *types.Date},
			{Name: "o", Type: *types.Timestamp},
			{Name: "p", Type: *types.TimestampTZ},
			{Name: "q", Type: *types.Int, Nullable: true},
			{Name: "r", Type: *types.Uuid},
			{Name: "s", Type: *types.INet},
		},
		PrimaryIndex: sqlbase.IndexDescriptor{
			Name: "primary", Unique: true, ColumnNames: []string{"a"},
			ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
		},
		Privileges:    sqlbase.NewDefaultPrivilegeDescriptor(),
		FormatVersion: sqlbase.FamilyFormatVersion,
	})
	intermediate(mut)
	if err := mut.AllocateIDs(); err != nil {
		t.Fatal(err)
	}
	return sqlbase.NewImmutableTableDescriptor(mut.TableDescriptor)
}

func makeSelectNode(t *testing.T, p *planner) *renderNode {
	desc := testTableDesc(t, func(*MutableTableDescriptor) {})
	sel := testInitDummySelectNode(t, p, desc)
	numColumns := len(sel.sourceInfo[0].SourceColumns)
	sel.ivarHelper = tree.MakeIndexedVarHelper(sel, numColumns)
	p.extendedEvalCtx.IVarContainer = sel
	return sel
}

func parseAndNormalizeExpr(t *testing.T, p *planner, sql string, sel *renderNode) tree.TypedExpr {
	expr, err := parser.ParseExpr(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}

	// Perform name resolution because {decompose,simplify}Expr want
	// expressions containing IndexedVars.
	if expr, _, _, err = p.resolveNamesForRender(expr, sel); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	p.semaCtx.IVarContainer = p.extendedEvalCtx.IVarContainer
	typedExpr, err := tree.TypeCheck(expr, &p.semaCtx, types.Any)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if typedExpr, err = p.extendedEvalCtx.NormalizeExpr(typedExpr); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return typedExpr
}
