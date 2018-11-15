// Copyright 2015 The Cockroach Authors.
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

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func testTableDesc() *sqlbase.ImmutableTableDescriptor {
	return sqlbase.NewImmutableTableDescriptor(sqlbase.TableDescriptor{
		Name:     "test",
		ID:       1001,
		ParentID: 1000,
		Columns: []sqlbase.ColumnDescriptor{
			{Name: "a", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}},
			{Name: "b", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}},
			{Name: "c", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}},
			{Name: "d", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}},
			{Name: "e", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}},
			{Name: "f", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}},
			{Name: "g", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}},
			{Name: "h", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_FLOAT}},
			{Name: "i", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING}},
			{Name: "j", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}},
			{Name: "k", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}},
			{Name: "l", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DECIMAL}},
			{Name: "m", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DECIMAL}},
			{Name: "n", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DATE}},
			{Name: "o", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_TIMESTAMP}},
			{Name: "p", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_TIMESTAMPTZ}},
			{Name: "q", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}, Nullable: true},
		},
		PrimaryIndex: sqlbase.IndexDescriptor{
			Name: "primary", Unique: true, ColumnNames: []string{"a"},
			ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
		},
		Privileges:    sqlbase.NewDefaultPrivilegeDescriptor(),
		FormatVersion: sqlbase.FamilyFormatVersion,
	})
}

func makeSelectNode(t *testing.T, p *planner) *renderNode {
	desc := testTableDesc()
	sel := testInitDummySelectNode(t, p, desc)
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}
	numColumns := len(sel.sourceInfo[0].SourceColumns)
	sel.ivarHelper = tree.MakeIndexedVarHelper(sel, numColumns)
	p.extendedEvalCtx.IVarContainer = sel
	sel.run.curSourceRow = make(tree.Datums, numColumns)
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

func TestSplitAndExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		expr     string
		expected string
	}{
		{`f`, `f`},
		{`f AND g`, `f, g`},
		{`f OR g`, `f OR g`},
		{`(f AND g) AND (c AND (d AND e))`, `f, g, c, d, e`},
	}
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(d.expr+"~"+d.expected, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			expr := parseAndNormalizeExpr(t, p, d.expr, sel)
			exprs := splitAndExpr(p.EvalContext(), expr, nil /* exprs */)
			if s := exprs.String(); d.expected != s {
				t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
			}
		})
	}
}
