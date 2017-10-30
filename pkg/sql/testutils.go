// Copyright 2017 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

func init() {
	distsqlrun.MakeTestSpans = makeTestSpansWithConstraints
}

func makeTestSpansWithConstraints(
	desc *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor, sql string,
) (roachpb.Spans, error) {
	evalCtx := parser.NewTestingEvalContext()
	defer evalCtx.Stop(context.Background())
	sel, err := makeSelectNodeWithDesc(desc)
	if err != nil {
		return nil, err
	}
	constraints, _, err := makeConstraints(evalCtx, sql, desc, index, sel)
	if err != nil {
		return nil, err
	}
	return makeSpans(evalCtx, constraints, desc, index)
}

func makeTestPlanner() *planner {
	return makeInternalPlanner("test", nil /* txn */, security.RootUser, &MemoryMetrics{})
}

func testInitDummySelectNode(desc *sqlbase.TableDescriptor) *renderNode {
	p := makeTestPlanner()
	scan := &scanNode{p: p}
	scan.desc = desc
	// Note: scan.initDescDefaults only returns an error if its 2nd argument is not nil.
	_ = scan.initDescDefaults(publicColumns, nil)

	sel := &renderNode{planner: p}
	sel.source.plan = scan
	testName := parser.TableName{TableName: parser.Name(desc.Name), DatabaseName: parser.Name("test")}
	cols := planColumns(scan)
	sel.source.info = newSourceInfoForSingleTable(testName, cols)
	sel.sourceInfo = multiSourceInfo{sel.source.info}
	sel.ivarHelper = parser.MakeIndexedVarHelper(sel, len(cols))

	return sel
}

func testTableDesc() *sqlbase.TableDescriptor {
	return &sqlbase.TableDescriptor{
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
		},
		PrimaryIndex: sqlbase.IndexDescriptor{
			Name: "primary", Unique: true, ColumnNames: []string{"a"},
			ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
		},
		Privileges:    sqlbase.NewDefaultPrivilegeDescriptor(),
		FormatVersion: sqlbase.FamilyFormatVersion,
	}
}

func makeSelectNode() (*renderNode, error) {
	return makeSelectNodeWithDesc(testTableDesc())
}

func makeSelectNodeWithDesc(desc *sqlbase.TableDescriptor) (*renderNode, error) {
	sel := testInitDummySelectNode(desc)
	if err := desc.AllocateIDs(); err != nil {
		return nil, err
	}
	numColumns := len(sel.sourceInfo[0].sourceColumns)
	sel.ivarHelper = parser.MakeIndexedVarHelper(sel, numColumns)
	sel.curSourceRow = make(parser.Datums, numColumns)
	return sel, nil
}

func parseAndNormalizeExpr(
	evalCtx *parser.EvalContext, sql string, sel *renderNode,
) (parser.TypedExpr, error) {
	expr, err := parser.ParseExpr(sql)
	if err != nil {
		return nil, errors.Wrapf(err, sql)
	}

	// Perform name resolution because {analyze,simplify}Expr want
	// expressions containing IndexedVars.
	if expr, _, _, err = sel.resolveNames(expr); err != nil {
		return nil, errors.Wrapf(err, sql)
	}
	typedExpr, err := parser.TypeCheck(expr, nil, types.Any)
	if err != nil {
		return nil, errors.Wrapf(err, sql)
	}
	if typedExpr, err = evalCtx.NormalizeExpr(typedExpr); err != nil {
		return nil, errors.Wrapf(err, sql)
	}
	return typedExpr, nil
}

func makeConstraints(
	evalCtx *parser.EvalContext,
	sql string,
	desc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	sel *renderNode,
) (orIndexConstraints, parser.TypedExpr, error) {
	expr, err := parseAndNormalizeExpr(evalCtx, sql, sel)
	if err != nil {
		return nil, nil, err
	}
	exprs, equiv := analyzeExpr(evalCtx, expr)

	c := &indexInfo{
		desc:     desc,
		index:    index,
		covering: true,
	}
	c.analyzeExprs(evalCtx, exprs)
	if equiv && len(exprs) == 1 {
		expr = joinAndExprs(exprs[0])
	}
	return c.constraints, expr, nil
}
