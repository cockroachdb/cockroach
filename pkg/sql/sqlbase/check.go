// Copyright 2016 The Cockroach Authors.
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

package sqlbase

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// CheckHelper validates check constraints on rows, on INSERT and UPDATE.
type CheckHelper struct {
	Exprs        []tree.TypedExpr
	cols         []ColumnDescriptor
	sourceInfo   *DataSourceInfo
	ivarHelper   *tree.IndexedVarHelper
	curSourceRow tree.Datums
}

// AnalyzeExprFunction is the function type used by the CheckHelper during
// initialization to analyze an expression. See sql/analyze_expr.go for details
// about the function.
type AnalyzeExprFunction func(
	ctx context.Context,
	raw tree.Expr,
	sources MultiSourceInfo,
	iVarHelper tree.IndexedVarHelper,
	expectedType types.T,
	requireType bool,
	typingContext string,
) (tree.TypedExpr, error)

// Init initializes the CheckHelper. This step should be done during planning.
func (c *CheckHelper) Init(
	ctx context.Context,
	analyzeExpr AnalyzeExprFunction,
	tn *tree.TableName,
	tableDesc *TableDescriptor,
) error {
	if len(tableDesc.Checks) == 0 {
		return nil
	}

	c.cols = tableDesc.Columns
	c.sourceInfo = NewSourceInfoForSingleTable(
		*tn, ResultColumnsFromColDescs(tableDesc.Columns),
	)

	c.Exprs = make([]tree.TypedExpr, len(tableDesc.Checks))
	exprStrings := make([]string, len(tableDesc.Checks))
	for i, check := range tableDesc.Checks {
		exprStrings[i] = check.Expr
	}
	exprs, err := parser.ParseExprs(exprStrings)
	if err != nil {
		return err
	}

	ivarHelper := tree.MakeIndexedVarHelper(c, len(c.cols))
	for i, raw := range exprs {
		typedExpr, err := analyzeExpr(
			ctx,
			raw,
			MakeMultiSourceInfo(c.sourceInfo),
			ivarHelper,
			types.Bool,
			false, /* requireType */
			"",    /* typingContext */
		)
		if err != nil {
			return err
		}
		c.Exprs[i] = typedExpr
	}
	c.ivarHelper = &ivarHelper
	c.curSourceRow = make(tree.Datums, len(c.cols))
	return nil
}

// LoadRow sets values in the IndexedVars used by the CHECK exprs.
// Any value not passed is set to NULL, unless `merge` is true, in which
// case it is left unchanged (allowing updating a subset of a row's values).
func (c *CheckHelper) LoadRow(colIdx map[ColumnID]int, row tree.Datums, merge bool) error {
	if len(c.Exprs) == 0 {
		return nil
	}
	// Populate IndexedVars.
	for _, ivar := range c.ivarHelper.GetIndexedVars() {
		if !ivar.Used {
			continue
		}
		ri, has := colIdx[c.cols[ivar.Idx].ID]
		if has {
			if row[ri] != tree.DNull {
				expected, provided := ivar.ResolvedType(), row[ri].ResolvedType()
				if !expected.Equivalent(provided) {
					return errors.Errorf("%s value does not match CHECK expr type %s", provided, expected)
				}
			}
			c.curSourceRow[ivar.Idx] = row[ri]
		} else if !merge {
			c.curSourceRow[ivar.Idx] = tree.DNull
		}
	}
	return nil
}

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (c *CheckHelper) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return c.curSourceRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (c *CheckHelper) IndexedVarResolvedType(idx int) types.T {
	return c.sourceInfo.SourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the parser.IndexedVarContainer interface.
func (c *CheckHelper) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return c.sourceInfo.NodeFormatter(idx)
}

// Check performs the actual checks based on the values stored in the
// CheckHelper.
func (c *CheckHelper) Check(ctx *tree.EvalContext) error {
	ctx.PushIVarContainer(c)
	defer func() { ctx.PopIVarContainer() }()
	for _, expr := range c.Exprs {
		if d, err := expr.Eval(ctx); err != nil {
			return err
		} else if res, err := tree.GetBool(d); err != nil {
			return err
		} else if !res && d != tree.DNull {
			// Failed to satisfy CHECK constraint.
			return pgerror.NewErrorf(pgerror.CodeCheckViolationError,
				"failed to satisfy CHECK constraint (%s)", expr)
		}
	}
	return nil
}
