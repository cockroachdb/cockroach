// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// CheckHelper validates check constraints on rows, on INSERT and UPDATE.
// CheckHelper has two different modes for executing check constraints:
//
//   1. Eval: in this mode, CheckHelper analyzes and evaluates each check
//            constraint as a standalone expression. This is used by the
//            heuristic planner, and is the backwards-compatible code path.
//
//   2. Input: in this mode, each check constraint expression is integrated with
//             the input expression as a boolean column. CheckHelper only
//             inspects the value of the column; if false, it reports a
//             constraint violation error. This mode is used by the cost-based
//             optimizer.
//
// In the Eval mode, callers should call NewEvalCheckHelper to initialize a new
// instance of CheckHelper. For each row, they call LoadEvalRow one or more
// times to set row values for evaluation, and then call CheckEval to trigger
// evaluation.
//
// In the Input mode, callers should call NewInputCheckHelper to initialize a
// new instance of CheckHelper. For each row, they call CheckInput with the
// boolean check columns.
type CheckHelper struct {
	Exprs        []tree.TypedExpr
	cols         []ColumnDescriptor
	sourceInfo   *DataSourceInfo
	ivarHelper   *tree.IndexedVarHelper
	curSourceRow tree.Datums
	checkSet     util.FastIntSet
	tableDesc    *ImmutableTableDescriptor
}

// AnalyzeExprFunction is the function type used by the CheckHelper during
// initialization to analyze an expression. See sql/analyze_expr.go for details
// about the function.
type AnalyzeExprFunction func(
	ctx context.Context,
	raw tree.Expr,
	sources MultiSourceInfo,
	iVarHelper tree.IndexedVarHelper,
	expectedType *types.T,
	requireType bool,
	typingContext string,
) (tree.TypedExpr, error)

// NewEvalCheckHelper constructs a new instance of the CheckHelper, to be used
// in the "Eval" mode (see comment for the CheckHelper struct).
func NewEvalCheckHelper(
	ctx context.Context, analyzeExpr AnalyzeExprFunction, tableDesc *ImmutableTableDescriptor,
) (*CheckHelper, error) {
	if len(tableDesc.ActiveChecks()) == 0 {
		return nil, nil
	}

	c := &CheckHelper{}
	c.cols = tableDesc.Columns
	c.sourceInfo = NewSourceInfoForSingleTable(
		tree.MakeUnqualifiedTableName(tree.Name(tableDesc.Name)),
		ResultColumnsFromColDescs(tableDesc.Columns),
	)

	c.Exprs = make([]tree.TypedExpr, len(tableDesc.ActiveChecks()))
	exprStrings := make([]string, len(tableDesc.ActiveChecks()))
	for i, check := range tableDesc.ActiveChecks() {
		exprStrings[i] = check.Expr
	}
	exprs, err := parser.ParseExprs(exprStrings)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		c.Exprs[i] = typedExpr
	}
	c.ivarHelper = &ivarHelper
	c.curSourceRow = make(tree.Datums, len(c.cols))
	return c, nil
}

// NewInputCheckHelper constructs a new instance of the CheckHelper, to be used
// in the "Input" mode (see comment for the CheckHelper struct).
func NewInputCheckHelper(checks util.FastIntSet, tableDesc *ImmutableTableDescriptor) *CheckHelper {
	if checks.Empty() {
		return nil
	}
	return &CheckHelper{checkSet: checks, tableDesc: tableDesc}
}

// Count returns the number of check constraints that need to be checked. The
// count can be less than the number of check constraints defined on the table
// descriptor if the planner was able to statically prove that some have already
// been fulfilled.
func (c *CheckHelper) Count() int {
	if len(c.Exprs) != 0 {
		return len(c.Exprs)
	}
	return c.checkSet.Len()
}

// NeedsEval returns true if CheckHelper is operating in the "Eval" mode. See
// the comment for the CheckHelper struct for more details.
func (c *CheckHelper) NeedsEval() bool {
	return len(c.Exprs) != 0
}

// LoadEvalRow sets values in the IndexedVars used by the CHECK exprs.
// Any value not passed is set to NULL, unless `merge` is true, in which
// case it is left unchanged (allowing updating a subset of a row's values).
func (c *CheckHelper) LoadEvalRow(colIdx map[ColumnID]int, row tree.Datums, merge bool) error {
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
func (c *CheckHelper) IndexedVarResolvedType(idx int) *types.T {
	return c.sourceInfo.SourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the parser.IndexedVarContainer interface.
func (c *CheckHelper) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return c.sourceInfo.NodeFormatter(idx)
}

// CheckEval evaluates each check constraint expression using values from the
// current row that was previously set via a call to LoadEvalRow.
func (c *CheckHelper) CheckEval(ctx *tree.EvalContext) error {
	ctx.PushIVarContainer(c)
	defer func() { ctx.PopIVarContainer() }()
	for _, expr := range c.Exprs {
		if d, err := expr.Eval(ctx); err != nil {
			return err
		} else if res, err := tree.GetBool(d); err != nil {
			return err
		} else if !res && d != tree.DNull {
			// Failed to satisfy CHECK constraint.
			return pgerror.Newf(pgcode.CheckViolation,
				"failed to satisfy CHECK constraint (%s)", expr)
		}
	}
	return nil
}

// CheckInput expects checkVals to already contain the boolean result of
// evaluating each check constraint. If any of the boolean values is false, then
// CheckInput reports a constraint violation error.
func (c *CheckHelper) CheckInput(checkVals tree.Datums) error {
	if len(checkVals) != c.checkSet.Len() {
		return errors.AssertionFailedf(
			"mismatched check constraint columns: expected %d, got %d", c.checkSet.Len(), len(checkVals))
	}

	for i, check := range c.tableDesc.ActiveChecks() {
		if !c.checkSet.Contains(i) {
			continue
		}

		if res, err := tree.GetBool(checkVals[i]); err != nil {
			return err
		} else if !res && checkVals[i] != tree.DNull {
			// Failed to satisfy CHECK constraint.
			return pgerror.Newf(pgcode.CheckViolation,
				"failed to satisfy CHECK constraint (%s)", check.Expr)
		}
	}
	return nil
}
