// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (b *Builder) buildCreateTable(ct *memo.CreateTableExpr) (execPlan, error) {
	var root exec.Node
	if ct.Syntax.As() {
		// Construct AS input to CREATE TABLE.
		input, err := b.buildRelational(ct.Input)
		if err != nil {
			return execPlan{}, err
		}
		// Impose ordering and naming on input columns, so that they match the
		// order and names of the table columns into which values will be
		// inserted.
		colList := make(opt.ColList, len(ct.InputCols))
		colNames := make([]string, len(ct.InputCols))
		for i := range ct.InputCols {
			colList[i] = ct.InputCols[i].ID
			colNames[i] = ct.InputCols[i].Alias
		}
		input, err = b.ensureColumns(input, colList, colNames, nil /* provided */)
		if err != nil {
			return execPlan{}, err
		}
		root = input.root
	}

	schema := b.mem.Metadata().Schema(ct.Schema)
	root, err := b.factory.ConstructCreateTable(root, schema, ct.Syntax)
	return execPlan{root: root}, err
}

func (b *Builder) buildExplain(explain *memo.ExplainExpr) (execPlan, error) {
	var node exec.Node

	if explain.Options.Mode == tree.ExplainOpt {
		fmtFlags := memo.ExprFmtHideAll
		switch {
		case explain.Options.Flags.Contains(tree.ExplainFlagVerbose):
			fmtFlags = memo.ExprFmtHideQualifications | memo.ExprFmtHideScalars | memo.ExprFmtHideTypes

		case explain.Options.Flags.Contains(tree.ExplainFlagTypes):
			fmtFlags = memo.ExprFmtHideQualifications
		}

		// Format the plan here and pass it through to the exec factory.
		f := memo.MakeExprFmtCtx(fmtFlags, b.mem)
		f.FormatExpr(explain.Input)
		planText := f.Buffer.String()

		// If we're going to display the environment, there's a bunch of queries we
		// need to run to get that information, and we can't run them from here, so
		// tell the exec factory what information it needs to fetch.
		var envOpts exec.ExplainEnvData
		if explain.Options.Flags.Contains(tree.ExplainFlagEnv) {
			envOpts = b.getEnvData()
		}

		var err error
		node, err = b.factory.ConstructExplainOpt(planText, envOpts)
		if err != nil {
			return execPlan{}, err
		}
	} else {
		input, err := b.buildRelational(explain.Input)
		if err != nil {
			return execPlan{}, err
		}

		plan, err := b.factory.ConstructPlan(input.root, b.subqueries, b.postqueries)
		if err != nil {
			return execPlan{}, err
		}

		node, err = b.factory.ConstructExplain(&explain.Options, explain.StmtType, plan)
		if err != nil {
			return execPlan{}, err
		}
	}

	ep := execPlan{root: node}
	for i := range explain.ColList {
		ep.outputCols.Set(int(explain.ColList[i]), i)
	}
	// The subqueries are now owned by the explain node; remove them so they don't
	// also show up in the final plan.
	b.subqueries = b.subqueries[:0]
	return ep, nil
}

func (b *Builder) buildShowTrace(show *memo.ShowTraceForSessionExpr) (execPlan, error) {
	node, err := b.factory.ConstructShowTrace(show.TraceType, show.Compact)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i := range show.ColList {
		ep.outputCols.Set(int(show.ColList[i]), i)
	}
	// The subqueries are now owned by the explain node; remove them so they don't
	// also show up in the final plan.
	return ep, nil
}
