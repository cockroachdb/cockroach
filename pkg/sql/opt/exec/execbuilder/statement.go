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
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
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

func (b *Builder) buildCreateView(cv *memo.CreateViewExpr) (execPlan, error) {
	md := b.mem.Metadata()
	schema := md.Schema(cv.Schema)
	cols := make(sqlbase.ResultColumns, len(cv.Columns))
	for i := range cols {
		cols[i].Name = cv.Columns[i].Alias
		cols[i].Typ = md.ColumnMeta(cv.Columns[i].ID).Type
	}
	root, err := b.factory.ConstructCreateView(
		schema,
		cv.ViewName,
		cv.IfNotExists,
		cv.Replace,
		cv.Temporary,
		cv.ViewQuery,
		cols,
		cv.Deps,
	)
	return execPlan{root: root}, err
}

func (b *Builder) buildExplain(explain *memo.ExplainExpr) (execPlan, error) {
	var node exec.Node

	if explain.Options.Mode == tree.ExplainOpt {
		fmtFlags := memo.ExprFmtHideAll
		switch {
		case explain.Options.Flags[tree.ExplainFlagVerbose]:
			fmtFlags = memo.ExprFmtHideQualifications | memo.ExprFmtHideScalars |
				memo.ExprFmtHideTypes | memo.ExprFmtHideNotNull

		case explain.Options.Flags[tree.ExplainFlagTypes]:
			fmtFlags = memo.ExprFmtHideQualifications
		}

		// Format the plan here and pass it through to the exec factory.

		// If catalog option was passed, show catalog object details for all tables.
		var planText bytes.Buffer
		if explain.Options.Flags[tree.ExplainFlagCatalog] {
			for _, t := range b.mem.Metadata().AllTables() {
				tp := treeprinter.New()
				cat.FormatTable(b.catalog, t.Table, tp)
				planText.WriteString(tp.String())
			}
			// TODO(radu): add views, sequences
		}

		f := memo.MakeExprFmtCtx(fmtFlags, b.mem, b.catalog)
		f.FormatExpr(explain.Input)
		planText.WriteString(f.Buffer.String())

		// If we're going to display the environment, there's a bunch of queries we
		// need to run to get that information, and we can't run them from here, so
		// tell the exec factory what information it needs to fetch.
		var envOpts exec.ExplainEnvData
		if explain.Options.Flags[tree.ExplainFlagEnv] {
			envOpts = b.getEnvData()
		}

		var err error
		node, err = b.factory.ConstructExplainOpt(planText.String(), envOpts)
		if err != nil {
			return execPlan{}, err
		}
	} else {

		// The auto commit flag should reflect what would happen if this statement
		// was run without the explain, so recalculate it.
		defer func(oldVal bool) {
			b.allowAutoCommit = oldVal
		}(b.allowAutoCommit)
		b.allowAutoCommit = b.canAutoCommit(explain.Input)

		input, err := b.buildRelational(explain.Input)
		if err != nil {
			return execPlan{}, err
		}

		plan, err := b.factory.ConstructPlan(input.root, b.subqueries, b.cascades, b.checks)
		if err != nil {
			return execPlan{}, err
		}

		node, err = b.factory.ConstructExplain(&explain.Options, explain.StmtType, plan)
		if err != nil {
			return execPlan{}, err
		}
	}

	ep := execPlan{root: node}
	for i, c := range explain.ColList {
		ep.outputCols.Set(int(c), i)
	}
	// The subqueries/cascades/checks are now owned by the explain node;
	// remove them so they don't also show up in the final plan.
	b.subqueries = nil
	b.cascades = nil
	b.checks = nil
	return ep, nil
}

func (b *Builder) buildShowTrace(show *memo.ShowTraceForSessionExpr) (execPlan, error) {
	node, err := b.factory.ConstructShowTrace(show.TraceType, show.Compact)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, c := range show.ColList {
		ep.outputCols.Set(int(c), i)
	}
	return ep, nil
}

func (b *Builder) buildAlterTableSplit(split *memo.AlterTableSplitExpr) (execPlan, error) {
	input, err := b.buildRelational(split.Input)
	if err != nil {
		return execPlan{}, err
	}
	scalarCtx := buildScalarCtx{}
	expiration, err := b.buildScalar(&scalarCtx, split.Expiration)
	if err != nil {
		return execPlan{}, err
	}
	table := b.mem.Metadata().Table(split.Table)
	node, err := b.factory.ConstructAlterTableSplit(
		table.Index(split.Index),
		input.root,
		expiration,
	)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, c := range split.Columns {
		ep.outputCols.Set(int(c), i)
	}
	return ep, nil
}

func (b *Builder) buildAlterTableUnsplit(unsplit *memo.AlterTableUnsplitExpr) (execPlan, error) {
	input, err := b.buildRelational(unsplit.Input)
	if err != nil {
		return execPlan{}, err
	}
	table := b.mem.Metadata().Table(unsplit.Table)
	node, err := b.factory.ConstructAlterTableUnsplit(
		table.Index(unsplit.Index),
		input.root,
	)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, c := range unsplit.Columns {
		ep.outputCols.Set(int(c), i)
	}
	return ep, nil
}

func (b *Builder) buildAlterTableUnsplitAll(
	unsplitAll *memo.AlterTableUnsplitAllExpr,
) (execPlan, error) {
	table := b.mem.Metadata().Table(unsplitAll.Table)
	node, err := b.factory.ConstructAlterTableUnsplitAll(table.Index(unsplitAll.Index))
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, c := range unsplitAll.Columns {
		ep.outputCols.Set(int(c), i)
	}
	return ep, nil
}

func (b *Builder) buildAlterTableRelocate(relocate *memo.AlterTableRelocateExpr) (execPlan, error) {
	input, err := b.buildRelational(relocate.Input)
	if err != nil {
		return execPlan{}, err
	}
	table := b.mem.Metadata().Table(relocate.Table)
	node, err := b.factory.ConstructAlterTableRelocate(
		table.Index(relocate.Index),
		input.root,
		relocate.RelocateLease,
	)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, c := range relocate.Columns {
		ep.outputCols.Set(int(c), i)
	}
	return ep, nil
}

func (b *Builder) buildControlJobs(ctl *memo.ControlJobsExpr) (execPlan, error) {
	input, err := b.buildRelational(ctl.Input)
	if err != nil {
		return execPlan{}, err
	}
	node, err := b.factory.ConstructControlJobs(
		ctl.Command,
		input.root,
	)
	if err != nil {
		return execPlan{}, err
	}
	// ControlJobs returns no columns.
	return execPlan{root: node}, nil
}

func (b *Builder) buildCancelQueries(cancel *memo.CancelQueriesExpr) (execPlan, error) {
	input, err := b.buildRelational(cancel.Input)
	if err != nil {
		return execPlan{}, err
	}
	node, err := b.factory.ConstructCancelQueries(input.root, cancel.IfExists)
	if err != nil {
		return execPlan{}, err
	}
	if !b.disableTelemetry {
		telemetry.Inc(sqltelemetry.CancelQueriesUseCounter)
	}
	// CancelQueries returns no columns.
	return execPlan{root: node}, nil
}

func (b *Builder) buildCancelSessions(cancel *memo.CancelSessionsExpr) (execPlan, error) {
	input, err := b.buildRelational(cancel.Input)
	if err != nil {
		return execPlan{}, err
	}
	node, err := b.factory.ConstructCancelSessions(input.root, cancel.IfExists)
	if err != nil {
		return execPlan{}, err
	}
	if !b.disableTelemetry {
		telemetry.Inc(sqltelemetry.CancelSessionsUseCounter)
	}
	// CancelSessions returns no columns.
	return execPlan{root: node}, nil
}

func (b *Builder) buildExport(export *memo.ExportExpr) (execPlan, error) {
	input, err := b.buildRelational(export.Input)
	if err != nil {
		return execPlan{}, err
	}

	scalarCtx := buildScalarCtx{}
	fileName, err := b.buildScalar(&scalarCtx, export.FileName)
	if err != nil {
		return execPlan{}, err
	}

	opts := make([]exec.KVOption, len(export.Options))
	for i, o := range export.Options {
		opts[i].Key = o.Key
		var err error
		opts[i].Value, err = b.buildScalar(&scalarCtx, o.Value)
		if err != nil {
			return execPlan{}, err
		}
	}

	node, err := b.factory.ConstructExport(
		input.root,
		fileName,
		export.FileFormat,
		opts,
	)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, c := range export.Columns {
		ep.outputCols.Set(int(c), i)
	}
	return ep, nil
}
