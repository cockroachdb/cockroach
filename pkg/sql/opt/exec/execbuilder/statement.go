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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/redact"
)

func (b *Builder) buildCreateTable(
	ct *memo.CreateTableExpr,
) (_ execPlan, outputCols colOrdMap, err error) {

	schema := b.mem.Metadata().Schema(ct.Schema)
	if !ct.Syntax.As() {
		root, err := b.factory.ConstructCreateTable(schema, ct.Syntax)
		return execPlan{root: root}, colOrdMap{}, err
	}

	// Construct AS input to CREATE TABLE.
	input, inputCols, err := b.buildRelational(ct.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	// Impose ordering and naming on input columns, so that they match the
	// order and names of the table columns into which values will be
	// inserted.
	input, _, err = b.applyPresentation(input, inputCols, ct.InputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	root, err := b.factory.ConstructCreateTableAs(input.root, schema, ct.Syntax)
	return execPlan{root: root}, colOrdMap{}, err
}

func (b *Builder) buildCreateView(
	cv *memo.CreateViewExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	md := b.mem.Metadata()
	schema := md.Schema(cv.Schema)
	cols := make(colinfo.ResultColumns, len(cv.Columns))
	for i := range cols {
		cols[i].Name = cv.Columns[i].Alias
		cols[i].Typ = md.ColumnMeta(cv.Columns[i].ID).Type
	}
	root, err := b.factory.ConstructCreateView(
		cv.Syntax,
		schema,
		cv.ViewQuery,
		cols,
		cv.Deps,
		cv.TypeDeps,
	)
	return execPlan{root: root}, colOrdMap{}, err
}

func (b *Builder) buildCreateFunction(
	cf *memo.CreateFunctionExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	md := b.mem.Metadata()
	schema := md.Schema(cf.Schema)
	root, err := b.factory.ConstructCreateFunction(
		schema,
		cf.Syntax,
		cf.Deps,
		cf.TypeDeps,
	)
	return execPlan{root: root}, colOrdMap{}, err
}

func (b *Builder) buildExplainOpt(
	explain *memo.ExplainExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	fmtFlags := memo.ExprFmtHideAll
	switch {
	case explain.Options.Flags[tree.ExplainFlagVerbose]:
		fmtFlags = memo.ExprFmtHideQualifications | memo.ExprFmtHideScalars |
			memo.ExprFmtHideTypes | memo.ExprFmtHideNotNull | memo.ExprFmtHideNotVisibleIndexInfo |
			memo.ExprFmtHideFastPathChecks

	case explain.Options.Flags[tree.ExplainFlagTypes]:
		fmtFlags = memo.ExprFmtHideQualifications | memo.ExprFmtHideNotVisibleIndexInfo
	}
	redactValues := explain.Options.Flags[tree.ExplainFlagRedact]

	// Format the plan here and pass it through to the exec factory.

	// If catalog option was passed, show catalog object details for all tables.
	var planText bytes.Buffer
	if explain.Options.Flags[tree.ExplainFlagCatalog] {
		for _, t := range b.mem.Metadata().AllTables() {
			tp := treeprinter.New()
			cat.FormatTable(b.ctx, b.catalog, t.Table, tp, redactValues)
			catStr := tp.String()
			if redactValues {
				catStr = string(redact.RedactableString(catStr).Redact())
			}
			planText.WriteString(catStr)
		}
		// TODO(radu): add views, sequences
	}

	// If MEMO option was passed, show the memo.
	if explain.Options.Flags[tree.ExplainFlagMemo] {
		memoStr := b.optimizer.FormatMemo(xform.FmtPretty, redactValues)
		if redactValues {
			memoStr = string(redact.RedactableString(memoStr).Redact())
		}
		planText.WriteString(memoStr)
	}

	f := memo.MakeExprFmtCtx(b.ctx, fmtFlags, redactValues, b.mem, b.catalog)
	f.FormatExpr(explain.Input)
	planStr := f.Buffer.String()
	if redactValues {
		planStr = string(redact.RedactableString(planStr).Redact())
	}
	planText.WriteString(planStr)

	// If we're going to display the environment, there's a bunch of queries we
	// need to run to get that information, and we can't run them from here, so
	// tell the exec factory what information it needs to fetch.
	var envOpts exec.ExplainEnvData
	if explain.Options.Flags[tree.ExplainFlagEnv] {
		var err error
		envOpts, err = b.getEnvData()
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
	}

	var ep execPlan
	ep.root, err = b.factory.ConstructExplainOpt(planText.String(), envOpts)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	outputCols, err = b.outputColsFromList(explain.ColList)
	return ep, outputCols, err
}

func (b *Builder) buildExplain(
	explainExpr *memo.ExplainExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	if explainExpr.Options.Mode == tree.ExplainOpt {
		return b.buildExplainOpt(explainExpr)
	}

	var ep execPlan
	ep.root, err = b.factory.ConstructExplain(
		&explainExpr.Options,
		explainExpr.StmtType,
		func(f exec.Factory) (exec.Plan, error) {
			// Create a separate builder for the explain query.	buildRelational
			// annotates nodes with extra information when the factory is an
			// exec.ExplainFactory so it must be the outer factory and the gist
			// factory must be the inner factory.
			gf := explain.NewPlanGistFactory(f)
			ef := explain.NewFactory(gf, b.semaCtx, b.evalCtx)

			explainBld := New(
				b.ctx, ef, b.optimizer, b.mem, b.catalog, explainExpr.Input,
				b.semaCtx, b.evalCtx, b.initialAllowAutoCommit, b.IsANSIDML,
			)
			explainBld.disableTelemetry = true
			plan, err := explainBld.Build()
			if err != nil {
				return nil, err
			}
			explainPlan := plan.(*explain.Plan)
			explainPlan.Gist = gf.PlanGist()
			return plan, nil
		},
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	outputCols, err = b.outputColsFromList(explainExpr.ColList)
	return ep, outputCols, err
}

func (b *Builder) buildShowTrace(
	show *memo.ShowTraceForSessionExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	var ep execPlan
	ep.root, err = b.factory.ConstructShowTrace(show.TraceType, show.Compact)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	outputCols, err = b.outputColsFromList(show.ColList)
	return ep, outputCols, err
}

func (b *Builder) buildAlterTableSplit(
	split *memo.AlterTableSplitExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, _, err := b.buildRelational(split.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	scalarCtx := buildScalarCtx{}
	expiration, err := b.buildScalar(&scalarCtx, split.Expiration)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	table := b.mem.Metadata().Table(split.Table)
	var ep execPlan
	ep.root, err = b.factory.ConstructAlterTableSplit(
		table.Index(split.Index),
		input.root,
		expiration,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	outputCols, err = b.outputColsFromList(split.Columns)
	return ep, outputCols, err
}

func (b *Builder) buildAlterTableUnsplit(
	unsplit *memo.AlterTableUnsplitExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, _, err := b.buildRelational(unsplit.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	table := b.mem.Metadata().Table(unsplit.Table)
	var ep execPlan
	ep.root, err = b.factory.ConstructAlterTableUnsplit(
		table.Index(unsplit.Index),
		input.root,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	outputCols, err = b.outputColsFromList(unsplit.Columns)
	return ep, outputCols, err
}

func (b *Builder) buildAlterTableUnsplitAll(
	unsplitAll *memo.AlterTableUnsplitAllExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	table := b.mem.Metadata().Table(unsplitAll.Table)
	var ep execPlan
	ep.root, err = b.factory.ConstructAlterTableUnsplitAll(table.Index(unsplitAll.Index))
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	outputCols, err = b.outputColsFromList(unsplitAll.Columns)
	return ep, outputCols, err
}

func (b *Builder) buildAlterTableRelocate(
	relocate *memo.AlterTableRelocateExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, _, err := b.buildRelational(relocate.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	table := b.mem.Metadata().Table(relocate.Table)
	var ep execPlan
	ep.root, err = b.factory.ConstructAlterTableRelocate(
		table.Index(relocate.Index),
		input.root,
		relocate.SubjectReplicas,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	outputCols, err = b.outputColsFromList(relocate.Columns)
	return ep, outputCols, err
}

func (b *Builder) buildAlterRangeRelocate(
	relocate *memo.AlterRangeRelocateExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, _, err := b.buildRelational(relocate.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	scalarCtx := buildScalarCtx{}
	toStoreID, err := b.buildScalar(&scalarCtx, relocate.ToStoreID)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	fromStoreID, err := b.buildScalar(&scalarCtx, relocate.FromStoreID)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var ep execPlan
	ep.root, err = b.factory.ConstructAlterRangeRelocate(
		input.root,
		relocate.SubjectReplicas,
		toStoreID,
		fromStoreID,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	outputCols, err = b.outputColsFromList(relocate.Columns)
	return ep, outputCols, err
}

func (b *Builder) buildControlJobs(
	ctl *memo.ControlJobsExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, _, err := b.buildRelational(ctl.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	scalarCtx := buildScalarCtx{}
	reason, err := b.buildScalar(&scalarCtx, ctl.Reason)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	var ep execPlan
	ep.root, err = b.factory.ConstructControlJobs(
		ctl.Command,
		input.root,
		reason,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	// ControlJobs returns no columns.
	return ep, colOrdMap{}, nil
}

func (b *Builder) buildControlSchedules(
	ctl *memo.ControlSchedulesExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, _, err := b.buildRelational(ctl.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var ep execPlan
	ep.root, err = b.factory.ConstructControlSchedules(
		ctl.Command,
		input.root,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	// ControlSchedules returns no columns.
	return ep, colOrdMap{}, nil
}

func (b *Builder) buildShowCompletions(
	ctl *memo.ShowCompletionsExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	var ep execPlan
	ep.root, err = b.factory.ConstructShowCompletions(
		ctl.Command,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	outputCols, err = b.outputColsFromList(ctl.Columns)
	return ep, outputCols, err
}

func (b *Builder) buildCancelQueries(
	cancel *memo.CancelQueriesExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, _, err := b.buildRelational(cancel.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var ep execPlan
	ep.root, err = b.factory.ConstructCancelQueries(input.root, cancel.IfExists)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	if !b.disableTelemetry {
		telemetry.Inc(sqltelemetry.CancelQueriesUseCounter)
	}
	// CancelQueries returns no columns.
	return ep, colOrdMap{}, nil
}

func (b *Builder) buildCancelSessions(
	cancel *memo.CancelSessionsExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, _, err := b.buildRelational(cancel.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	node, err := b.factory.ConstructCancelSessions(input.root, cancel.IfExists)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	if !b.disableTelemetry {
		telemetry.Inc(sqltelemetry.CancelSessionsUseCounter)
	}
	// CancelSessions returns no columns.
	return execPlan{root: node}, colOrdMap{}, nil
}

func (b *Builder) buildCreateStatistics(
	c *memo.CreateStatisticsExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	node, err := b.factory.ConstructCreateStatistics(c.Syntax)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	// CreateStatistics returns no columns.
	return execPlan{root: node}, colOrdMap{}, nil
}

func (b *Builder) buildExport(
	export *memo.ExportExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, inputCols, err := b.buildRelational(export.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	scalarCtx := buildScalarCtx{}
	fileName, err := b.buildScalar(&scalarCtx, export.FileName)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	opts := make([]exec.KVOption, len(export.Options))
	for i, o := range export.Options {
		opts[i].Key = o.Key
		var err error
		opts[i].Value, err = b.buildScalar(&scalarCtx, o.Value)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
	}
	notNullColsSet, err := getNodeColumnOrdinalSet(inputCols, export.Input.Relational().NotNullCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	var ep execPlan
	ep.root, err = b.factory.ConstructExport(
		input.root,
		fileName,
		export.FileFormat,
		opts,
		notNullColsSet,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	outputCols, err = b.outputColsFromList(export.Columns)
	return ep, outputCols, err
}

// planWithColumns creates an execPlan for a node which has a fixed output
// schema.
func (b *Builder) outputColsFromList(cols opt.ColList) (colOrdMap, error) {
	outputCols := b.colOrdsAlloc.Alloc()
	for i, c := range cols {
		if err := outputCols.Set(c, i); err != nil {
			return colOrdMap{}, err
		}
	}
	return outputCols, nil
}
