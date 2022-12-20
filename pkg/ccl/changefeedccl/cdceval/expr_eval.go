// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/errors"
)

// Evaluator is a responsible for evaluating expressions in CDC.
type Evaluator struct {
	norm *NormalizedSelectClause

	// Plan related state.
	input     execinfra.RowReceiver
	planGroup ctxgroup.Group
	errCh     chan error
	currDesc  *cdcevent.EventDescriptor
	prevDesc  *cdcevent.EventDescriptor

	// Execution context.
	execCfg    *sql.ExecutorConfig
	user       username.SQLUsername
	fnResolver CDCFunctionResolver

	// rowCh receives projection datums.
	rowCh      chan tree.Datums
	projection cdcevent.Projection

	// rowEvalCtx contains state necessary to evaluate expressions.
	// updated for each row.
	rowEvalCtx rowEvalContext
}

// NewEvaluator constructs new evaluator for changefeed expression.
func NewEvaluator(
	sc *tree.SelectClause, execCfg *sql.ExecutorConfig, user username.SQLUsername,
) (*Evaluator, error) {
	e := Evaluator{
		execCfg: execCfg,
		user:    user,
		norm: &NormalizedSelectClause{
			SelectClause: sc,
		},
		rowCh: make(chan tree.Datums, 1),
	}

	// Arrange to be notified when event does not match predicate.
	predicateAsProjection(e.norm)

	return &e, nil
}

// Eval evaluates projection for the specified updated and (optional) previous row.
// Returns projection result.  If the filter does not match the event, returns
// "zero" Row.
func (e *Evaluator) Eval(
	ctx context.Context, updatedRow cdcevent.Row, prevRow cdcevent.Row,
) (projection cdcevent.Row, evalErr error) {
	defer func() {
		if evalErr != nil {
			// If we can't evaluate a row, we are bound to keep failing.
			// So mark error permanent.
			evalErr = changefeedbase.WithTerminalError(evalErr)
		}
	}()

	if !e.sameVersion(updatedRow.EventDescriptor, prevRow.EventDescriptor) {
		// Descriptor versions changed; re-initialize.
		if err := e.closeErr(); err != nil {
			return cdcevent.Row{}, err
		}

		e.errCh = make(chan error, 1)
		e.currDesc, e.prevDesc = updatedRow.EventDescriptor, prevRow.EventDescriptor

		if err := e.planAndRun(ctx); err != nil {
			return cdcevent.Row{}, err
		}
	}

	// Setup context.
	if err := e.setupContextForRow(ctx, updatedRow, prevRow); err != nil {
		return cdcevent.Row{}, err
	}

	// Push data into DistSQL.
	if st := e.input.Push(updatedRow.EncDatums(), nil); st != execinfra.NeedMoreRows {
		return cdcevent.Row{}, errors.Newf("evaluator shutting down due to status %s", st)
	}

	// Read the evaluation result.
	select {
	case <-ctx.Done():
		return cdcevent.Row{}, ctx.Err()
	case err := <-e.errCh:
		return cdcevent.Row{}, err
	case row := <-e.rowCh:
		filter, err := tree.GetBool(row[0])
		if err != nil {
			return cdcevent.Row{}, err
		}
		if !filter {
			// Filter did not match.
			return cdcevent.Row{}, nil
		}
		// Strip out temporary boolean value (result of the WHERE clause)
		// since this information is not sent to the consumer.
		row = row[1:]

		for i, d := range row {
			if err := e.projection.SetValueDatumAt(i, d); err != nil {
				return cdcevent.Row{}, err
			}
		}
		projection, err := e.projection.Project(updatedRow)
		if err != nil {
			return cdcevent.Row{}, err
		}
		return projection, nil
	}
}

// sameVersion returns true if row descriptor versions match this evaluator
// versions. Note: current, and previous maybe at different versions, but we
// don't really care about that.
func (e *Evaluator) sameVersion(curr, prev *cdcevent.EventDescriptor) bool {
	if e.currDesc == nil {
		return false
	}
	if sameVersion, sameTypes := e.currDesc.EqualsWithUDTCheck(curr); !(sameVersion && sameTypes) {
		return false
	}

	if !e.norm.RequiresPrev() {
		return true
	}
	sameVersion, sameTypes := e.prevDesc.EqualsWithUDTCheck(prev)
	return sameVersion && sameTypes
}

// planAndRun plans CDC expression and starts execution pipeline.
func (e *Evaluator) planAndRun(ctx context.Context) (err error) {
	var plan sql.CDCExpressionPlan
	if err := e.preparePlan(ctx, &plan); err != nil {
		return err
	}
	e.setupProjection(plan.Presentation)
	e.input, err = e.executePlan(ctx, plan)
	return err
}

func (e *Evaluator) preparePlan(ctx context.Context, plan *sql.CDCExpressionPlan) error {
	return withPlanner(
		ctx, e.execCfg, e.user, e.currDesc.SchemaTS, sessiondatapb.SessionData{},
		func(ctx context.Context, execCtx sql.JobExecContext) error {
			semaCtx := execCtx.SemaCtx()
			semaCtx.FunctionResolver = &e.fnResolver
			semaCtx.Properties.Require("cdc", rejectInvalidCDCExprs)
			semaCtx.Annotations = tree.MakeAnnotations(cdcAnnotationAddr)

			evalCtx := execCtx.ExtendedEvalContext().Context
			evalCtx.Annotations = &semaCtx.Annotations
			evalCtx.Annotations.Set(cdcAnnotationAddr, &e.rowEvalCtx)

			e.norm.desc = e.currDesc
			if e.norm.RequiresPrev() {
				e.rowEvalCtx.prevRowTuple = e.fnResolver.setPrevFuncForEventDescriptor(e.prevDesc)
			} else {
				e.rowEvalCtx.prevRowTuple = nil
			}

			p, err := sql.PlanCDCExpression(ctx, execCtx, e.norm.SelectStatementForFamily(e.currDesc.FamilyID))
			if err != nil {
				return err
			}
			*plan = p
			return nil
		},
	)
}

// setupProjection configures evaluator projection.
func (e *Evaluator) setupProjection(presentation colinfo.ResultColumns) {
	e.projection = cdcevent.MakeProjection(e.currDesc)

	// makeUniqueName returns a unique name for the specified name. We do this
	// because seeing same named fields in JSON output might be confusing (though
	// allowed).
	nameUseCount := make(map[string]int, len(presentation))
	makeUniqueName := func(as string) string {
		useCount := nameUseCount[as]
		nameUseCount[as]++
		if useCount > 0 {
			as = fmt.Sprintf("%s_%d", as, useCount)
		}
		return as
	}

	// Add presentation columns to the final project, skipping the first
	// column which contains the result of the filter evaluation.
	for i := 1; i < len(presentation); i++ {
		c := presentation[i]
		e.projection.AddValueColumn(makeUniqueName(c.Name), c.Typ)
	}
}

// inputSpecForEventDescriptor returns input specification for the
// event descriptor.
func inputSpecForEventDescriptor(
	ed *cdcevent.EventDescriptor,
) ([]*types.T, catalog.TableColMap, error) {
	numCols := len(ed.ResultColumns()) + len(colinfo.AllSystemColumnDescs)
	inputTypes := make([]*types.T, numCols)
	var inputCols catalog.TableColMap
	for i, c := range ed.ResultColumns() {
		col, err := ed.TableDescriptor().FindColumnWithName(tree.Name(c.Name))
		if err != nil {
			return inputTypes, inputCols, err
		}
		inputCols.Set(col.GetID(), i)
		inputTypes[i] = c.Typ
	}

	// Add system columns.
	for _, sc := range colinfo.AllSystemColumnDescs {
		inputCols.Set(sc.ID, inputCols.Len())
		inputTypes = append(inputTypes, sc.Type)
	}

	return inputTypes, inputCols, nil
}

// executePlan starts execution of the plan and returns input which receives
// rows that need to be evaluated.
func (e *Evaluator) executePlan(
	ctx context.Context, plan sql.CDCExpressionPlan,
) (inputReceiver execinfra.RowReceiver, err error) {
	// Configure input.
	inputTypes, inputCols, err := inputSpecForEventDescriptor(e.currDesc)
	if err != nil {
		return nil, err
	}

	// The row channel created below will have exactly 1 sender (this evaluator).
	// The buffer size parameter doesn't matter much, as long as it is greater
	// than 0 to make sure that if the main context is cancelled and the flow
	// exits, that we can still push data into the row channel without blocking,
	// so that we notice cancellation request when we try to read the result of
	// the evaluation.
	const numSenders = 1
	const bufSize = 16
	var input execinfra.RowChannel
	input.InitWithBufSizeAndNumSenders(inputTypes, bufSize, numSenders)

	// writer sends result of the evaluation into row channel.
	writer := sql.NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e.rowCh <- row:
			return nil
		}
	})

	// receiver writes the results to the writer.
	receiver := sql.MakeDistSQLReceiver(
		ctx,
		writer,
		tree.Rows,
		e.execCfg.RangeDescriptorCache,
		nil,
		nil, /* clockUpdater */
		&sql.SessionTracing{},
		e.execCfg.ContentionRegistry,
	)

	// Start execution.
	e.planGroup = ctxgroup.WithContext(ctx)
	e.planGroup.GoCtx(func(ctx context.Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = errors.Newf("error evaluating CDC expression %q: %s",
					tree.AsString(e.norm.SelectClause), r)
			}
			e.errCh <- err
		}()

		defer receiver.Release()
		if err := sql.RunCDCEvaluation(ctx, plan, &input, inputCols, receiver); err != nil {
			return err
		}
		return writer.Err()
	})

	return &input, nil
}

// setupContextForRow configures evaluation context with the provided row
// information.
func (e *Evaluator) setupContextForRow(ctx context.Context, updated, prev cdcevent.Row) error {
	e.rowEvalCtx.ctx = ctx
	e.rowEvalCtx.updatedRow = updated
	if e.norm.RequiresPrev() {
		if err := prev.CopyInto(e.rowEvalCtx.prevRowTuple); err != nil {
			return err
		}
	}
	return nil
}

// Close closes currently running execution.
func (e *Evaluator) Close() {
	_ = e.closeErr() // We expect to see an error, such as context cancelled.
}

func (e *Evaluator) closeErr() error {
	if e.errCh != nil {
		defer close(e.errCh) // Must be deferred since planGroup  go routine might write.
	}

	if e.input != nil {
		e.input.ProducerDone()
		e.input = nil
		return e.planGroup.Wait()
	}
	return nil
}

// rowEvalContext represents the context needed to evaluate row expressions.
type rowEvalContext struct {
	ctx          context.Context
	updatedRow   cdcevent.Row
	prevRowTuple *tree.DTuple
}

// cdcAnnotationAddr is the address used to store relevant information
// in the Annotation field of evalCtx when evaluating expressions.
const cdcAnnotationAddr tree.AnnotationIdx = iota + 1

// rowEvalContextFromEvalContext returns rowEvalContext stored as an annotation
// in evalCtx.
func rowEvalContextFromEvalContext(evalCtx *eval.Context) *rowEvalContext {
	return evalCtx.Annotations.Get(cdcAnnotationAddr).(*rowEvalContext)
}

const rejectInvalidCDCExprs = tree.RejectAggregates | tree.RejectGenerators |
	tree.RejectWindowApplications | tree.RejectNestedGenerators

// configSemaForCDC configures existing semaCtx to be used for CDC expression
// evaluation; returns cleanup function which restores previous configuration.
func configSemaForCDC(semaCtx *tree.SemaContext, d *cdcevent.EventDescriptor) func() {
	origProps, origResolver := semaCtx.Properties, semaCtx.FunctionResolver
	var r CDCFunctionResolver
	r.setPrevFuncForEventDescriptor(d)
	semaCtx.FunctionResolver = &r
	semaCtx.Properties.Require("cdc", rejectInvalidCDCExprs)

	return func() {
		semaCtx.Properties.Restore(origProps)
		semaCtx.FunctionResolver = origResolver
	}
}

// predicateAsProjection replaces predicate (where clause) with a projection
// (select clause). The "matches" predicate will be the first predicate. This
// step is done so that distSQL notifies us about the events that should be
// filtered, as opposed to filtering those events directly, since we need to
// perform cleanup tasks (release allocation, update metrics, etc.), even
// for events that do not match the predicate.
func predicateAsProjection(n *NormalizedSelectClause) {
	filter := tree.SelectExpr{
		Expr: tree.DBoolTrue,
		As:   "__crdb_filter",
	}

	if n.Where != nil {
		filter.Expr = &tree.ParenExpr{Expr: n.Where.Expr}
		n.Where = nil
	}

	n.SelectClause.Exprs = append(tree.SelectExprs{filter}, n.SelectClause.Exprs...)
}
