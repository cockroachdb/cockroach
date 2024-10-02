// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Evaluator is a responsible for evaluating expressions in CDC.
type Evaluator struct {
	sc *tree.SelectClause

	statementTS hlc.Timestamp
	// Execution context.
	execCfg     *sql.ExecutorConfig
	user        username.SQLUsername
	sessionData *sessiondata.SessionData
	withDiff    bool
	familyEval  map[descpb.FamilyID]*familyEvaluator
}

// familyEvaluator is a responsible for evaluating expressions in CDC
// targeted to specific column family.
type familyEvaluator struct {
	norm           *NormalizedSelectClause
	targetFamilyID descpb.FamilyID

	// Plan related state.
	cleanup      func()
	input        execinfra.RowReceiver
	planGroup    ctxgroup.Group
	errCh        chan error
	currDesc     *cdcevent.EventDescriptor
	prevDesc     *cdcevent.EventDescriptor
	prevRowTuple *tree.DTuple
	alloc        tree.DatumAlloc

	// Execution context.
	execCfg     *sql.ExecutorConfig
	user        username.SQLUsername
	sessionData *sessiondata.SessionData

	// rowCh receives projection datums.
	rowCh      chan tree.Datums
	projection cdcevent.Projection

	statementTS hlc.Timestamp
	withDiff    bool

	// rowEvalCtx contains state necessary to evaluate expressions.
	// updated for each row.
	// Initialized during preparePlan().
	rowEvalCtx *rowEvalContext
}

// NewEvaluator constructs new evaluator for changefeed expression.
func NewEvaluator(
	sc *tree.SelectClause,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	sd *sessiondata.SessionData,
	statementTS hlc.Timestamp,
	withDiff bool,
) *Evaluator {
	return &Evaluator{
		sc:          sc,
		execCfg:     execCfg,
		user:        user,
		sessionData: sd,
		statementTS: statementTS,
		withDiff:    withDiff,
		familyEval:  make(map[descpb.FamilyID]*familyEvaluator, 1), // usually, just 1 family.
	}
}

// NewEvaluator constructs new familyEvaluator for changefeed expression.
func newFamilyEvaluator(
	sc *tree.SelectClause,
	targetFamilyID descpb.FamilyID,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	sd *sessiondata.SessionData,
	statementTS hlc.Timestamp,
	withDiff bool,
) *familyEvaluator {
	e := familyEvaluator{
		targetFamilyID: targetFamilyID,
		execCfg:        execCfg,
		user:           user,
		sessionData:    sd,
		norm: &NormalizedSelectClause{
			SelectClause: sc,
		},
		rowCh:       make(chan tree.Datums, 1),
		statementTS: statementTS,
		withDiff:    withDiff,
	}

	// Arrange to be notified when event does not match predicate.
	predicateAsProjection(e.norm)

	return &e
}

// Close closes currently running execution.
func (e *Evaluator) Close() {
	for _, fe := range e.familyEval {
		_ = fe.closeErr() // We expect to see an error, such as context cancelled.
	}

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

	fe, ok := e.familyEval[updatedRow.FamilyID]
	if !ok {
		fe = newFamilyEvaluator(
			e.sc, updatedRow.FamilyID, e.execCfg, e.user, e.sessionData, e.statementTS, e.withDiff,
		)
		e.familyEval[updatedRow.FamilyID] = fe
	}

	return fe.eval(ctx, updatedRow, prevRow)
}

// eval evaluates projection for the specified updated and (optional) previous row.
// Returns projection result.  If the filter does not match the event, returns
// "zero" Row.
func (e *familyEvaluator) eval(
	ctx context.Context, updatedRow cdcevent.Row, prevRow cdcevent.Row,
) (projection cdcevent.Row, evalErr error) {
	if updatedRow.FamilyID != e.targetFamilyID {
		return cdcevent.Row{}, errors.AssertionFailedf(
			"row family id (%d) differs from target id (%d)", updatedRow.FamilyID, e.targetFamilyID)
	}

	if prevRow.IsInitialized() && updatedRow.FamilyID != prevRow.FamilyID {
		return cdcevent.Row{}, errors.AssertionFailedf(
			"current family id (%d) differs from previous (%d)", updatedRow.FamilyID, prevRow.FamilyID)
	}

	havePrev := prevRow.IsInitialized()
	if !(sameVersion(e.currDesc, updatedRow.EventDescriptor) &&
		(!havePrev || sameVersion(e.prevDesc, prevRow.EventDescriptor))) {
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

	encDatums := updatedRow.EncDatums()
	if havePrev {
		if prevRow.IsDeleted() {
			encDatums = append(encDatums, rowenc.EncDatum{Datum: tree.DNull})
		} else {
			if err := e.copyPrevRow(prevRow); err != nil {
				return cdcevent.Row{}, err
			}
			encDatums = append(encDatums, rowenc.EncDatum{Datum: e.prevRowTuple})
		}
	}

	// Push data into DistSQL.
	if st := e.input.Push(encDatums, nil); st != execinfra.NeedMoreRows {
		return cdcevent.Row{}, errors.Newf("familyEvaluator shutting down due to status %s", st)
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

// sameVersion returns true if row descriptor versions match.
func sameVersion(currentVersion, newVersion *cdcevent.EventDescriptor) bool {
	if currentVersion == nil {
		return false
	}
	sameVersion, sameTypes := newVersion.EqualsWithUDTCheck(currentVersion)
	return sameVersion && sameTypes
}

// planAndRun plans CDC expression and starts execution pipeline.
func (e *familyEvaluator) planAndRun(ctx context.Context) (err error) {
	if log.V(1) {
		start := timeutil.Now()
		defer func() {
			log.Infof(ctx, "Planning for CDC expression %s (v=%d) took %s (err=%v)",
				tree.AsString(e.norm), e.norm.desc.Version, timeutil.Since(start), err)
		}()
	}

	var plan sql.CDCExpressionPlan
	var prevCol catalog.Column
	plan, prevCol, err = e.preparePlan(ctx)
	if err != nil {
		return withErrorHint(err, e.currDesc.FamilyName, e.currDesc.HasOtherFamilies)
	}

	e.setupProjection(plan.Presentation)
	e.input, err = e.executePlan(ctx, plan, prevCol)
	return err
}

// preparePlan creates a plan for CDC expression. If no error is returned, the
// caller must call e.performCleanup().
func (e *familyEvaluator) preparePlan(
	ctx context.Context,
) (plan sql.CDCExpressionPlan, prevCol catalog.Column, err error) {
	// Perform cleanup of the previous plan if there is one.
	e.performCleanup()

	err = withPlanner(ctx, e.execCfg, e.statementTS, e.user, e.currDesc.SchemaTS, e.sessionData,
		func(ctx context.Context, execCtx sql.JobExecContext, cleanup func()) error {
			e.cleanup = cleanup
			e.rowEvalCtx = rowEvalContextFromEvalContext(&execCtx.ExtendedEvalContext().Context)
			e.rowEvalCtx.withDiff = e.withDiff
			e.rowEvalCtx.creationTime = e.statementTS

			e.norm.desc = e.currDesc
			requiresPrev := e.prevDesc != nil
			var opts []sql.CDCOption
			if requiresPrev {
				prevCol, err = newPrevColumnForDesc(e.prevDesc)
				if err != nil {
					return err
				}
				e.prevRowTuple = tree.NewDTupleWithLen(
					prevCol.GetType(), len(prevCol.GetType().InternalType.TupleContents))
				opts = append(opts, sql.WithExtraColumn(prevCol))
			}

			plan, err = sql.PlanCDCExpression(ctx, execCtx, e.norm.SelectStatementForFamily(), opts...)
			return err
		})
	if err != nil {
		e.performCleanup()
		return sql.CDCExpressionPlan{}, nil, err
	}
	return plan, prevCol, nil
}

// setupProjection configures familyEvaluator projection.
func (e *familyEvaluator) setupProjection(presentation colinfo.ResultColumns) {
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
	ed *cdcevent.EventDescriptor, prevCol catalog.Column,
) ([]*types.T, catalog.TableColMap, error) {
	numCols := len(ed.ResultColumns()) + len(colinfo.AllSystemColumnDescs)
	inputTypes := make([]*types.T, 0, numCols)
	var inputCols catalog.TableColMap
	for i, c := range ed.ResultColumns() {
		col, err := catalog.MustFindColumnByName(ed.TableDescriptor(), c.Name)
		if err != nil {
			return inputTypes, inputCols, err
		}
		inputCols.Set(col.GetID(), i)
		inputTypes = append(inputTypes, c.Typ)
	}

	// Add system columns.
	for _, sc := range colinfo.AllSystemColumnDescs {
		inputCols.Set(sc.ID, inputCols.Len())
		inputTypes = append(inputTypes, sc.Type)
	}

	// Setup cdc_prev if needed.
	if prevCol != nil {
		inputCols.Set(prevCol.GetID(), inputCols.Len())
		inputTypes = append(inputTypes, prevCol.GetType())
	}
	return inputTypes, inputCols, nil
}

// executePlan starts execution of the plan and returns input which receives
// rows that need to be evaluated.
func (e *familyEvaluator) executePlan(
	ctx context.Context, plan sql.CDCExpressionPlan, prevCol catalog.Column,
) (inputReceiver execinfra.RowReceiver, err error) {
	// Configure input.
	inputTypes, inputCols, err := inputSpecForEventDescriptor(e.currDesc, prevCol)
	if err != nil {
		return nil, err
	}

	// The row channel created below will have exactly 1 sender (this familyEvaluator).
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

// copyPrevRow copies previous row into prevRowTuple.
func (e *familyEvaluator) copyPrevRow(prev cdcevent.Row) error {
	tupleTypes := e.prevRowTuple.ResolvedType().InternalType.TupleContents
	encDatums := prev.EncDatums()
	if len(tupleTypes) != len(encDatums) {
		return errors.AssertionFailedf("cannot copy row with %d datums into tuple with %d",
			len(encDatums), len(tupleTypes))
	}

	for i, typ := range tupleTypes {
		if err := encDatums[i].EnsureDecoded(typ, &e.alloc); err != nil {
			return errors.Wrapf(err, "error decoding column [%d] as type %s", i, typ)
		}
		e.prevRowTuple.D[i] = encDatums[i].Datum
	}
	return nil
}

// setupContextForRow configures evaluation context with the provided row
// information.
func (e *familyEvaluator) setupContextForRow(
	ctx context.Context, updated cdcevent.Row, prevRow cdcevent.Row,
) error {
	e.rowEvalCtx.ctx = ctx
	e.rowEvalCtx.updatedRow = updated

	if updated.IsDeleted() {
		e.rowEvalCtx.op = eventTypeDelete
	} else {
		// Insert or update.
		if e.rowEvalCtx.withDiff {
			if prevRow.IsDeleted() || !prevRow.IsInitialized() {
				e.rowEvalCtx.op = eventTypeInsert
			} else {
				e.rowEvalCtx.op = eventTypeUpdate
			}
		} else {
			// Without diff option we can't tell insert from update; so, use upsert.
			e.rowEvalCtx.op = eventTypeUpsert
		}
	}

	return nil
}

func (e *familyEvaluator) performCleanup() {
	if e.cleanup != nil {
		e.cleanup()
		e.cleanup = nil
	}
}

func (e *familyEvaluator) closeErr() error {
	defer e.performCleanup()

	if e.errCh != nil {
		// Must be deferred since planGroup  go routine might write.
		defer func() {
			close(e.errCh)
			e.errCh = nil
		}()
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
	creationTime hlc.Timestamp
	withDiff     bool
	updatedRow   cdcevent.Row
	op           tree.Datum
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
func configSemaForCDC(semaCtx *tree.SemaContext, statementTS hlc.Timestamp) func() {
	origProps, origResolver := semaCtx.Properties, semaCtx.FunctionResolver
	semaCtx.FunctionResolver = newCDCFunctionResolver(semaCtx.FunctionResolver)
	semaCtx.Properties.Require("cdc", rejectInvalidCDCExprs)
	semaCtx.Annotations = tree.MakeAnnotations(cdcAnnotationAddr)
	semaCtx.Annotations.Set(cdcAnnotationAddr, &rowEvalContext{creationTime: statementTS})

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
