// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// CDCExpression represents CDC specific expression to plan.
// This expression is just a limited select clause;
// see sql.y for details.  The expression is normalized and validated by CDC
// (see cdc_eval library) to ensure the expression is safe to use in CDC
// context.
type CDCExpression = *tree.Select

// CDCOption is an option to configure cdc expression planning and execution.
type CDCOption interface {
	apply(cfg *cdcConfig)
}

// WithExtraColumn returns an option to add column to the
// CDC table. The caller expected to provide the value
// of this column when executing this flow.
func WithExtraColumn(c catalog.Column) CDCOption {
	return funcOpt(func(config *cdcConfig) {
		config.extraColumns = append(config.extraColumns, c)
	})
}

// CDCExpressionPlan encapsulates execution plan for evaluation of CDC expressions.
type CDCExpressionPlan struct {
	Plan         planMaybePhysical     // Underlying plan...
	PlanCtx      *PlanningCtx          // ... and plan context
	Spans        roachpb.Spans         // Set of spans for rangefeed.
	Presentation colinfo.ResultColumns // List of result columns.
}

// PlanCDCExpression plans the execution of CDCExpression.
//
// CDC expressions may contain only a single table. Because of the limited
// nature of the CDCExpression, this code assumes (and verifies) that the
// produced plan has only one instance of *scanNode.
//
// localPlanner is assumed to be an instance of planner created specifically for
// planning and execution of CDC expressions. This planner ought to be
// configured appropriately to resolve correct descriptor versions.
func PlanCDCExpression(
	ctx context.Context, localPlanner interface{}, cdcExpr CDCExpression, opts ...CDCOption,
) (cdcPlan CDCExpressionPlan, _ error) {
	p, ok := localPlanner.(*planner)
	if !ok {
		return CDCExpressionPlan{}, errors.AssertionFailedf("expected planner, found %T", localPlanner)
	}

	var cfg cdcConfig
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	p.stmt = makeStatement(statements.Statement[tree.Statement]{
		AST: cdcExpr,
		SQL: tree.AsString(cdcExpr),
	}, clusterunique.ID{}, /* queryID */
		tree.FmtFlags(queryFormattingForFingerprintsMask.Get(&p.execCfg.Settings.SV)),
	)

	p.curPlan.init(&p.stmt, &p.instrumentation)
	opc := &p.optPlanningCtx
	opc.init(p)
	opc.reset(ctx)
	opc.useCache = false
	opc.allowMemoReuse = false

	familyID, err := extractFamilyID(cdcExpr)
	if err != nil {
		return cdcPlan, err
	}

	cdcCat := &cdcOptCatalog{
		optCatalog:     opc.catalog.(*optCatalog),
		cdcConfig:      cfg,
		targetFamilyID: familyID,
		semaCtx:        &p.semaCtx,
	}

	// Reset catalog to cdc specific implementation.
	opc.catalog = cdcCat
	opc.optimizer.Init(ctx, p.EvalContext(), opc.catalog)

	memo, err := opc.buildExecMemo(ctx)
	if err != nil {
		return cdcPlan, err
	}
	if log.V(2) {
		log.Infof(ctx, "Optimized CDC expression: %s", memo)
	}

	const allowAutoCommit = false
	const disableTelemetryAndPlanGists = false
	if err := opc.runExecBuilder(
		ctx, &p.curPlan, &p.stmt, newExecFactory(ctx, p), memo, p.SemaCtx(),
		p.EvalContext(), allowAutoCommit, disableTelemetryAndPlanGists,
	); err != nil {
		return cdcPlan, err
	}

	// Walk the plan, perform sanity checks and extract information we need.
	var spans roachpb.Spans
	var presentation colinfo.ResultColumns

	if err := walkPlan(ctx, p.curPlan.main.planNode, planObserver{
		enterNode: func(ctx context.Context, nodeName string, plan planNode) (bool, error) {
			switch n := plan.(type) {
			case *scanNode:
				// Collect spans we wanted to scan.  The select statement used for this
				// plan should result in a single table scan of primary index span.
				if len(spans) > 0 {
					return false, errors.AssertionFailedf("unexpected multiple primary index scan operations")
				}
				if n.index.GetID() != n.desc.GetPrimaryIndexID() {
					return false, errors.AssertionFailedf(
						"expect scan of primary index, found scan of %d", n.index.GetID())
				}
				spans = n.spans
			case *zeroNode:
				return false, errors.Newf(
					"changefeed expression %s does not match any rows", tree.AsString(cdcExpr))
			}

			// Because the walk is top down, the top node is the node containing the
			// list of columns to return.
			if len(presentation) == 0 {
				presentation = planColumns(plan)
			}
			return true, nil
		},
	}); err != nil {
		return cdcPlan, err
	}

	if len(spans) == 0 {
		// Should have been handled by the zeroNode check above.
		return cdcPlan, errors.AssertionFailedf("expected at least 1 span to scan")
	}

	if len(presentation) == 0 {
		return cdcPlan, errors.AssertionFailedf("unable to determine result columns")
	}

	if len(p.curPlan.subqueryPlans) > 0 || len(p.curPlan.cascades) > 0 ||
		len(p.curPlan.checkPlans) > 0 || len(p.curPlan.triggers) > 0 {
		return cdcPlan, errors.AssertionFailedf("unexpected query structure")
	}

	planCtx := p.DistSQLPlanner().NewPlanningCtx(ctx, &p.extendedEvalCtx, p, p.txn, LocalDistribution)

	return CDCExpressionPlan{
		Plan:         p.curPlan.main,
		PlanCtx:      planCtx,
		Spans:        spans,
		Presentation: presentation,
	}, nil
}

// RunCDCEvaluation runs plan previously prepared by PlanCDCExpression.
// Data is pushed into this flow from source, which generates data for the
// specified table columns.
// Results of evaluations are written to the receiver.
func RunCDCEvaluation(
	ctx context.Context,
	cdcPlan CDCExpressionPlan,
	source execinfra.RowSource,
	sourceCols catalog.TableColMap,
	receiver *DistSQLReceiver,
) (err error) {
	cdcPlan.Plan.planNode, err = prepareCDCPlan(ctx, cdcPlan.Plan.planNode, source, sourceCols)
	if err != nil {
		return err
	}

	p := cdcPlan.PlanCtx.planner
	// Make sure that as soon as the local flow setup completes, and all
	// descriptors have been resolved (including leases for user defined types),
	// we can release those descriptors. If we don't, then the descriptor leases
	// acquired will be held for the DefaultDescriptorLeaseDuration (5 minutes),
	// blocking potential schema changes.
	finishedSetupFn := func(flowinfra.Flow) {
		p.Descriptors().ReleaseAll(ctx)
	}

	p.DistSQLPlanner().PlanAndRun(
		ctx, &p.extendedEvalCtx, cdcPlan.PlanCtx, p.txn, cdcPlan.Plan, receiver, finishedSetupFn,
	)
	return nil
}

func prepareCDCPlan(
	ctx context.Context, plan planNode, source execinfra.RowSource, sourceCols catalog.TableColMap,
) (planNode, error) {
	// Replace a single scan node (this was checked when constructing
	// CDCExpressionPlan) with a cdcValuesNode that reads from the source, which
	// includes specified column IDs.
	replaced := false
	v := makePlanVisitor(ctx, planObserver{
		replaceNode: func(ctx context.Context, nodeName string, plan planNode) (planNode, error) {
			scan, ok := plan.(*scanNode)
			if !ok {
				return nil, nil
			}
			replaced = true
			defer scan.Close(ctx)
			return newCDCValuesNode(scan, source, sourceCols)
		},
	})
	plan = v.visit(plan)
	if v.err != nil {
		return nil, v.err
	}
	if !replaced {
		return nil, errors.AssertionFailedf("expected to find one scan node, found none")
	}
	return plan, nil
}

// CollectPlanColumns invokes collector callback for each column accessed
// by this plan.  Collector may return false to stop iteration.
// Collector function may be invoked multiple times for the same column.
func (p CDCExpressionPlan) CollectPlanColumns(collector func(column colinfo.ResultColumn) bool) {
	v := makePlanVisitor(context.Background(), planObserver{
		enterNode: func(ctx context.Context, nodeName string, plan planNode) (bool, error) {
			cols := planColumns(plan)
			for _, c := range cols {
				stop := collector(c)
				if stop {
					return false, nil
				}
			}
			return true, nil // Continue onto the next node.
		},
	})
	_ = v.visit(p.Plan.planNode)
}

// cdcValuesNode replaces regular scanNode with cdc specific implementation
// which returns values from the execinfra.RowSource.
// The input source produces a never ending stream of encoded datums, and those
// datums must match the number of inputs (and types) expected by this flow
// (verified below).
type cdcValuesNode struct {
	zeroInputPlanNode
	source        execinfra.RowSource
	datumRow      []tree.Datum
	colOrd        []int
	resultColumns []colinfo.ResultColumn
	alloc         tree.DatumAlloc
}

var _ planNode = (*cdcValuesNode)(nil)

func newCDCValuesNode(
	scan *scanNode, source execinfra.RowSource, sourceCols catalog.TableColMap,
) (planNode, error) {
	v := cdcValuesNode{
		source:        source,
		datumRow:      make([]tree.Datum, len(scan.resultColumns)),
		resultColumns: scan.resultColumns,
		colOrd:        make([]int, len(scan.cols)),
	}

	for i, c := range scan.cols {
		sourceOrd, ok := sourceCols.Get(c.GetID())
		if !ok {
			return nil, errors.Newf("source does not contain column %s (id %d)", c.GetName(), c.GetID())
		}
		v.colOrd[i] = sourceOrd
	}

	return &v, nil
}

// startExec implements planNode.
func (n *cdcValuesNode) startExec(params runParams) error {
	n.source.Start(params.ctx)
	return nil
}

// Next implements planNode.
func (n *cdcValuesNode) Next(params runParams) (bool, error) {
	row, meta := n.source.Next()
	if meta != nil {
		return false, errors.AssertionFailedf("unexpected producer meta returned")
	}
	if row == nil {
		return false, nil
	}

	typs := n.source.OutputTypes()
	for i, ord := range n.colOrd {
		if err := row[ord].EnsureDecoded(typs[ord], &n.alloc); err != nil {
			return false, err
		}
		n.datumRow[i] = row[ord].Datum
	}
	return true, nil
}

// Values implements planNode.
func (n *cdcValuesNode) Values() tree.Datums {
	return n.datumRow
}

// Close implements planNode.
func (n *cdcValuesNode) Close(ctx context.Context) {
	// Note that we do not call ConsumerClosed on it since it is not the
	// responsibility of this node (the responsibility belongs to
	// the caller -- cdc evaluation planNodeToRowSource).
	// This node is used in the following tree:
	// DistSQLReceiver <- (arbitrary DistSQL processors) <- planNodeToRowSource <- cdcValuesNode <- RowChannel
	// RowChannel is added as the "input to drain" by planNodeToRowSource (in SetInput),
	// so planNodeToRowSource will call ConsumerDone or ConsumerClosed
	// (depending on why the flow is being shutdown).
}

type cdcOptCatalog struct {
	*optCatalog
	cdcConfig
	targetFamilyID catid.FamilyID
	semaCtx        *tree.SemaContext
}

var _ cat.Catalog = (*cdcOptCatalog)(nil)

// extractFamilyID extracts family ID hint from CDCExpression.
func extractFamilyID(stmt CDCExpression) (catid.FamilyID, error) {
	sc, ok := stmt.Select.(*tree.SelectClause)
	if !ok {
		return 0, errors.AssertionFailedf("unexpected expression type %T", stmt.Select)
	}
	if t, ok := sc.From.Tables[0].(*tree.AliasedTableExpr); ok {
		if t.IndexFlags != nil && t.IndexFlags.FamilyID != nil {
			return *t.IndexFlags.FamilyID, nil
		}
	}
	return 0, nil
}

// ResolveDataSource implements cat.Catalog interface.
// We provide custom implementation to ensure that we return data source for
// primary index.
func (c *cdcOptCatalog) ResolveDataSource(
	ctx context.Context, flags cat.Flags, name *cat.DataSourceName,
) (cat.DataSource, cat.DataSourceName, error) {
	lflags := tree.ObjectLookupFlags{
		Required:             true,
		DesiredObjectKind:    tree.TableObject,
		DesiredTableDescKind: tree.ResolveRequireTableDesc,
	}
	_, desc, err := resolver.ResolveExistingTableObject(ctx, c.planner, name, lflags)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}

	// We block tables with row-level security enabled because they can inject
	// filters into the select op. This conflicts with the CDC expression's
	// expectation that the SELECT contains no filters — that all filters are
	// converted into a projection for the __crdb_filter column (see
	// predicateAsProjection). Since the RLS filters aren’t included in
	// __crdb_filter, we block this functionality until that work is complete.
	if desc.IsRowLevelSecurityEnabled() {
		return nil, cat.DataSourceName{}, unimplemented.NewWithIssuef(
			142171,
			"CDC queries are not supported on tables with row-level security enabled")
	}

	ds, err := c.newCDCDataSource(ctx, desc, c.targetFamilyID)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}
	return ds, *name, nil
}

// ResolveDataSourceByID implements cat.Catalog interface.
// We provide custom implementation to ensure that we return data source for
// primary index span.
func (c *cdcOptCatalog) ResolveDataSourceByID(
	ctx context.Context, flags cat.Flags, id cat.StableID,
) (cat.DataSource, bool, error) {
	desc, err := c.planner.LookupTableByID(ctx, descpb.ID(id))
	if err != nil {
		return nil, false, err
	}

	ds, err := c.newCDCDataSource(ctx, desc, c.targetFamilyID)
	if err != nil {
		return nil, false, err
	}
	return ds, false, nil
}

// ResolveFunction implements cat.Catalog interface.
// We provide custom implementation to resolve CDC specific functions.
func (c *cdcOptCatalog) ResolveFunction(
	ctx context.Context, fnName tree.UnresolvedRoutineName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	if c.semaCtx != nil && c.semaCtx.FunctionResolver != nil {
		fnDef, err := c.semaCtx.FunctionResolver.ResolveFunction(ctx, fnName, path)
		if err != nil {
			return nil, err
		}
		return fnDef, nil
	}
	return c.optCatalog.ResolveFunction(ctx, fnName, path)
}

// newCDCDataSource builds an optTable for the target cdc table and family.
func (c *cdcOptCatalog) newCDCDataSource(
	ctx context.Context, original catalog.TableDescriptor, familyID catid.FamilyID,
) (cat.DataSource, error) {
	d, err := newFamilyTableDescriptor(original, familyID, c.extraColumns)
	if err != nil {
		return nil, err
	}
	return newOptTable(ctx, d, c.codec(), nil /* stats */, emptyZoneConfig)
}

// familyTableDescriptor wraps underlying catalog.TableDescriptor,
// but restricts access to a single column family.
type familyTableDescriptor struct {
	catalog.TableDescriptor
	includeSet catalog.TableColSet
	extraCols  []catalog.Column
}

func newFamilyTableDescriptor(
	original catalog.TableDescriptor, familyID catid.FamilyID, extraCols []catalog.Column,
) (catalog.TableDescriptor, error) {
	// Build the set of columns in the family, along with the primary
	// key columns.
	fam, err := catalog.MustFindFamilyByID(original, familyID)
	if err != nil {
		return nil, err
	}

	includeSet := original.GetPrimaryIndex().CollectKeyColumnIDs()
	for _, id := range fam.ColumnIDs {
		includeSet.Add(id)
	}

	// Add system columns -- those are always available.
	for _, col := range colinfo.AllSystemColumnDescs {
		includeSet.Add(col.ID)
	}

	return &familyTableDescriptor{
		TableDescriptor: original,
		includeSet:      includeSet,
		extraCols:       extraCols,
	}, nil
}

// DeletableNonPrimaryIndexes implements catalog.TableDescriptor interface.
// CDC currently supports primary index only.
// TODO(yevgeniy): We should be able to use secondary indexes provided
// the CDC expression access only the columns available in that secondary index.
func (d *familyTableDescriptor) DeletableNonPrimaryIndexes() []catalog.Index {
	return nil
}

// ActiveIndexes implements catalog.TableDescriptor.
// Only primary index supported for now.
func (d *familyTableDescriptor) ActiveIndexes() []catalog.Index {
	return d.TableDescriptor.ActiveIndexes()[:1]
}

// DeletableColumns implements catalog.TableDescriptor interface.
// This implementation filters out underlying descriptor DeletableColumns to
// only include columns referenced by the target column family.
func (d *familyTableDescriptor) DeletableColumns() []catalog.Column {
	return d.familyColumns(d.TableDescriptor.DeletableColumns())
}

// AllColumns implements catalog.TableDescriptor interface.
// This implementation filters out underlying descriptor AllColumns to
// only include columns referenced by the target column family.
func (d *familyTableDescriptor) AllColumns() []catalog.Column {
	return d.familyColumns(d.TableDescriptor.AllColumns())
}

// VisibleColumns implements catalog.TableDescriptor interface.
// This implementation filters out underlying descriptor AllColumns to
// only include columns referenced by the target column family.
func (d *familyTableDescriptor) VisibleColumns() []catalog.Column {
	return d.familyColumns(d.TableDescriptor.VisibleColumns())
}

// EnforcedUniqueConstraintsWithoutIndex implements catalog.TableDescriptor interface.
// This implementation filters out constraints that reference columns outside of
// target column family.
func (d *familyTableDescriptor) EnforcedUniqueConstraintsWithoutIndex() []catalog.UniqueWithoutIndexConstraint {
	constraints := d.TableDescriptor.EnforcedUniqueConstraintsWithoutIndex()
	filtered := make([]catalog.UniqueWithoutIndexConstraint, 0, len(constraints))
	for _, c := range constraints {
		if c.CollectKeyColumnIDs().SubsetOf(d.includeSet) {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

// FindColumnWithID implements catalog.TableDescriptor and provides
// access to extra CDC columns.
func (d *familyTableDescriptor) FindColumnWithID(id descpb.ColumnID) (catalog.Column, error) {
	c := catalog.FindColumnByID(d.TableDescriptor, id)
	if c != nil {
		return c, nil
	}
	for _, extra := range d.extraCols {
		if extra.GetID() == id {
			return c, nil
		}
	}
	return nil, pgerror.Newf(pgcode.UndefinedColumn, "column-id \"%d\" does not exist", id)
}

// EnforcedCheckConstraints implements catalog.TableDescriptor interface.
// This implementation filters out constraints that reference columns outside of
// target column family.
func (d *familyTableDescriptor) EnforcedCheckConstraints() []catalog.CheckConstraint {
	constraints := d.TableDescriptor.EnforcedCheckConstraints()
	filtered := make([]catalog.CheckConstraint, 0, len(constraints))
	for _, c := range constraints {
		if c.CollectReferencedColumnIDs().SubsetOf(d.includeSet) {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

// familyColumns returns column list adopted for targeted column family.
func (d *familyTableDescriptor) familyColumns(cols []catalog.Column) []catalog.Column {
	filtered := make([]catalog.Column, 0, len(cols)+1)
	for _, col := range cols {
		if d.includeSet.Contains(col.GetID()) {
			filtered = append(filtered, &remappedOrdinalColumn{Column: col, ord: len(filtered)})
		}
	}
	for _, col := range d.extraCols {
		filtered = append(filtered, &remappedOrdinalColumn{Column: col, ord: len(filtered)})
	}
	return filtered
}

// remappedOrdinalColumn wraps underlying catalog.Column
// but changes its ordinal position.
type remappedOrdinalColumn struct {
	catalog.Column
	ord int
}

// Ordinal implements catalog.Column interface.
func (c *remappedOrdinalColumn) Ordinal() int {
	return c.ord
}

type cdcConfig struct {
	extraColumns []catalog.Column
}

type funcOpt func(config *cdcConfig)

func (fn funcOpt) apply(config *cdcConfig) {
	fn(config)
}
