// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// CDCExpression represents CDC specific expression to plan.
// This expression is just a limited select clause;
// see sql.y for details.  The expression is normalized and validated by CDC
// (see cdc_eval library) to ensure the expression is safe to use in CDC
// context.
type CDCExpression = *tree.Select

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
	ctx context.Context, localPlanner interface{}, cdcExpr CDCExpression,
) (cdcPlan CDCExpressionPlan, _ error) {
	p, ok := localPlanner.(*planner)
	if !ok {
		return CDCExpressionPlan{}, errors.AssertionFailedf("expected planner, found %T", localPlanner)
	}

	p.stmt = makeStatement(parser.Statement{
		AST: cdcExpr,
		SQL: tree.AsString(cdcExpr),
	}, clusterunique.ID{} /* queryID */)

	p.curPlan.init(&p.stmt, &p.instrumentation)
	p.optPlanningCtx.init(p)
	opc := &p.optPlanningCtx
	opc.reset(ctx)

	cdcCat := &cdcOptCatalog{
		optCatalog: opc.catalog.(*optCatalog),
	}
	opc.catalog = cdcCat

	// We could use opc.buildExecMemo; alas, it has too much logic we don't
	// need, and, it also allows stable fold -- something we don't want to do.
	// So, just build memo ourselves.
	f := opc.optimizer.Factory()
	f.FoldingControl().DisallowStableFolds()
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), opc.catalog, f, opc.p.stmt.AST)
	if err := bld.Build(); err != nil {
		return cdcPlan, err
	}

	oe, err := opc.optimizer.Optimize()
	if err != nil {
		return cdcPlan, err
	}
	if log.V(2) {
		log.Infof(ctx, "Optimized CDC expression: %s", oe.String())
	}
	execMemo := f.Memo()

	const allowAutoCommit = false
	if err := opc.runExecBuilder(
		ctx, &p.curPlan, &p.stmt, newExecFactory(ctx, p), execMemo, p.EvalContext(), allowAutoCommit,
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

	if len(p.curPlan.subqueryPlans) > 0 || len(p.curPlan.cascades) > 0 || len(p.curPlan.checkPlans) > 0 {
		return cdcPlan, errors.AssertionFailedf("unexpected query structure")
	}

	planCtx := p.DistSQLPlanner().NewPlanningCtx(ctx, &p.extendedEvalCtx, p, p.txn, DistributionTypeNone)

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

	// Execute.
	p := cdcPlan.PlanCtx.planner
	p.DistSQLPlanner().PlanAndRun(
		ctx, &p.extendedEvalCtx, cdcPlan.PlanCtx, p.txn, cdcPlan.Plan, receiver)
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

// cdcValuesNode replaces regular scanNode with cdc specific implementation
// which returns values from the execinfra.RowSource.
// The input source produces a never ending stream of encoded datums, and those
// datums must match the number of inputs (and types) expected by this flow
// (verified below).
type cdcValuesNode struct {
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
	n.source.ConsumerDone()
}

type cdcOptCatalog struct {
	*optCatalog
}

var _ cat.Catalog = (*cdcOptCatalog)(nil)

// ResolveDataSource implements cat.Catalog interface.
// We provide custom implementation to ensure that we return data source for
// primary index.
func (c *cdcOptCatalog) ResolveDataSource(
	ctx context.Context, flags cat.Flags, name *cat.DataSourceName,
) (cat.DataSource, cat.DataSourceName, error) {
	lflags := tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveRequireTableDesc)
	_, desc, err := resolver.ResolveExistingTableObject(ctx, c.planner, name, lflags)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}

	ds, err := c.newCDCDataSource(desc)
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
	ds, err := c.newCDCDataSource(desc)
	if err != nil {
		return nil, false, err
	}
	return ds, false, nil
}

// newCDCDataSource builds an optTable for the target cdc table.
// The descriptor presented to the optimizer hides all but the primary index.
// TODO(yevgeniy): We should be able to use secondary indexes provided
// the CDC expression access only the columns available in that secondary index.
func (c *cdcOptCatalog) newCDCDataSource(original catalog.TableDescriptor) (cat.DataSource, error) {
	// Build descriptor with all indexes other than primary removed.
	desc := protoutil.Clone(original.TableDesc()).(*descpb.TableDescriptor)
	desc.Indexes = desc.Indexes[:0]
	updated := tabledesc.NewBuilder(desc).BuildImmutableTable()
	return newOptTable(updated, c.codec(), nil /* stats */, emptyZoneConfig)
}
