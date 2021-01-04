// Copyright 2020 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// cascadeBuilder is a helper that fills in exec.Cascade metadata; it also
// contains the implementation of exec.Cascade.PlanFn.
//
// We walk through a simple example of a cascade to illustrate the flow around
// executing cascades:
//
//   CREATE TABLE parent (p INT PRIMARY KEY);
//   CREATE TABLE child (
//     c INT PRIMARY KEY,
//     p INT NOT NULL REFERENCES parent(p) ON DELETE CASCADE
//   );
//
//   DELETE FROM parent WHERE p > 1;
//
// The optimizer expression for this query is:
//
//   delete parent
//    ├── columns: <none>
//    ├── fetch columns: p:2
//    ├── input binding: &1
//    ├── cascades
//    │    └── fk_p_ref_parent
//    └── select
//         ├── columns: p:2!null
//         ├── scan parent
//         │    └── columns: p:2!null
//         └── filters
//              └── p:2 > 1
//
// Note that at this time, the cascading query in the child table was not built.
// The expression above does contain a reference to a memo.CascadeBuilder which
// will be invoked to build the query at a later time.
//
// When execbuilding the query above, a buffer node is constructed for the
// mutation input (binding &1 above) and a cascadeBuilder object is constructed
// for the cascade.
//
// The setupCascade method is called to fill in an exec.Cascade which is passed
// to ConstructPlan. Note that we still did not build the cascading query; all
// we did was provide some plumbing and an entry point (through PlanFn) for that
// to happen later.
//
// The plan is constructed and processed by the execution engine. After the
// plans for the subqueries and the main query are executed, the cascades are
// processed (in a queue). At this time the PlanFn method is called and the
// following happens:
//
//  1. We set up a new empty memo and add metadata for the columns of the
//     buffer node (binding &1).
//
//  2. We invoke the memo.CascadeBuilder to optbuild the cascading query. At this
//     point, the new memo will contain the following expression:
//
//      delete child
//       ├── columns: <none>
//       ├── fetch columns: c:4 child.p:5
//       └── semi-join (hash)
//            ├── columns: c:4!null child.p:5!null
//            ├── scan child
//            │    └── columns: c:4!null child.p:5!null
//            ├── with-scan &1
//            │    ├── columns: p:6!null
//            │    └── mapping:
//            │         └──  parent.p:1 => p:6
//            └── filters
//                  └── child.p:5 = p:6
//
//    Notes:
//     - normally, a WithScan can only refer to an ancestor mutation or With
//       operator. In this case we are creating a reference "out of the void".
//       This works just fine; we can consider adding a special dummy root
//       operator but so far it hasn't been necessary;
//     - the binding &1 column ID has changed: it used to be 2, it is now 1.
//       This is because we are starting with a fresh memo. We need to take into
//       account this remapping when referring to the foreign key columns.
//
//  3. We optimize the newly built expression.
//
//  4. We execbuild the optimizer expression. We have to be careful to set up
//     the "With" reference before starting.
//
// After PlanFn is called, the resulting plan is executed. Note that this plan
// could itself have more exec.Cascades; these are queued and handled in the
// same way.
//
type cascadeBuilder struct {
	b              *Builder
	mutationBuffer exec.Node
	// mutationBufferCols maps With column IDs from the original memo to buffer
	// node column ordinals; see builtWithExpr.outputCols.
	mutationBufferCols opt.ColMap

	// colMeta remembers the metadata of the With columns from the original memo.
	colMeta []opt.ColumnMeta
}

// cascadeInputWithID is a special WithID that we use to refer to a cascade
// input. It should be large enough to never clash with "regular" WithIDs (which
// are generated sequentially).
const cascadeInputWithID opt.WithID = 1000000

func makeCascadeBuilder(b *Builder, mutationWithID opt.WithID) (*cascadeBuilder, error) {
	cb := &cascadeBuilder{b: b}
	if mutationWithID == 0 {
		// Cascade does not require the buffered input.
		return cb, nil
	}

	withExpr := b.findBuiltWithExpr(mutationWithID)
	if withExpr == nil {
		return nil, errors.AssertionFailedf("cannot find mutation input withExpr")
	}

	cb.mutationBuffer = withExpr.bufferNode
	cb.mutationBufferCols = withExpr.outputCols

	// Remember the column metadata, as we will need to recreate it in the new
	// memo.
	md := b.mem.Metadata()
	cb.colMeta = make([]opt.ColumnMeta, 0, cb.mutationBufferCols.Len())
	cb.mutationBufferCols.ForEach(func(key, val int) {
		id := opt.ColumnID(key)
		cb.colMeta = append(cb.colMeta, *md.ColumnMeta(id))
	})

	return cb, nil
}

// setupCascade fills in an exec.Cascade struct for the given cascade.
func (cb *cascadeBuilder) setupCascade(cascade *memo.FKCascade) exec.Cascade {
	return exec.Cascade{
		FKName: cascade.FKName,
		Buffer: cb.mutationBuffer,
		PlanFn: func(
			ctx context.Context,
			semaCtx *tree.SemaContext,
			evalCtx *tree.EvalContext,
			execFactory exec.Factory,
			bufferRef exec.Node,
			numBufferedRows int,
			allowAutoCommit bool,
		) (exec.Plan, error) {
			return cb.planCascade(
				ctx, semaCtx, evalCtx, execFactory, cascade, bufferRef, numBufferedRows, allowAutoCommit,
			)
		},
	}
}

// planCascade is used to plan a cascade query. It is NOT run while
// planning the query; it is run by the execution logic (through
// exec.Cascade.PlanFn) after the main query was executed.
//
// See the comment for cascadeBuilder for a detailed explanation of the
// process.
func (cb *cascadeBuilder) planCascade(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	execFactory exec.Factory,
	cascade *memo.FKCascade,
	bufferRef exec.Node,
	numBufferedRows int,
	allowAutoCommit bool,
) (exec.Plan, error) {
	// 1. Set up a brand new memo in which to plan the cascading query.
	var o xform.Optimizer
	o.Init(evalCtx, cb.b.catalog)
	factory := o.Factory()
	md := factory.Metadata()

	// 2. Invoke the memo.CascadeBuilder to build the cascade.
	var relExpr memo.RelExpr
	// bufferColMap is the mapping between the column IDs in the new memo and
	// the column ordinal in the buffer node.
	var bufferColMap opt.ColMap
	if bufferRef == nil {
		// No input buffering.
		var err error
		relExpr, err = cascade.Builder.Build(
			ctx,
			semaCtx,
			evalCtx,
			cb.b.catalog,
			factory,
			0,   /* binding */
			nil, /* bindingProps */
			nil, /* oldValues */
			nil, /* newValues */
		)
		if err != nil {
			return nil, errors.Wrap(err, "while building cascade expression")
		}
	} else {
		// Set up metadata for the buffer columns.

		// withColRemap is the mapping between the With column IDs in the original
		// memo and the corresponding column IDs in the new memo.
		var withColRemap opt.ColMap
		var withCols opt.ColSet
		for i := range cb.colMeta {
			id := md.AddColumn(cb.colMeta[i].Alias, cb.colMeta[i].Type)
			withCols.Add(id)
			ordinal, _ := cb.mutationBufferCols.Get(int(cb.colMeta[i].MetaID))
			bufferColMap.Set(int(id), ordinal)
			withColRemap.Set(int(cb.colMeta[i].MetaID), int(id))
		}

		// Create relational properties for the special WithID input.
		// TODO(radu): save some more information from the original binding props
		// (like not-null columns, FDs) and remap them to the new columns.
		var bindingProps props.Relational
		bindingProps.Populated = true
		bindingProps.OutputCols = withCols
		bindingProps.Cardinality = props.Cardinality{
			Min: uint32(numBufferedRows),
			Max: uint32(numBufferedRows),
		}
		bindingProps.Stats = props.Statistics{
			Available: true,
			RowCount:  float64(numBufferedRows),
		}

		// Remap the cascade columns.
		oldVals, err := remapColumns(cascade.OldValues, withColRemap)
		if err != nil {
			return nil, err
		}
		newVals, err := remapColumns(cascade.NewValues, withColRemap)
		if err != nil {
			return nil, err
		}

		relExpr, err = cascade.Builder.Build(
			ctx,
			semaCtx,
			evalCtx,
			cb.b.catalog,
			factory,
			cascadeInputWithID,
			&bindingProps,
			oldVals,
			newVals,
		)
		if err != nil {
			return nil, errors.Wrap(err, "while building cascade expression")
		}
	}

	o.Memo().SetRoot(relExpr, &physical.Required{})

	// 3. Assign placeholders if they exist.
	if factory.Memo().HasPlaceholders() {
		// Construct a new memo that is copied from the memo created above, but with
		// placeholders assigned. Stable operators can be constant-folded at this
		// time.
		preparedMemo := o.DetachMemo()
		factory.FoldingControl().AllowStableFolds()
		if err := factory.AssignPlaceholders(preparedMemo); err != nil {
			return nil, errors.Wrap(err, "while assigning placeholders in cascade expression")
		}
	}

	// 4. Optimize the expression.
	optimizedExpr, err := o.Optimize()
	if err != nil {
		return nil, errors.Wrap(err, "while optimizing cascade expression")
	}

	// 5. Execbuild the optimized expression.
	eb := New(execFactory, factory.Memo(), cb.b.catalog, optimizedExpr, evalCtx, allowAutoCommit)
	if bufferRef != nil {
		// Set up the With binding.
		eb.addBuiltWithExpr(cascadeInputWithID, bufferColMap, bufferRef)
	}
	plan, err := eb.Build()
	if err != nil {
		return nil, errors.Wrap(err, "while building cascade plan")
	}
	return plan, nil
}

// Remap columns according to a ColMap.
func remapColumns(cols opt.ColList, m opt.ColMap) (opt.ColList, error) {
	res := make(opt.ColList, len(cols))
	for i := range cols {
		val, ok := m.Get(int(cols[i]))
		if !ok {
			return nil, errors.AssertionFailedf("column %d not in mapping %s\n", cols[i], m.String())
		}
		res[i] = opt.ColumnID(val)
	}
	return res, nil
}
