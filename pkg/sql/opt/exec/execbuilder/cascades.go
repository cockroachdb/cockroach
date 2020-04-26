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

type cascadeBuilder struct {
	b              *Builder
	mutationBuffer exec.BufferNode
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
	withExpr := b.findBuiltWithExpr(mutationWithID)
	if withExpr == nil {
		return nil, errors.AssertionFailedf("cannot find mutation input withExpr")
	}
	cb := &cascadeBuilder{
		b:                  b,
		mutationBuffer:     withExpr.bufferNode,
		mutationBufferCols: withExpr.outputCols,
	}

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

// setupCascade populates an exec.Cascade struct for the given cascade.
func (cb *cascadeBuilder) setupCascade(cascade *memo.FKCascade) exec.Cascade {
	return exec.Cascade{
		FKName: cascade.FKName,
		Buffer: cb.mutationBuffer,
		PlanFn: func(
			ctx context.Context,
			semaCtx *tree.SemaContext,
			evalCtx *tree.EvalContext,
			bufferRef exec.BufferNode,
			numBufferedRows int,
		) (exec.Plan, error) {
			return cb.planCascade(ctx, semaCtx, evalCtx, cascade, bufferRef, numBufferedRows)
		},
	}
}

// planCascade is used to plan a cascade query. It is NOT run while
// planning the query; it is run by the execution logic (through
// exec.Cascade.PlanFn) after the main query was executed.
func (cb *cascadeBuilder) planCascade(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	cascade *memo.FKCascade,
	bufferRef exec.BufferNode,
	numBufferedRows int,
) (exec.Plan, error) {
	// We plan the cascade in a brand new memo.
	var o xform.Optimizer
	o.Init(evalCtx, cb.b.catalog)
	factory := o.Factory()
	md := factory.Metadata()

	// Set up metadata for the buffer columns.

	// withColRemap is the mapping between the With column IDs in the original
	// memo and the corresponding column IDs in the new memo.
	var withColRemap opt.ColMap
	// bufferColMap is the mapping between the column IDs in the new memo and
	// the column ordinal in the buffer node.
	var bufferColMap opt.ColMap
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

	// Invoke the CascadeBuilder to build the cascade.
	relExpr, err := cascade.Builder.Build(
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

	o.Memo().SetRoot(relExpr, &physical.Required{})

	optimizedExpr, err := o.Optimize()
	if err != nil {
		return nil, errors.Wrap(err, "while optimizing cascade expression")
	}

	eb := New(cb.b.factory, factory.Memo(), cb.b.catalog, optimizedExpr, evalCtx)
	// TODO(radu): we could allow autocommit for the last cascade (if there are no
	// checks to run).
	eb.DisallowAutoCommit()
	// Set up the With binding.
	eb.addBuiltWithExpr(cascadeInputWithID, bufferColMap, bufferRef)
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
