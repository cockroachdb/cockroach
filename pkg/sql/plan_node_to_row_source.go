// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type metadataForwarder interface {
	forwardMetadata(metadata *execinfrapb.ProducerMetadata)
}

type planNodeToRowSource struct {
	execinfra.ProcessorBase

	input execinfra.RowSource

	fastPath bool

	node        planNode
	params      runParams
	outputTypes []*types.T

	firstNotWrapped planNode

	// run time state machine values
	row rowenc.EncDatumRow
}

var _ execinfra.OpNode = &planNodeToRowSource{}

func makePlanNodeToRowSource(
	source planNode, params runParams, fastPath bool,
) (*planNodeToRowSource, error) {
	var typs []*types.T
	if fastPath {
		// If our node is a "fast path node", it means that we're set up to
		// just return a row count meaning we'll output a single row with a
		// single INT column.
		typs = []*types.T{types.Int}
	} else {
		typs = getTypesFromResultColumns(planColumns(source))
	}
	row := make(rowenc.EncDatumRow, len(typs))

	return &planNodeToRowSource{
		node:        source,
		params:      params,
		outputTypes: typs,
		row:         row,
		fastPath:    fastPath,
	}, nil
}

var _ execinfra.LocalProcessor = &planNodeToRowSource{}

// MustBeStreaming implements the execinfra.Processor interface.
func (p *planNodeToRowSource) MustBeStreaming() bool {
	// hookFnNode is special because it might be blocked forever if we decide to
	// buffer its output.
	_, isHookFnNode := p.node.(*hookFnNode)
	return isHookFnNode
}

// InitWithOutput implements the LocalProcessor interface.
func (p *planNodeToRowSource) InitWithOutput(
	flowCtx *execinfra.FlowCtx, post *execinfrapb.PostProcessSpec, output execinfra.RowReceiver,
) error {
	return p.InitWithEvalCtx(
		p,
		post,
		p.outputTypes,
		flowCtx,
		p.params.EvalContext(),
		0, /* processorID */
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{},
	)
}

// SetInput implements the LocalProcessor interface.
// input is the first upstream RowSource. When we're done executing, we need to
// drain this row source of its metadata in case the planNode tree we're
// wrapping returned an error, since planNodes don't know how to drain trailing
// metadata.
func (p *planNodeToRowSource) SetInput(ctx context.Context, input execinfra.RowSource) error {
	if p.firstNotWrapped == nil {
		// Short-circuit if we never set firstNotWrapped - indicating this planNode
		// tree had no DistSQL-plannable subtrees.
		return nil
	}
	p.input = input
	p.AddInputToDrain(input)
	// Search the plan we're wrapping for firstNotWrapped, which is the planNode
	// that DistSQL planning resumed in. Replace that planNode with input,
	// wrapped as a planNode.
	return walkPlan(ctx, p.node, planObserver{
		replaceNode: func(ctx context.Context, nodeName string, plan planNode) (planNode, error) {
			if plan == p.firstNotWrapped {
				return makeRowSourceToPlanNode(input, p, planColumns(p.firstNotWrapped), p.firstNotWrapped), nil
			}
			return nil, nil
		},
	})
}

func (p *planNodeToRowSource) Start(ctx context.Context) {
	ctx = p.StartInternalNoSpan(ctx)
	p.params.ctx = ctx
	// This starts all of the nodes below this node.
	if err := startExec(p.params, p.node); err != nil {
		p.MoveToDraining(err)
	}
}

func (p *planNodeToRowSource) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if p.State == execinfra.StateRunning && p.fastPath {
		var count int
		// If our node is a "fast path node", it means that we're set up to just
		// return a row count. So trigger the fast path and return the row count as
		// a row with a single column.
		fastPath, ok := p.node.(planNodeFastPath)

		if ok {
			var res bool
			if count, res = fastPath.FastPathResults(); res {
				if p.params.extendedEvalCtx.Tracing.Enabled() {
					log.VEvent(p.params.ctx, 2, "fast path completed")
				}
			} else {
				// Fall back to counting the rows.
				count = 0
				ok = false
			}
		}

		if !ok {
			// If we have no fast path to trigger, fall back to counting the rows
			// by Nexting our source until exhaustion.
			next, err := p.node.Next(p.params)
			for ; next; next, err = p.node.Next(p.params) {
				count++
			}
			if err != nil {
				p.MoveToDraining(err)
				return nil, p.DrainHelper()
			}
		}
		p.MoveToDraining(nil /* err */)
		// Return the row count the only way we can: as a single-column row with
		// the count inside.
		return rowenc.EncDatumRow{rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(count))}}, nil
	}

	for p.State == execinfra.StateRunning {
		valid, err := p.node.Next(p.params)
		if err != nil || !valid {
			p.MoveToDraining(err)
			return nil, p.DrainHelper()
		}

		for i, datum := range p.node.Values() {
			if datum != nil {
				p.row[i] = rowenc.DatumToEncDatum(p.outputTypes[i], datum)
			}
		}
		// ProcessRow here is required to deal with projections, which won't be
		// pushed into the wrapped plan.
		if outRow := p.ProcessRowHelper(p.row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, p.DrainHelper()
}

// forwardMetadata will be called by any upstream rowSourceToPlanNode processors
// that need to forward metadata to the end of the flow. They can't pass
// metadata through local processors, so they instead add the metadata to our
// trailing metadata and expect us to forward it further.
func (p *planNodeToRowSource) forwardMetadata(metadata *execinfrapb.ProducerMetadata) {
	p.ProcessorBase.AppendTrailingMeta(*metadata)
}

// ChildCount is part of the execinfra.OpNode interface.
func (p *planNodeToRowSource) ChildCount(verbose bool) int {
	if _, ok := p.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (p *planNodeToRowSource) Child(nth int, verbose bool) execinfra.OpNode {
	switch nth {
	case 0:
		if n, ok := p.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to planNodeToRowSource is not an execinfra.OpNode")
	default:
		panic(errors.AssertionFailedf("invalid index %d", nth))
	}
}
