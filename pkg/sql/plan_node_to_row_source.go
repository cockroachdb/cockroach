// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type metadataForwarder interface {
	forwardMetadata(metadata *distsqlrun.ProducerMetadata)
}

type planNodeToRowSource struct {
	distsqlrun.ProcessorBase

	started bool

	fastPath bool

	node        planNode
	params      runParams
	outputTypes []sqlbase.ColumnType

	firstNotWrapped planNode

	// run time state machine values
	row sqlbase.EncDatumRow
}

func makePlanNodeToRowSource(
	source planNode, params runParams, fastPath bool,
) (*planNodeToRowSource, error) {
	nodeColumns := planColumns(source)

	types := make([]sqlbase.ColumnType, len(nodeColumns))
	for i := range nodeColumns {
		colTyp, err := sqlbase.DatumTypeToColumnType(nodeColumns[i].Typ)
		if err != nil {
			return nil, err
		}
		types[i] = colTyp
	}
	row := make(sqlbase.EncDatumRow, len(nodeColumns))

	return &planNodeToRowSource{
		node:        source,
		params:      params,
		outputTypes: types,
		row:         row,
		fastPath:    fastPath,
	}, nil
}

var _ distsqlrun.LocalProcessor = &planNodeToRowSource{}

// InitWithOutput implements the LocalProcessor interface.
func (p *planNodeToRowSource) InitWithOutput(
	post *distsqlrun.PostProcessSpec, output distsqlrun.RowReceiver,
) error {
	return p.InitWithEvalCtx(
		p,
		post,
		p.outputTypes,
		nil, /* flowCtx */
		p.params.EvalContext(),
		0, /* processorID */
		output,
		nil, /* memMonitor */
		distsqlrun.ProcStateOpts{},
	)
}

// SetInput implements the LocalProcessor interface.
// input is the first upstream RowSource. When we're done executing, we need to
// drain this row source of its metadata in case the planNode tree we're
// wrapping returned an error, since planNodes don't know how to drain trailing
// metadata.
func (p *planNodeToRowSource) SetInput(ctx context.Context, input distsqlrun.RowSource) error {
	if p.firstNotWrapped == nil {
		// Short-circuit if we never set firstNotWrapped - indicating this planNode
		// tree had no DistSQL-plannable subtrees.
		return nil
	}
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

func (p *planNodeToRowSource) Start(ctx context.Context) context.Context {
	// We do not call p.StartInternal to avoid creating a span. Only the context
	// needs to be set.
	p.Ctx = ctx
	p.params.ctx = ctx
	if !p.started {
		p.started = true
		// This starts all of the nodes below this node.
		if err := startExec(p.params, p.node); err != nil {
			p.MoveToDraining(err)
			return ctx
		}
	}
	return ctx
}

func (p *planNodeToRowSource) InternalClose() {
	if p.ProcessorBase.InternalClose() {
		p.started = true
	}
}

func (p *planNodeToRowSource) startExec(_ runParams) error {
	// If we're getting startExec'd, it means we're running in local mode - so we
	// mark ourselves already started, since local mode will have taken care of
	// starting the child nodes of this node.
	p.started = true
	return nil
}

func (p *planNodeToRowSource) Next() (sqlbase.EncDatumRow, *distsqlrun.ProducerMetadata) {
	if p.State == distsqlrun.StateRunning && p.fastPath {
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
				// If we're tracking memory, clear the previous row's memory account.
				if p.params.extendedEvalCtx.ActiveMemAcc != nil {
					p.params.extendedEvalCtx.ActiveMemAcc.Clear(p.params.ctx)
				}
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
		return sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(count))}}, nil
	}

	for p.State == distsqlrun.StateRunning {
		valid, err := p.node.Next(p.params)
		if err != nil || !valid {
			p.MoveToDraining(err)
			return nil, p.DrainHelper()
		}

		for i, datum := range p.node.Values() {
			if datum != nil {
				p.row[i] = sqlbase.DatumToEncDatum(p.outputTypes[i], datum)
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

func (p *planNodeToRowSource) ConsumerDone() {
	p.MoveToDraining(nil /* err */)
}

func (p *planNodeToRowSource) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	p.InternalClose()
}

// forwardMetadata will be called by any upstream rowSourceToPlanNode processors
// that need to forward metadata to the end of the flow. They can't pass
// metadata through local processors, so they instead add the metadata to our
// trailing metadata and expect us to forward it further.
func (p *planNodeToRowSource) forwardMetadata(metadata *distsqlrun.ProducerMetadata) {
	p.ProcessorBase.AppendTrailingMeta(*metadata)
}
