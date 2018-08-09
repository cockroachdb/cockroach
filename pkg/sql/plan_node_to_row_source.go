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

	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type planNodeToRowSource struct {
	started bool
	running bool

	fastPath bool

	node        planNode
	params      runParams
	outputTypes []sqlbase.ColumnType

	out distsqlrun.ProcOutputHelper

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
		running:     true,
		fastPath:    fastPath,
	}, nil
}

var _ distsqlrun.LocalProcessor = &planNodeToRowSource{}

// InitWithOutput implements the LocalProcessor interface.
func (p *planNodeToRowSource) InitWithOutput(
	post *distsqlrun.PostProcessSpec, output distsqlrun.RowReceiver,
) error {
	return p.out.Init(post, p.outputTypes, p.params.EvalContext(), output)
}

func (p *planNodeToRowSource) OutputTypes() []sqlbase.ColumnType {
	return p.out.OutputTypes()
}

func (p *planNodeToRowSource) Start(ctx context.Context) context.Context {
	p.params.ctx = ctx
	return ctx
}

func (p *planNodeToRowSource) internalClose() {
	if p.running {
		p.node.Close(p.params.ctx)
		p.running = false
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
	if !p.running {
		return nil, nil
	}
	if !p.started {
		p.started = true
		// This starts all of the nodes below this node.
		if err := startExec(p.params, p.node); err != nil {
			p.internalClose()
			return nil, &distsqlrun.ProducerMetadata{Err: err}
		}

		if p.fastPath {
			var count int
			// If our node is a "fast path node", it means that we're set up to just
			// return a row count. So trigger the fast path and return the row count as
			// a row with a single column.
			if fastPath, ok := p.node.(planNodeFastPath); ok {
				count, ok = fastPath.FastPathResults()
				if !ok {
					p.internalClose()
					return nil, nil
				}
				if p.params.extendedEvalCtx.Tracing.Enabled() {
					log.VEvent(p.params.ctx, 2, "fast path completed")
				}
			} else {
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
					return nil, &distsqlrun.ProducerMetadata{Err: err}
				}
			}
			p.internalClose()
			// Return the row count the only way we can: as a single-column row with
			// the count inside.
			return sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(count))}}, nil
		}
	}

	for p.running {
		valid, err := p.node.Next(p.params)
		if err != nil {
			p.internalClose()
			return nil, &distsqlrun.ProducerMetadata{Err: err}
		}
		if !valid {
			p.internalClose()
			return nil, nil
		}

		for i, datum := range p.node.Values() {
			p.row[i] = sqlbase.DatumToEncDatum(p.outputTypes[i], datum)
		}
		// ProcessRow here is required to deal with projections, which won't be
		// pushed into the wrapped plan.
		outRow, ok, err := p.out.ProcessRow(p.params.ctx, p.row)
		if err != nil {
			p.internalClose()
			return nil, &distsqlrun.ProducerMetadata{Err: err}
		}
		if !ok {
			p.internalClose()
		}
		if outRow != nil {
			return outRow, nil
		}
	}
	return nil, nil
}

func (p *planNodeToRowSource) ConsumerDone() {
	p.internalClose()
}

func (p *planNodeToRowSource) ConsumerClosed() {
	p.internalClose()
}

func (p *planNodeToRowSource) Run(ctx context.Context, wg *sync.WaitGroup) {
	if p.out.Output() == nil {
		panic("processor output not initialized for emitting rows")
	}
	ctx = p.Start(ctx)
	distsqlrun.Run(ctx, p, p.out.Output())
	p.internalClose()
	if wg != nil {
		wg.Done()
	}
}
