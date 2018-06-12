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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type planNodeToRowSource struct {
	running bool

	node        planNode
	params      runParams
	outputTypes []sqlbase.ColumnType

	// run time state machine values
	ctx context.Context
	row sqlbase.EncDatumRow
}

func makePlanNodeToRowSource(source planNode, params runParams) (*planNodeToRowSource, error) {
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
	}, nil
}

var _ distsqlrun.RowSource = &planNodeToRowSource{}

func (p *planNodeToRowSource) OutputTypes() []sqlbase.ColumnType {
	return p.outputTypes
}

func (p *planNodeToRowSource) Start(ctx context.Context) context.Context {
	p.ctx = ctx
	return ctx
}

func (p *planNodeToRowSource) internalClose() {
	if p.running {
		p.node.Close(p.ctx)
		p.running = false
	}
}

func (p *planNodeToRowSource) Next() (sqlbase.EncDatumRow, *distsqlrun.ProducerMetadata) {
	if !p.running {
		return nil, nil
	}

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
	return p.row, nil
}

func (p *planNodeToRowSource) ConsumerDone() {
	p.internalClose()
}

func (p *planNodeToRowSource) ConsumerClosed() {
	p.internalClose()
}
