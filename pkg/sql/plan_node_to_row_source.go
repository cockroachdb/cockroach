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
	node        planNode
	params      runParams
	outputTypes []sqlbase.ColumnType

	// run time state machine values
	ctx context.Context
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

	return &planNodeToRowSource{
		node:        source,
		params:      params,
		outputTypes: types,
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

func (p *planNodeToRowSource) Next() (sqlbase.EncDatumRow, *distsqlrun.ProducerMetadata) {
	valid, err := p.node.Next(p.params)
	if err != nil {
		return nil, &distsqlrun.ProducerMetadata{Err: err}
	}
	if !valid {
		return nil, nil
	}

	rows := p.node.Values()
	return sqlbase.DatumsToEncDatumRow(rows, p.outputTypes), nil
}

func (p *planNodeToRowSource) ConsumerDone() {
}

func (p *planNodeToRowSource) ConsumerClosed() {
	p.node.Close(p.ctx)
}
