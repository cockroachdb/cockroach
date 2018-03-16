package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type planNodeToRowSource struct {
	source      planNode
	params      runParams
	outputTypes []sqlbase.ColumnType

	// run time state machine values
	valid bool
	ctx   context.Context
}

func CreatePlanNodeToRowSource(source planNode, params runParams) (*planNodeToRowSource, error) {
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
		source:      source,
		params:      params,
		outputTypes: types,
	}, nil
}

var _ distsqlrun.RowSource = &planNodeToRowSource{}

func (p *planNodeToRowSource) OutputTypes() []sqlbase.ColumnType {
	return p.outputTypes
}

func (p *planNodeToRowSource) Start(ctx context.Context) context.Context {
	p.valid = true
	p.ctx = ctx
	return ctx
}

func (p *planNodeToRowSource) Next() (sqlbase.EncDatumRow, *distsqlrun.ProducerMetadata) {
	valid, err := p.source.Next(p.params)
	if err != nil {
		return nil, &distsqlrun.ProducerMetadata{Err: err}
	}
	if !valid {
		p.valid = false
		return nil, nil
	}

	rows := p.source.Values()
	return p.DatumsToEncDatumRow(rows), nil
}

func (p *planNodeToRowSource) ConsumerDone() {}

func (p *planNodeToRowSource) ConsumerClosed() {
	p.source.Close(p.ctx)
}

func (p *planNodeToRowSource) DatumsToEncDatumRow(values tree.Datums) sqlbase.EncDatumRow {
	encDatumRow := make(sqlbase.EncDatumRow, len(values))
	for i, datum := range values {
		encDatumRow[i] = sqlbase.DatumToEncDatum(p.outputTypes[i], datum)
	}
	return encDatumRow
}
