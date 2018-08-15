package distsqlrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type countStarTable struct {
	ProcessorBase

	table *sqlbase.TableDescriptor
}

const countStarTableProcName = "count star table"

func newCountStarTable(
	flowCtx *FlowCtx, processorID int32, spec *CountStarTableSpec, post *PostProcessSpec, output RowReceiver,
) (*countStarTable, error) {
	ag := &countStarTable{
		table: &spec.Table,
	}

	if err := ag.Init(
		ag,
		post,
		outputTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		ProcStateOpts{},
	); err != nil {
		return nil, err
	}

	return ag, nil
}

func (c *countStarTable) Start(context.Context) context.Context {
	return c.StartInternal(ctx, countStarTableProcName)
}

func (c *countStarTable) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	c.flowCtx.txn.
		panic("implement me")
}

func (c *countStarTable) ConsumerDone() {
	c.MoveToDraining(nil /* err */)
}

func (c *countStarTable) ConsumerClosed() {
	c.InternalClose()
}
