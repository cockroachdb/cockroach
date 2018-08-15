package distsqlrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

func (c *countStarTable) Start(ctx context.Context) context.Context {
	return c.StartInternal(ctx, countStarTableProcName)
}

func (c *countStarTable) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for c.State == StateRunning {
		c.MoveToDraining(nil)
		var b roachpb.BatchRequest
		span := c.table.PrimaryIndexSpan()
		b.Add(&roachpb.CountKeysRequest{
			RequestHeader: roachpb.RequestHeader{Key: span.Key, EndKey: span.EndKey},
		})

		resp, pErr := c.flowCtx.txn.Send(c.flowCtx.EvalCtx.Ctx(), b)
		if pErr != nil {
			log.Fatal(c.flowCtx.EvalCtx.Ctx(), pErr)
		}

		count := resp.Responses[0].GetInner().(*roachpb.CountKeysResponse).Count

		ret := make(sqlbase.EncDatumRow, 1)
		ret[0] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(count))}
		return ret, nil
	}
	return nil, nil
}

func (c *countStarTable) ConsumerDone() {
	c.MoveToDraining(nil /* err */)
}

func (c *countStarTable) ConsumerClosed() {
	c.InternalClose()
}
