package ingeststopped

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/errors"
)

type proc struct {
	execinfra.ProcessorBase
}

func newIngestStoppedProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.IngestStoppedSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	p := &proc{}
	if err := p.Init(ctx, p, post, nil, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			// This processor doesn't have any inputs to drain.
			InputsToDrain:        nil,
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata { return nil },
		}); err != nil {
		return nil, err
	}
	if flowCtx.Cfg.JobRegistry.IsIngesting(spec.JobID) {
		p.MoveToDraining(errors.Errorf("jobs is still ingesting on node %d", flowCtx.NodeID.SQLInstanceID()))
	} else {
		p.MoveToDraining(nil)
	}

	return p, nil
}

// Start is part of the RowSource interface.
func (p *proc) Start(ctx context.Context) {
}

// Next is part of the RowSource interface.
func (p *proc) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return nil, p.DrainHelper()
}

func (p *proc) ConsumerClosed() {
	p.InternalClose()
}

func init() {
	rowexec.NewIngestStoppedProcessor = newIngestStoppedProcessor
}
