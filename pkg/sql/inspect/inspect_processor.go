// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type inspectProcessor struct {
	execinfra.ProcessorBase
	spec execinfrapb.InspectSpec
}

var _ execinfra.Processor = (*inspectProcessor)(nil)
var _ execinfra.RowSource = (*inspectProcessor)(nil)

func (p *inspectProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return nil, p.DrainHelper()
}

func (p *inspectProcessor) Start(context.Context) {}

func (p *inspectProcessor) Run(ctx context.Context, output execinfra.RowReceiver) {
	ctx = p.StartInternal(ctx, "inspect")
	err := p.runInspect(ctx, output)
	if err != nil {
		output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
	}
	execinfra.SendTraceData(ctx, p.FlowCtx, output)
	output.ProducerDone()
}

func (p *inspectProcessor) runInspect(ctx context.Context, output execinfra.RowReceiver) error {
	log.Infof(ctx, "INSPECT processor started processorID=%d", p.ProcessorID)
	return nil
}

func newInspectProcessor(
	ctx context.Context, flowCtx *execinfra.FlowCtx, processorID int32, spec execinfrapb.InspectSpec,
) (execinfra.Processor, error) {
	proc := &inspectProcessor{
		spec: spec,
	}
	if err := proc.Init(
		ctx,
		proc,
		&execinfrapb.PostProcessSpec{},
		[]*types.T{},
		flowCtx,
		processorID,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{},
	); err != nil {
		return nil, err
	}
	return proc, nil
}

func init() {
	rowexec.NewInspectProcessor = newInspectProcessor
}
