// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

type inspectProcessor struct {
	processorID int32
	flowCtx     *execinfra.FlowCtx
	spec        execinfrapb.InspectSpec
}

var _ execinfra.Processor = (*inspectProcessor)(nil)

// OutputTypes is part of the execinfra.Processor interface.
func (*inspectProcessor) OutputTypes() []*types.T {
	return nil
}

// MustBeStreaming is part of the execinfra.Processor interface.
func (*inspectProcessor) MustBeStreaming() bool {
	return false
}

// Resume is part of the execinfra.Processor interface.
func (*inspectProcessor) Resume(execinfra.RowReceiver) {
	panic("not implemented")
}

// Close is part of the execinfra.Processor interface.
func (*inspectProcessor) Close(context.Context) {}

// Run is part of the execinfra.Processor interface.
func (p *inspectProcessor) Run(ctx context.Context, output execinfra.RowReceiver) {
	var span *tracing.Span
	noSpan := p.flowCtx != nil && p.flowCtx.Cfg != nil &&
		p.flowCtx.Cfg.TestingKnobs.ProcessorNoTracingSpan
	if !noSpan {
		ctx, span = execinfra.ProcessorSpan(ctx, p.flowCtx, "inspect", p.processorID)
		defer span.Finish()
	}
	err := p.runInspect(ctx, output)
	if err != nil {
		output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
	}
	execinfra.SendTraceData(ctx, p.flowCtx, output)
	output.ProducerDone()
}

func (p *inspectProcessor) runInspect(ctx context.Context, output execinfra.RowReceiver) error {
	log.Infof(ctx, "INSPECT processor started processorID=%d", p.processorID)
	return nil
}

func newInspectProcessor(
	ctx context.Context, flowCtx *execinfra.FlowCtx, processorID int32, spec execinfrapb.InspectSpec,
) (execinfra.Processor, error) {
	return &inspectProcessor{
		spec:        spec,
		processorID: processorID,
		flowCtx:     flowCtx,
	}, nil
}

func init() {
	rowexec.NewInspectProcessor = newInspectProcessor
}
