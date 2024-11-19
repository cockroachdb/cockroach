// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	spec execinfrapb.IngestStoppedSpec
}

func newIngestStoppedProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.IngestStoppedSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	p := &proc{spec: spec}
	if err := p.Init(ctx, p, post, nil, flowCtx, processorID, nil /* memMonitor */, execinfra.ProcStateOpts{}); err != nil {
		return nil, err
	}
	return p, nil
}

// Start is part of the RowSource interface.
func (p *proc) Start(ctx context.Context) {
	p.StartInternal(ctx, "ingeststopped.proc")
	if p.FlowCtx.Cfg.JobRegistry.IsIngesting(p.spec.JobID) {
		p.MoveToDraining(errors.Errorf("jobs is still ingesting on node %d", p.FlowCtx.NodeID.SQLInstanceID()))
	} else {
		p.MoveToDraining(nil)
	}
}

// Next is part of the RowSource interface.
func (p *proc) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return nil, p.DrainHelper()
}

func init() {
	rowexec.NewIngestStoppedProcessor = newIngestStoppedProcessor
}
