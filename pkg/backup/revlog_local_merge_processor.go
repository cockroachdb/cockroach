// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const revlogLocalMergeProcessorName = "revlogLocalMerge"

// revlogLocalMergeProcessor runs one node's share of the revision log
// local merge. Each processor receives a set of tick manifests in its
// spec and merges them into consolidated SSTs on nodelocal storage.
//
// Phase 1 (this implementation) is a stub that logs the tick
// assignment and immediately drains. Phase 2 will implement the
// actual k-way merge, key prefix rewriting, and SST output.
type revlogLocalMergeProcessor struct {
	execinfra.ProcessorBase
	spec execinfrapb.RevlogLocalMergeSpec
}

var (
	_ execinfra.Processor = &revlogLocalMergeProcessor{}
	_ execinfra.RowSource = &revlogLocalMergeProcessor{}
)

func newRevlogLocalMergeProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.RevlogLocalMergeSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	proc := &revlogLocalMergeProcessor{spec: spec}
	if err := proc.Init(
		ctx, proc, post, []*types.T{}, flowCtx, processorID,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: nil,
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	return proc, nil
}

// Start implements the execinfra.RowSource interface.
func (p *revlogLocalMergeProcessor) Start(ctx context.Context) {
	p.StartInternal(ctx, revlogLocalMergeProcessorName)
	log.Dev.Infof(
		p.Ctx(),
		"revlog local merge processor started with %d ticks",
		len(p.spec.Ticks),
	)
}

// Next implements the execinfra.RowSource interface.
func (p *revlogLocalMergeProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// Phase 1 stub: immediately signal completion.
	p.MoveToDraining(nil /* err */)
	return nil, p.DrainHelper()
}

func init() {
	rowexec.NewRevlogLocalMergeProcessor = newRevlogLocalMergeProcessor
}
