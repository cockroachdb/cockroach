// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
)

var ingestFileProcessorOutputTypes = []*types.T{
	// No output types
}

var (
	_ execinfra.Processor = &ingestFileProcessor{}
	_ execinfra.RowSource = &ingestFileProcessor{}
)

type ingestFileProcessor struct {
	execinfra.ProcessorBase
	spec    execinfrapb.IngestFileSpec
	flowCtx *execinfra.FlowCtx
	ctx     context.Context

	buffer []byte
}

func (p *ingestFileProcessor) Start(ctx context.Context) {
	p.StartInternal(ctx, "ingestFileProcessor")
	p.ctx = ctx
}

func (p *ingestFileProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if p.State != execinfra.StateRunning {
		return nil, p.DrainHelper()
	}

	// TODO(jeffswenson): read tasks from an input stream instead of assuming
	// this processor should handle every task in the spec.
	for _, sst := range p.spec.Ssts {
		if err := p.doIngest(p.ctx, sst); err != nil {
			p.MoveToDraining(err)
			return nil, p.DrainHelper()
		}
	}

	p.MoveToDraining(nil)
	return nil, p.DrainHelper()
}

func (p *ingestFileProcessor) claimLease(ctx context.Context, span roachpb.Span) error {
	// TODO(jeffswenson): make the node processing the request the leader for
	// the range.
	return nil
}

func (p *ingestFileProcessor) doIngest(
	ctx context.Context, sst execinfrapb.BulkMergeSpec_SST,
) error {
	// TODO(jeffswenson): cache the external storage between objects.
	storage, err := p.flowCtx.Cfg.ExternalStorageFromURI(ctx, sst.Uri, p.flowCtx.EvalCtx.SessionData().User())
	if err != nil {
		return err
	}

	db := p.flowCtx.Cfg.DB.KV()
	err = func() error {
		reader, _, err := storage.ReadFile(ctx, "", cloud.ReadOptions{})
		if err != nil {
			return err
		}
		defer reader.Close(ctx)

		// TODO(jeffswenson): okay, we probably do want to share this buffer
		// across to ingest. So we need a long running flow after all.
		var bytes []byte
		bytes, err = ioctx.ReadAll(ctx, reader)
		if err != nil {
			return err
		}

		end := roachpb.Key(sst.EndKey).Next()

		// TODO(jeffswenson): how to handle replays?
		_, _, err = db.AddSSTable(ctx, sst.StartKey, end, bytes, false, hlc.Timestamp{}, nil, false, hlc.Timestamp{})
		return err
	}()
	if err != nil {
		return err
	}
	return nil
}

func newIngestFileProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.IngestFileSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	ip := &ingestFileProcessor{
		spec:    spec,
		flowCtx: flowCtx,
	}
	err := ip.Init(ctx, ip, post, ingestFileProcessorOutputTypes, flowCtx, processorID, nil, execinfra.ProcStateOpts{})
	if err != nil {
		return nil, err
	}
	return ip, nil
}

func init() {
	rowexec.NewIngestFileProcessor = newIngestFileProcessor
}
