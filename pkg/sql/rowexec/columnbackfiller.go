// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// columnBackfiller is a processor for backfilling columns.
type columnBackfiller struct {
	backfiller

	backfill.ColumnBackfiller

	desc *sqlbase.ImmutableTableDescriptor
}

var _ execinfra.Processor = &columnBackfiller{}
var _ chunkBackfiller = &columnBackfiller{}

func newColumnBackfiller(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BackfillerSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*columnBackfiller, error) {
	cb := &columnBackfiller{
		desc: sqlbase.NewImmutableTableDescriptor(spec.Table),
		backfiller: backfiller{
			name:        "Column",
			filter:      backfill.ColumnMutationFilter,
			flowCtx:     flowCtx,
			processorID: processorID,
			output:      output,
			spec:        spec,
		},
	}
	cb.backfiller.chunks = cb

	evalCtx := cb.flowCtx.NewEvalCtx()
	evalCtx.DB = cb.flowCtx.Cfg.DB
	if err := cb.ColumnBackfiller.Init(ctx, evalCtx, cb.desc); err != nil {
		return nil, err
	}

	return cb, nil
}

func (cb *columnBackfiller) close(ctx context.Context) {}
func (cb *columnBackfiller) prepare(ctx context.Context) error {
	return nil
}
func (cb *columnBackfiller) flush(ctx context.Context) error {
	return nil
}
func (cb *columnBackfiller) CurrentBufferFill() float32 {
	return 0
}

// runChunk implements the chunkBackfiller interface.
func (cb *columnBackfiller) runChunk(
	ctx context.Context,
	mutations []sqlbase.DescriptorMutation,
	sp roachpb.Span,
	chunkSize int64,
	readAsOf hlc.Timestamp,
) (roachpb.Key, error) {
	var key roachpb.Key
	err := cb.flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if cb.flowCtx.Cfg.TestingKnobs.RunBeforeBackfillChunk != nil {
			if err := cb.flowCtx.Cfg.TestingKnobs.RunBeforeBackfillChunk(sp); err != nil {
				return err
			}
		}
		if cb.flowCtx.Cfg.TestingKnobs.RunAfterBackfillChunk != nil {
			defer cb.flowCtx.Cfg.TestingKnobs.RunAfterBackfillChunk()
		}

		// TODO(knz): do KV tracing in DistSQL processors.
		var err error
		key, err = cb.RunColumnBackfillChunk(
			ctx,
			txn,
			cb.desc,
			sp,
			chunkSize,
			true,  /*alsoCommit*/
			false, /*traceKV*/
		)
		return err
	})
	return key, err
}
