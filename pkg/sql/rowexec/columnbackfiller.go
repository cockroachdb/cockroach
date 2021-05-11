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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// columnBackfiller is a processor for backfilling columns.
type columnBackfiller struct {
	backfiller

	backfill.ColumnBackfiller

	desc catalog.TableDescriptor

	// commitWaitFns contains a set of functions, each of which was returned
	// from a call to (*kv.Txn).DeferCommitWait when backfilling a single chunk
	// of rows. The functions must be called to ensure consistency with any
	// causally dependent readers.
	commitWaitFns []func(context.Context) error
}

var _ execinfra.Processor = &columnBackfiller{}
var _ chunkBackfiller = &columnBackfiller{}

// maxCommitWaitFns is the maximum number of commit-wait functions that the
// columnBackfiller will accumulate before consuming them to reclaim memory.
// Each function retains a reference to its corresponding TxnCoordSender, so we
// need to be careful not to accumulate an unbounded number of these functions.
var backfillerMaxCommitWaitFns = settings.RegisterIntSetting(
	"schemachanger.backfiller.max_commit_wait_fns",
	"the maximum number of commit-wait functions that the columnBackfiller will accumulate before consuming them to reclaim memory",
	128,
	settings.PositiveInt,
)

func newColumnBackfiller(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BackfillerSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*columnBackfiller, error) {
	columnBackfillerMon := execinfra.NewMonitor(ctx, flowCtx.Cfg.BackfillerMonitor,
		"column-backfill-mon")
	cb := &columnBackfiller{
		desc: spec.BuildTableDescriptor(),
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

	if err := cb.ColumnBackfiller.InitForDistributedUse(ctx, flowCtx, cb.desc,
		columnBackfillerMon); err != nil {
		return nil, err
	}

	return cb, nil
}

func (cb *columnBackfiller) close(ctx context.Context) {
	cb.ColumnBackfiller.Close(ctx)
}

func (cb *columnBackfiller) prepare(ctx context.Context) error {
	return nil
}
func (cb *columnBackfiller) flush(ctx context.Context) error {
	return cb.runCommitWait(ctx)
}
func (cb *columnBackfiller) CurrentBufferFill() float32 {
	return 0
}

// runChunk implements the chunkBackfiller interface.
func (cb *columnBackfiller) runChunk(
	ctx context.Context, sp roachpb.Span, chunkSize int64, _ hlc.Timestamp,
) (roachpb.Key, error) {
	var key roachpb.Key
	var commitWaitFn func(context.Context) error
	err := cb.flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if cb.flowCtx.Cfg.TestingKnobs.RunBeforeBackfillChunk != nil {
			if err := cb.flowCtx.Cfg.TestingKnobs.RunBeforeBackfillChunk(sp); err != nil {
				return err
			}
		}
		if cb.flowCtx.Cfg.TestingKnobs.RunAfterBackfillChunk != nil {
			defer cb.flowCtx.Cfg.TestingKnobs.RunAfterBackfillChunk()
		}

		// Defer the commit-wait operation so that we can coalesce this wait
		// across all batches. This dramatically reduces the total time we spend
		// waiting for consistency when backfilling a column on GLOBAL tables.
		commitWaitFn = txn.DeferCommitWait(ctx)

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
	if err == nil {
		cb.commitWaitFns = append(cb.commitWaitFns, commitWaitFn)
		maxCommitWaitFns := int(backfillerMaxCommitWaitFns.Get(&cb.flowCtx.Cfg.Settings.SV))
		if len(cb.commitWaitFns) >= maxCommitWaitFns {
			if err := cb.runCommitWait(ctx); err != nil {
				return nil, err
			}
		}
	}
	return key, err
}

// runCommitWait consumes the commit-wait functions that the columnBackfiller
// has accumulated across the chunks that it has backfilled. It calls each
// commit-wait function to ensure that any dependent reads on the rows we just
// backfilled observe the new column.
func (cb *columnBackfiller) runCommitWait(ctx context.Context) error {
	for i, fn := range cb.commitWaitFns {
		if err := fn(ctx); err != nil {
			return err
		}
		cb.commitWaitFns[i] = nil
	}
	cb.commitWaitFns = cb.commitWaitFns[:0]
	return nil
}
