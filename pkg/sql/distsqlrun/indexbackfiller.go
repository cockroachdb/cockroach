// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// indexBackfiller is a processor that backfills new indexes.
type indexBackfiller struct {
	backfiller

	backfill.IndexBackfiller

	adder storagebase.BulkAdder

	desc *sqlbase.ImmutableTableDescriptor
}

var _ Processor = &indexBackfiller{}
var _ chunkBackfiller = &indexBackfiller{}

var backfillerBufferSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.buffer_size", "amount to buffer in memory during backfills", 196<<20,
)

var backillerSSTSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.max_sst_size", "target size for ingested files during backfills", 16<<20,
)

func newIndexBackfiller(
	flowCtx *FlowCtx,
	processorID int32,
	spec distsqlpb.BackfillerSpec,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
) (*indexBackfiller, error) {
	ib := &indexBackfiller{
		desc: sqlbase.NewImmutableTableDescriptor(spec.Table),
		backfiller: backfiller{
			name:        "Index",
			filter:      backfill.IndexMutationFilter,
			flowCtx:     flowCtx,
			processorID: processorID,
			output:      output,
			spec:        spec,
		},
	}
	ib.backfiller.chunks = ib

	if err := ib.IndexBackfiller.Init(ib.desc); err != nil {
		return nil, err
	}

	return ib, nil
}

func (ib *indexBackfiller) prepare(ctx context.Context) error {
	bufferSize := backfillerBufferSize.Get(&ib.flowCtx.Settings.SV)
	sstSize := backillerSSTSize.Get(&ib.flowCtx.Settings.SV)
	adder, err := ib.flowCtx.BulkAdder(ctx, ib.flowCtx.ClientDB, bufferSize, sstSize, ib.spec.ReadAsOf)
	if err != nil {
		return err
	}
	ib.adder = adder
	ib.adder.SkipLocalDuplicates(ib.ContainsInvertedIndex())
	return nil
}

func (ib indexBackfiller) close(ctx context.Context) {
	ib.adder.Close(ctx)
}

func (ib *indexBackfiller) flush(ctx context.Context) error {
	return ib.wrapDupError(ctx, ib.adder.Flush(ctx))
}

func (ib *indexBackfiller) CurrentBufferFill() float32 {
	return ib.adder.CurrentBufferFill()
}

func (ib *indexBackfiller) wrapDupError(ctx context.Context, orig error) error {
	if orig == nil {
		return nil
	}
	typed, ok := orig.(storagebase.DuplicateKeyError)
	if !ok {
		return orig
	}

	desc, err := ib.desc.MakeFirstMutationPublic(sqlbase.IncludeConstraints)
	immutable := sqlbase.NewImmutableTableDescriptor(*desc.TableDesc())
	if err != nil {
		return err
	}
	v := &roachpb.Value{RawBytes: typed.Value}
	return row.NewUniquenessConstraintViolationError(ctx, immutable, typed.Key, v)
}

func (ib *indexBackfiller) runChunk(
	tctx context.Context,
	mutations []sqlbase.DescriptorMutation,
	sp roachpb.Span,
	chunkSize int64,
	readAsOf hlc.Timestamp,
) (roachpb.Key, error) {
	if ib.flowCtx.testingKnobs.RunBeforeBackfillChunk != nil {
		if err := ib.flowCtx.testingKnobs.RunBeforeBackfillChunk(sp); err != nil {
			return nil, err
		}
	}
	if ib.flowCtx.testingKnobs.RunAfterBackfillChunk != nil {
		defer ib.flowCtx.testingKnobs.RunAfterBackfillChunk()
	}

	ctx, traceSpan := tracing.ChildSpan(tctx, "chunk")
	defer tracing.FinishSpan(traceSpan)

	var key roachpb.Key

	start := timeutil.Now()
	var entries []sqlbase.IndexEntry
	if err := ib.flowCtx.ClientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		txn.SetFixedTimestamp(ctx, readAsOf)

		// TODO(knz): do KV tracing in DistSQL processors.
		var err error
		entries, key, err = ib.BuildIndexEntriesChunk(ctx, txn, ib.desc, sp, chunkSize, false /*traceKV*/)
		return err
	}); err != nil {
		return nil, err
	}
	prepTime := timeutil.Now().Sub(start)

	start = timeutil.Now()
	for _, i := range entries {
		if err := ib.adder.Add(ctx, i.Key, i.Value.RawBytes); err != nil {
			return nil, ib.wrapDupError(ctx, err)
		}
	}
	if ib.flowCtx.testingKnobs.RunAfterBackfillChunk != nil {
		if err := ib.adder.Flush(ctx); err != nil {
			return nil, ib.wrapDupError(ctx, err)
		}
	}
	addTime := timeutil.Now().Sub(start)

	if log.V(3) {
		log.Infof(ctx, "index backfill stats: entries %d, prepare %+v, add-sst %+v",
			len(entries), prepTime, addTime)
	}
	return key, nil
}
