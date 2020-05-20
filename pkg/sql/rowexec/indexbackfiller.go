// Copyright 2016 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// indexBackfiller is a processor that backfills new indexes.
type indexBackfiller struct {
	backfiller

	backfill.IndexBackfiller

	adder kvserverbase.BulkAdder

	desc *sqlbase.ImmutableTableDescriptor
}

var _ execinfra.Processor = &indexBackfiller{}
var _ chunkBackfiller = &indexBackfiller{}

var backfillerBufferSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.buffer_size", "the initial size of the BulkAdder buffer handling index backfills", 32<<20,
)

var backfillerMaxBufferSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.max_buffer_size", "the maximum size of the BulkAdder buffer handling index backfills", 512<<20,
)

var backfillerBufferIncrementSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.buffer_increment", "the size by which the BulkAdder attempts to grow its buffer before flushing", 32<<20,
)

var backillerSSTSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.max_sst_size", "target size for ingested files during backfills", 16<<20,
)

func newIndexBackfiller(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BackfillerSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
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

	// Copy in the DB pointer from flowCtx into evalCtx, because the Init
	// step needs access to the DB.
	evalCtx := flowCtx.NewEvalCtx()
	evalCtx.DB = flowCtx.Cfg.DB
	if err := ib.IndexBackfiller.Init(evalCtx, ib.desc); err != nil {
		return nil, err
	}

	return ib, nil
}

func (ib *indexBackfiller) prepare(ctx context.Context) error {
	minBufferSize := backfillerBufferSize.Get(&ib.flowCtx.Cfg.Settings.SV)
	maxBufferSize := func() int64 { return backfillerMaxBufferSize.Get(&ib.flowCtx.Cfg.Settings.SV) }
	sstSize := func() int64 { return backillerSSTSize.Get(&ib.flowCtx.Cfg.Settings.SV) }
	stepSize := backfillerBufferIncrementSize.Get(&ib.flowCtx.Cfg.Settings.SV)
	opts := kvserverbase.BulkAdderOptions{
		SSTSize:        sstSize,
		MinBufferSize:  minBufferSize,
		MaxBufferSize:  maxBufferSize,
		StepBufferSize: stepSize,
		SkipDuplicates: ib.ContainsInvertedIndex(),
	}
	adder, err := ib.flowCtx.Cfg.BulkAdder(ctx, ib.flowCtx.Cfg.DB, ib.spec.ReadAsOf, opts)
	if err != nil {
		return err
	}
	ib.adder = adder
	return nil
}

func (ib *indexBackfiller) close(ctx context.Context) {
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
	var typed *kvserverbase.DuplicateKeyError
	if !errors.As(orig, &typed) {
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
	knobs := &ib.flowCtx.Cfg.TestingKnobs
	if knobs.RunBeforeBackfillChunk != nil {
		if err := knobs.RunBeforeBackfillChunk(sp); err != nil {
			return nil, err
		}
	}
	if knobs.RunAfterBackfillChunk != nil {
		defer knobs.RunAfterBackfillChunk()
	}

	ctx, traceSpan := tracing.ChildSpan(tctx, "chunk")
	defer tracing.FinishSpan(traceSpan)

	var key roachpb.Key

	start := timeutil.Now()
	var entries []sqlbase.IndexEntry
	if err := ib.flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
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
	if knobs.RunAfterBackfillChunk != nil {
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
