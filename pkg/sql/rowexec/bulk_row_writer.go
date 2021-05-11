// Copyright 2019 The Cockroach Authors.
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
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// CTASPlanResultTypes is the result types for EXPORT plans.
var CTASPlanResultTypes = []*types.T{
	types.Bytes, // rows
}

type bulkRowWriter struct {
	execinfra.ProcessorBase
	flowCtx        *execinfra.FlowCtx
	processorID    int32
	batchIdxAtomic int64
	tableDesc      catalog.TableDescriptor
	spec           execinfrapb.BulkRowWriterSpec
	input          execinfra.RowSource
	output         execinfra.RowReceiver
	summary        roachpb.BulkOpSummary
}

var _ execinfra.Processor = &bulkRowWriter{}
var _ execinfra.RowSource = &bulkRowWriter{}

func newBulkRowWriterProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BulkRowWriterSpec,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	c := &bulkRowWriter{
		flowCtx:        flowCtx,
		processorID:    processorID,
		batchIdxAtomic: 0,
		tableDesc:      spec.BuildTableDescriptor(),
		spec:           spec,
		input:          input,
		output:         output,
	}
	if err := c.Init(
		c, &execinfrapb.PostProcessSpec{}, CTASPlanResultTypes, flowCtx, processorID, output,
		nil /* memMonitor */, execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{input}},
	); err != nil {
		return nil, err
	}
	return c, nil
}

// Start is part of the RowSource interface.
func (sp *bulkRowWriter) Start(ctx context.Context) {
	ctx = sp.StartInternal(ctx, "bulkRowWriter")
	sp.input.Start(ctx)
	err := sp.work(ctx)
	sp.MoveToDraining(err)
}

// Next is part of the RowSource interface.
func (sp *bulkRowWriter) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// If there wasn't an error while processing, output the summary.
	if sp.ProcessorBase.State == execinfra.StateRunning {
		countsBytes, marshalErr := protoutil.Marshal(&sp.summary)
		sp.MoveToDraining(marshalErr)
		if marshalErr == nil {
			// Output the summary.
			return rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(countsBytes))),
			}, nil
		}
	}
	return nil, sp.DrainHelper()
}

func (sp *bulkRowWriter) work(ctx context.Context) error {
	kvCh := make(chan row.KVBatch, 10)
	var g ctxgroup.Group

	conv, err := row.NewDatumRowConverter(ctx,
		sp.tableDesc, nil /* targetColNames */, sp.EvalCtx, kvCh, nil /* seqChunkProvider */)
	if err != nil {
		return err
	}
	if conv.EvalCtx.SessionData == nil {
		panic("uninitialized session data")
	}

	g = ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		return sp.ingestLoop(ctx, kvCh)
	})
	g.GoCtx(func(ctx context.Context) error {
		return sp.convertLoop(ctx, kvCh, conv)
	})
	return g.Wait()
}

func (sp *bulkRowWriter) wrapDupError(ctx context.Context, orig error) error {
	var typed *kvserverbase.DuplicateKeyError
	if !errors.As(orig, &typed) {
		return orig
	}
	v := &roachpb.Value{RawBytes: typed.Value}
	return row.NewUniquenessConstraintViolationError(ctx, sp.tableDesc, typed.Key, v)
}

func (sp *bulkRowWriter) ingestLoop(ctx context.Context, kvCh chan row.KVBatch) error {
	writeTS := sp.spec.Table.CreateAsOfTime
	const bufferSize = 64 << 20
	adder, err := sp.flowCtx.Cfg.BulkAdder(
		ctx, sp.flowCtx.Cfg.DB, writeTS, kvserverbase.BulkAdderOptions{
			MinBufferSize: bufferSize,
			// We disallow shadowing here to ensure that we report errors when builds
			// of unique indexes fail when there are duplicate values.
			DisallowShadowing: true,
		},
	)
	if err != nil {
		return err
	}
	defer adder.Close(ctx)

	// ingestKvs drains kvs from the channel until it closes, ingesting them using
	// the BulkAdder. It handles the required buffering/sorting/etc.
	ingestKvs := func() error {
		for kvBatch := range kvCh {
			for _, kv := range kvBatch.KVs {
				if err := adder.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
					return sp.wrapDupError(ctx, err)
				}
			}
		}

		if err := adder.Flush(ctx); err != nil {
			return sp.wrapDupError(ctx, err)
		}
		return nil
	}

	// Drain the kvCh using the BulkAdder until it closes.
	if err := ingestKvs(); err != nil {
		return err
	}

	sp.summary = adder.GetSummary()
	return nil
}

func (sp *bulkRowWriter) convertLoop(
	ctx context.Context, kvCh chan row.KVBatch, conv *row.DatumRowConverter,
) error {
	defer close(kvCh)

	done := false
	alloc := &rowenc.DatumAlloc{}
	typs := sp.input.OutputTypes()

	for {
		var rows int64
		for {
			row, meta := sp.input.Next()
			if meta != nil {
				if meta.Err != nil {
					return meta.Err
				}
				sp.AppendTrailingMeta(*meta)
				continue
			}
			if row == nil {
				done = true
				break
			}
			rows++

			for i, ed := range row {
				if ed.IsNull() {
					conv.Datums[i] = tree.DNull
					continue
				}
				if err := ed.EnsureDecoded(typs[i], alloc); err != nil {
					return err
				}
				conv.Datums[i] = ed.Datum
			}

			// `conv.Row` uses these as arguments to GenerateUniqueID to generate
			// hidden primary keys, when necessary. We want them to be ascending per
			// to reduce overlap in the resulting kvs and non-conflicting (because
			// of primary key uniqueness). The ids that come out of GenerateUniqueID
			// are sorted by (fileIndex, rowIndex) and unique as long as the two
			// inputs are a unique combo, so using the processor ID and a
			// monotonically increasing batch index should do what we want.
			if err := conv.Row(ctx, sp.processorID, sp.batchIdxAtomic); err != nil {
				return err
			}
			atomic.AddInt64(&sp.batchIdxAtomic, 1)
		}
		if rows < 1 {
			break
		}

		if err := conv.SendBatch(ctx); err != nil {
			return err
		}

		if done {
			break
		}
	}

	return nil
}
