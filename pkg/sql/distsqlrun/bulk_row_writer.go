// Copyright 2019 The Cockroach Authors.
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
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// CTASPlanResultTypes is the result types for EXPORT plans.
var CTASPlanResultTypes = []types.T{
	*types.Bytes, // rows
}

type bulkRowWriter struct {
	flowCtx        *FlowCtx
	processorID    int32
	batchIdxAtomic int64
	spec           distsqlpb.BulkRowWriterSpec
	input          RowSource
	out            ProcOutputHelper
	output         RowReceiver
	summary        roachpb.BulkOpSummary
}

var _ Processor = &bulkRowWriter{}

func newBulkRowWriterProcessor(
	flowCtx *FlowCtx,
	processorID int32,
	spec distsqlpb.BulkRowWriterSpec,
	input RowSource,
	output RowReceiver,
) (Processor, error) {
	c := &bulkRowWriter{
		flowCtx:        flowCtx,
		processorID:    processorID,
		batchIdxAtomic: 0,
		spec:           spec,
		input:          input,
		output:         output,
	}
	if err := c.out.Init(&distsqlpb.PostProcessSpec{}, CTASPlanResultTypes,
		flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return c, nil
}

func (sp *bulkRowWriter) OutputTypes() []types.T {
	return CTASPlanResultTypes
}

func (sp *bulkRowWriter) ingestLoop(ctx context.Context, kvCh chan []roachpb.KeyValue) error {
	writeTS := sp.spec.Table.CreateAsOfTime
	const bufferSize, flushSize = 64 << 20, 16 << 20
	adder, err := sp.flowCtx.BulkAdder(ctx, sp.flowCtx.ClientDB,
		bufferSize, flushSize, writeTS)
	if err != nil {
		return err
	}
	defer adder.Close(ctx)

	// ingestKvs drains kvs from the channel until it closes, ingesting them using
	// the BulkAdder. It handles the required buffering/sorting/etc.
	ingestKvs := func() error {
		for kvBatch := range kvCh {
			for _, kv := range kvBatch {
				if err := adder.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
					if _, ok := err.(storagebase.DuplicateKeyError); ok {
						return errors.WithStack(err)
					}
					return err
				}
			}
		}

		if err := adder.Flush(ctx); err != nil {
			if err, ok := err.(storagebase.DuplicateKeyError); ok {
				return errors.WithStack(err)
			}
			return err
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
	ctx context.Context, kvCh chan []roachpb.KeyValue, conv *row.DatumRowConverter,
) error {
	defer close(kvCh)

	done := false
	alloc := &sqlbase.DatumAlloc{}
	input := MakeNoMetadataRowSource(sp.input, sp.output)
	typs := sp.input.OutputTypes()

	for {
		var rows int64
		for {
			row, err := input.NextRow()
			if err != nil {
				return err
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
				if err := ed.EnsureDecoded(&typs[i], alloc); err != nil {
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

func (sp *bulkRowWriter) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "bulkRowWriter")
	defer tracing.FinishSpan(span)

	var kvCh chan []roachpb.KeyValue
	var g ctxgroup.Group

	// Create a new evalCtx per converter so each go routine gets its own
	// collationenv, which can't be accessed in parallel.
	evalCtx := sp.flowCtx.EvalCtx.Copy()
	kvCh = make(chan []roachpb.KeyValue, 10)

	sp.input.Start(ctx)

	conv, err := row.NewDatumRowConverter(&sp.spec.Table, nil /* targetColNames */, evalCtx, kvCh)
	if err != nil {
		DrainAndClose(
			ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
		return
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
	err = g.Wait()

	// Emit a row with the BulkOpSummary from the processor after the rows in the
	// new table have been written.
	if err == nil {
		if countsBytes, marshalErr := protoutil.Marshal(&sp.summary); marshalErr != nil {
			err = marshalErr
		} else if cs, emitErr := sp.out.EmitRow(ctx, sqlbase.EncDatumRow{
			sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(countsBytes))),
		}); emitErr != nil {
			err = emitErr
		} else if cs != NeedMoreRows {
			err = errors.New("unexpected closure of consumer")
		}
	}

	DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
}
