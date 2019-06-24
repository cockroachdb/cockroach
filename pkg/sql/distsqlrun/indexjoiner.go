// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

const indexJoinerBatchSize = 100

// indexJoiner performs a join between a secondary index, the `input`, and the
// primary index of the same table, `desc`, to retrieve columns which are not
// stored in the secondary index.
type indexJoiner struct {
	ProcessorBase

	input RowSource
	desc  sqlbase.TableDescriptor

	// fetcher wraps the row.Fetcher used to perform lookups. This enables the
	// indexJoiner to wrap the fetcher with a stat collector when necessary.
	fetcher rowFetcher
	// fetcherReady indicates that we have started an index scan and there are
	// potentially more rows to retrieve.
	fetcherReady bool
	// Batch size for fetches. Not a constant so we can lower for testing.
	batchSize int

	// keyPrefix is the primary index's key prefix.
	keyPrefix []byte
	// spans is the batch of spans we will next retrieve from the index.
	spans roachpb.Spans

	alloc sqlbase.DatumAlloc
}

var _ Processor = &indexJoiner{}
var _ RowSource = &indexJoiner{}
var _ distsqlpb.MetadataSource = &indexJoiner{}

const indexJoinerProcName = "index joiner"

func newIndexJoiner(
	flowCtx *FlowCtx,
	processorID int32,
	spec *distsqlpb.JoinReaderSpec,
	input RowSource,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
) (*indexJoiner, error) {
	if spec.IndexIdx != 0 {
		return nil, errors.Errorf("index join must be against primary index")
	}
	ij := &indexJoiner{
		input:     input,
		desc:      spec.Table,
		keyPrefix: sqlbase.MakeIndexKeyPrefix(&spec.Table, spec.Table.PrimaryIndex.ID),
		batchSize: indexJoinerBatchSize,
	}
	needMutations := spec.Visibility == distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	if err := ij.Init(
		ij,
		post,
		ij.desc.ColumnTypesWithMutations(needMutations),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		ProcStateOpts{
			InputsToDrain: []RowSource{ij.input},
			TrailingMetaCallback: func(ctx context.Context) []distsqlpb.ProducerMetadata {
				ij.InternalClose()
				return ij.generateMeta(ctx)
			},
		},
	); err != nil {
		return nil, err
	}
	var fetcher row.Fetcher
	if _, _, err := initRowFetcher(
		&fetcher,
		&ij.desc,
		0, /* primary index */
		ij.desc.ColumnIdxMapWithMutations(needMutations),
		false, /* reverse */
		ij.out.neededColumns(),
		false, /* isCheck */
		&ij.alloc,
		spec.Visibility,
	); err != nil {
		return nil, err
	}

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		// Enable stats collection.
		ij.input = NewInputStatCollector(ij.input)
		ij.fetcher = newRowFetcherStatCollector(&fetcher)
		ij.finishTrace = ij.outputStatsToTrace
	} else {
		ij.fetcher = &rowFetcherWrapper{Fetcher: &fetcher}
	}

	return ij, nil
}

// Start is part of the RowSource interface.
func (ij *indexJoiner) Start(ctx context.Context) context.Context {
	ij.input.Start(ctx)
	ij.fetcher.Start(ctx)
	return ij.StartInternal(ctx, indexJoinerProcName)
}

// Next is part of the RowSource interface.
func (ij *indexJoiner) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for ij.State == StateRunning {
		if !ij.fetcherReady {
			// Retrieve a batch of rows from the input.
			for len(ij.spans) < ij.batchSize {
				row, meta := ij.input.Next()
				if meta != nil {
					if meta.Err != nil {
						ij.MoveToDraining(nil /* err */)
					}
					return nil, meta
				}
				if row == nil {
					break
				}
				span, err := ij.generateSpan(row)
				if err != nil {
					ij.MoveToDraining(err)
					return nil, ij.DrainHelper()
				}
				ij.spans = append(ij.spans, span)
			}
			if len(ij.spans) == 0 {
				// All done.
				ij.MoveToDraining(nil /* err */)
				return nil, ij.DrainHelper()
			}
			// Scan the primary index for this batch.
			err := ij.fetcher.StartScan(
				ij.Ctx, ij.flowCtx.txn, ij.spans, false /* limitBatches */, 0, /* limitHint */
				ij.flowCtx.traceKV)
			if err != nil {
				ij.MoveToDraining(err)
				return nil, ij.DrainHelper()
			}
			ij.fetcherReady = true
			ij.spans = ij.spans[:0]
		}
		row, meta := ij.fetcher.Next()
		if meta != nil {
			ij.MoveToDraining(scrub.UnwrapScrubError(meta.Err))
			return nil, ij.DrainHelper()
		}
		if row == nil {
			// Done with this batch.
			ij.fetcherReady = false
		} else if outRow := ij.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, ij.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (ij *indexJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ij.InternalClose()
}

func (ij *indexJoiner) generateSpan(row sqlbase.EncDatumRow) (roachpb.Span, error) {
	numKeyCols := len(ij.desc.PrimaryIndex.ColumnIDs)
	if len(row) < numKeyCols {
		return roachpb.Span{}, errors.Errorf(
			"index join input has %d columns, expected at least %d", len(row), numKeyCols)
	}
	// There may be extra values on the row, e.g. to allow an ordered
	// synchronizer to interleave multiple input streams. Will need at most
	// numKeyCols.
	keyRow := row[:numKeyCols]
	types := ij.input.OutputTypes()[:numKeyCols]
	return sqlbase.MakeSpanFromEncDatums(
		ij.keyPrefix, keyRow, types, ij.desc.PrimaryIndex.ColumnDirections, &ij.desc,
		&ij.desc.PrimaryIndex, &ij.alloc)
}

// outputStatsToTrace outputs the collected indexJoiner stats to the trace. Will
// fail silently if the indexJoiner is not collecting stats.
func (ij *indexJoiner) outputStatsToTrace() {
	is, ok := getInputStats(ij.flowCtx, ij.input)
	if !ok {
		return
	}
	ils, ok := getFetcherInputStats(ij.flowCtx, ij.fetcher)
	if !ok {
		return
	}
	jrs := &JoinReaderStats{
		InputStats:       is,
		IndexLookupStats: ils,
	}
	if sp := opentracing.SpanFromContext(ij.Ctx); sp != nil {
		tracing.SetSpanStats(sp, jrs)
	}
}

func (ij *indexJoiner) generateMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	if meta := getTxnCoordMeta(ctx, ij.flowCtx.txn); meta != nil {
		return []distsqlpb.ProducerMetadata{{TxnCoordMeta: meta}}
	}
	return nil
}

// DrainMeta is part of the MetadataSource interface.
func (ij *indexJoiner) DrainMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	return ij.generateMeta(ctx)
}
