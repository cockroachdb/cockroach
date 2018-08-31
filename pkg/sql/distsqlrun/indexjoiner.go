// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

	// fetcherInput wraps fetcher in a RowSource implementation and should be used
	// to get rows from the fetcher. This enables the indexJoiner to wrap the
	// fetcherInput with a stat collector when necessary.
	fetcherInput RowSource
	fetcher      sqlbase.RowFetcher
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

const indexJoinerProcName = "index joiner"

func newIndexJoiner(
	flowCtx *FlowCtx,
	processorID int32,
	spec *JoinReaderSpec,
	input RowSource,
	post *PostProcessSpec,
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
	needMutations := spec.Visibility == ScanVisibility_PUBLIC_AND_NOT_PUBLIC
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
			TrailingMetaCallback: func(ctx context.Context) []ProducerMetadata {
				ij.InternalClose()
				if meta := getTxnCoordMeta(ctx, ij.flowCtx.txn); meta != nil {
					return []ProducerMetadata{{TxnCoordMeta: meta}}
				}
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	if _, _, err := initRowFetcher(
		&ij.fetcher,
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
	ij.fetcherInput = &rowFetcherWrapper{RowFetcher: &ij.fetcher}

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		// Enable stats collection.
		ij.input = NewInputStatCollector(ij.input)
		ij.fetcherInput = NewInputStatCollector(ij.fetcherInput)
		ij.finishTrace = ij.outputStatsToTrace
	}

	return ij, nil
}

// Start is part of the RowSource interface.
func (ij *indexJoiner) Start(ctx context.Context) context.Context {
	ij.input.Start(ctx)
	ij.fetcherInput.Start(ctx)
	return ij.StartInternal(ctx, indexJoinerProcName)
}

// Next is part of the RowSource interface.
func (ij *indexJoiner) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
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
		row, meta := ij.fetcherInput.Next()
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

// ConsumerDone is part of the RowSource interface.
func (ij *indexJoiner) ConsumerDone() {
	ij.MoveToDraining(nil /* err */)
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
	key, err := sqlbase.MakeKeyFromEncDatums(
		types, keyRow, &ij.desc, &ij.desc.PrimaryIndex, ij.keyPrefix, &ij.alloc)
	if err != nil {
		return roachpb.Span{}, err
	}
	return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}, nil
}

// outputStatsToTrace outputs the collected indexJoiner stats to the trace. Will
// fail silently if the indexJoiner is not collecting stats.
func (ij *indexJoiner) outputStatsToTrace() {
	is, ok := getInputStats(ij.flowCtx, ij.input)
	if !ok {
		return
	}
	ils, ok := getInputStats(ij.flowCtx, ij.fetcherInput)
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
