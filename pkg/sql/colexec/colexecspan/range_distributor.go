// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecspan

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// TupleRangeDistributor is a colflow.TupleDistributor that distributes tuples based
// on a preset mapping of spans to nodes. Keys are assembled from the values at
// each routingCol'th value in each tuple, and these keys are located within
// the span mapping.
// This TupleRangeDistributor is used to implement the "by range" routing policy for
// DistSQL.
type TupleRangeDistributor struct {
	// selections stores the selection vectors that actually define how to
	// distribute the tuples from the batch.
	selections [][]int

	assembler ColSpanAssembler

	spans []execinfrapb.OutputRouterSpec_RangeRouterSpec_Span

	// defaultDest, if set, sends any row not matching a span to this stream. If
	// not set and a non-matching row is encountered, an error is returned and
	// the router is shut down.
	defaultDest *int32

	// cancelChecker is used during the hashing of the rows to distribute to
	// check for query cancellation.
	cancelChecker colexecutils.CancelChecker
}

// NewTupleRangeDistributor constructs a new TupleRangeDistributor that routes
// tuples based on the given mapping inside of the spans parameter, using
// the given set of encodings, one per input type, and sends them to the given
// number of outputs. If a particular tuple doesn't have an associated output
// judging by the mapping in spans, it will be sent to the defaultDest'th
// output.
func NewTupleRangeDistributor(
	codec keys.SQLCodec,
	allocator *colmem.Allocator,
	inputTypes []*types.T,
	spans []execinfrapb.OutputRouterSpec_RangeRouterSpec_Span,
	encodings []execinfrapb.OutputRouterSpec_RangeRouterSpec_ColumnEncoding,
	numOutputs int,
	defaultDest *int32,
) *TupleRangeDistributor {
	keyEncDirections := make([]catenumpb.IndexColumn_Direction, len(encodings))
	keyOrdinals := make([]int, len(encodings))
	for i := range encodings {
		switch encodings[i].Encoding {
		case catenumpb.DatumEncoding_ASCENDING_KEY:
			keyEncDirections[i] = catenumpb.IndexColumn_ASC
		case catenumpb.DatumEncoding_DESCENDING_KEY:
			keyEncDirections[i] = catenumpb.IndexColumn_DESC
		default:
			colexecerror.InternalError(errors.Newf("unsupported encoding type %s", encodings[i].Encoding.String()))
		}
		keyOrdinals[i] = int(encodings[i].Column)
	}

	return &TupleRangeDistributor{
		assembler:   NewColSpanAssemblerWithoutTablePrefix(allocator, nil /* fetchSpec */, nil /* splitFamilyIDs */, inputTypes, keyEncDirections, keyOrdinals),
		selections:  make([][]int, numOutputs),
		spans:       spans,
		defaultDest: defaultDest,
	}
}

// Init initializes the TupleRangeDistributor and gets it ready for use. It
// must be called before use.
func (r *TupleRangeDistributor) Init(ctx context.Context) {
	r.cancelChecker.Init(ctx)
}

func (r *TupleRangeDistributor) streamForSpan(span roachpb.Span) int {
	idx := sort.Search(len(r.spans), func(j int) bool {
		return bytes.Compare(r.spans[j].End, span.Key) > 0
	})
	if idx == len(r.spans) {
		return -1
	}
	// Make sure the Start is <= data.
	if bytes.Compare(r.spans[idx].Start, span.EndKey) > 0 {
		return -1
	}
	return int(r.spans[idx].Stream)
}

// Distribute implements the RangeDistributor interface.
func (r *TupleRangeDistributor) Distribute(b coldata.Batch) [][]int {
	n := b.Length()

	r.assembler.ConsumeBatch(b, 0, b.Length())
	spans := r.assembler.GetSpans()

	// Reset selections.
	for i := 0; i < len(r.selections); i++ {
		r.selections[i] = r.selections[i][:0]
	}

	outputIdxs := make([]int, len(spans))
	for i := range spans {
		idx := r.streamForSpan(spans[i])
		if idx == -1 {
			if r.defaultDest == nil {
				colexecerror.ExpectedError(fmt.Errorf("no range destination for span %s", spans[i]))
			}
			idx = int(*r.defaultDest)
		}
		outputIdxs[i] = idx
	}

	// Build a selection vector for each output.
	selection := b.Selection()
	if selection != nil {
		_ = outputIdxs[n-1]
		for i, selIdx := range selection[:n] {
			//gcassert:bce
			outputIdx := outputIdxs[i]
			r.selections[outputIdx] = append(r.selections[outputIdx], selIdx)
		}
	} else {
		_ = outputIdxs[n-1]
		for i := 0; i < n; i++ {
			//gcassert:bce
			outputIdx := outputIdxs[i]
			r.selections[outputIdx] = append(r.selections[outputIdx], i)
		}
	}
	return r.selections
}
