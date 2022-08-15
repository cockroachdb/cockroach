// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	gojson "encoding/json"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

type rangeStatsOperator struct {
	colexecop.OneInputHelper
	fetcher     eval.RangeStatsFetcher
	allocator   *colmem.Allocator
	argumentCol int
	outputIdx   int
}

var _ colexecop.Operator = (*rangeStatsOperator)(nil)

// newRangeStatsOperator constructs a vectorized operator to fetch range
// statistics. Importantly, this operator issues RangeStatsRequests in
// parallel to help amortize the latency required to issue the requests
// to nodes in remote regions.
func newRangeStatsOperator(
	fetcher eval.RangeStatsFetcher,
	allocator *colmem.Allocator,
	argumentCol int,
	outputIdx int,
	input colexecop.Operator,
) (colexecop.Operator, error) {
	return &rangeStatsOperator{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		allocator:      allocator,
		argumentCol:    argumentCol,
		outputIdx:      outputIdx,
		fetcher:        fetcher,
	}, nil
}

// TODO(ajwerner): Generalize this operator to deal with other very similar
// builtins like crdb_internal.leaseholder. Some of the places which retrieve
// range statistics also call that function; optimizing one without the other
// is pointless, and they are very similar.

func (r *rangeStatsOperator) Next() coldata.Batch {
	// Naively take the input batch and use it to define the batch size.
	//
	// TODO(ajwerner): As a first step towards being more sophisticated,
	// this code could accumulate up to some minimum batch size before
	// sending the first batch.
	batch := r.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	inSel := batch.Selection()
	inVec := batch.ColVec(r.argumentCol)
	inBytes := inVec.Bytes()

	output := batch.ColVec(r.outputIdx)
	jsonOutput := output.JSON()
	r.allocator.PerformOperation(
		[]coldata.Vec{output},
		func() {
			keys := make([]roachpb.Key, 0, batch.Length())
			if inSel == nil {
				for i := 0; i < batch.Length(); i++ {
					keys = append(keys, inBytes.Get(i))
				}
			} else {
				for _, idx := range inSel {
					keys = append(keys, inBytes.Get(idx))
				}
			}
			// TODO(ajwerner): Reserve memory for the responses. We know they'll
			// at least, on average, contain keys so it'll be 2x the size of the
			// keys plus some constant multiple.
			res, err := r.fetcher.RangeStats(r.Ctx, keys...)
			if err != nil {
				colexecerror.ExpectedError(err)
			}
			readResponse := func(resultIndex, outputIndex int) {
				jsonStr, err := gojson.Marshal(&res[resultIndex].MVCCStats)
				if err != nil {
					colexecerror.ExpectedError(err)
				}
				jsonDatum, err := json.ParseJSON(string(jsonStr))
				if err != nil {
					colexecerror.ExpectedError(err)
				}
				jsonOutput.Set(outputIndex, jsonDatum)
			}
			if inSel != nil {
				for i, s := range inSel {
					readResponse(i, s)
				}
			} else {
				for i := range res {
					readResponse(i, i)
				}
			}
		})
	return batch
}
