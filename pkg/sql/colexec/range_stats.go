// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	gojson "encoding/json"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

type rangeStatsOperator struct {
	colexecop.OneInputHelper
	fetcher     eval.RangeStatsFetcher
	allocator   *colmem.Allocator
	argumentCol int
	outputIdx   int
	// withErrors defines if the operator includes any encountered errors in the
	// returned JSON struct. If true, these errors will not fail the query.
	withErrors bool
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
	withErrors bool,
) (colexecop.Operator, error) {
	return &rangeStatsOperator{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		allocator:      allocator,
		argumentCol:    argumentCol,
		outputIdx:      outputIdx,
		fetcher:        fetcher,
		withErrors:     withErrors,
	}, nil
}

// TODO(ajwerner): Generalize this operator to deal with other very similar
// builtins like crdb_internal.leaseholder. Some of the places which retrieve
// range statistics also call that function; optimizing one without the other
// is pointless, and they are very similar.

// appendKey appends the ith value from the inBytes vector to the given keys
// and returns the updated slice. A copy of the ith value is made since these
// keys then go into the KV layer, and we are not allowed to modify them after
// (which we will do with the inBytes vector since it is reset and reused). We
// can avoid making this copy once #75452 is resolved.
func appendKey(keys []roachpb.Key, inBytes *coldata.Bytes, i int) []roachpb.Key {
	origKey := inBytes.Get(i)
	keyCopy := make([]byte, len(origKey))
	copy(keyCopy, origKey)
	return append(keys, keyCopy)
}

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
	inNulls := inVec.Nulls()

	output := batch.ColVec(r.outputIdx)
	jsonOutput := output.JSON()
	r.allocator.PerformOperation(
		[]*coldata.Vec{output},
		func() {
			// TODO(yuzefovich): consider reusing these slices across
			// iterations.
			keys := make([]roachpb.Key, 0, batch.Length())
			// keysOutputIdx is a 1-to-1 mapping with keys which stores the
			// position in the output vector that the results of the range stats
			// lookup for the corresponding key should be put into.
			keysOutputIdx := make([]int, 0, batch.Length())
			if inSel == nil {
				for i := 0; i < batch.Length(); i++ {
					if inNulls.MaybeHasNulls() && inNulls.NullAt(i) {
						// Skip all NULL keys.
						continue
					}
					keys = appendKey(keys, inBytes, i)
					keysOutputIdx = append(keysOutputIdx, i)
				}
			} else {
				for _, idx := range inSel[:batch.Length()] {
					if inNulls.MaybeHasNulls() && inNulls.NullAt(idx) {
						// Skip all NULL keys.
						continue
					}
					keys = appendKey(keys, inBytes, idx)
					keysOutputIdx = append(keysOutputIdx, idx)
				}
			}
			if inNulls.MaybeHasNulls() {
				output.Nulls().Copy(inNulls)
			}
			if len(keys) == 0 {
				// All keys were NULL.
				return
			}
			// TODO(ajwerner): Reserve memory for the responses. We know they'll
			// at least, on average, contain keys so it'll be 2x the size of the
			// keys plus some constant multiple.
			// TODO(yuzefovich): add unit tests that use the RunTests test
			// harness.
			res, rangeStatsErr := r.fetcher.RangeStats(r.Ctx, keys...)
			if rangeStatsErr != nil && !r.withErrors {
				colexecerror.ExpectedError(rangeStatsErr)
			}
			if len(res) != len(keys) && !r.withErrors {
				colexecerror.InternalError(
					errors.AssertionFailedf(
						"unexpected number of RangeStats responses %d: %d expected", len(res), len(keys),
					),
				)
			}
			for i, outputIdx := range keysOutputIdx {
				rswe := &rangeStatsWithErrors{}
				if rangeStatsErr != nil {
					rswe.Error = rangeStatsErr.Error()
				}
				// Not all keys from the keysOutputIdx are guaranteed to be
				// present in res (e.g. some may be missing if there were errors
				// in fetcher.RangeStats and r.withErrors = true).
				if i < len(res) {
					rswe.RangeStats = &res[i].MVCCStats
				}
				var jsonStr []byte
				var err error
				if r.withErrors {
					jsonStr, err = gojson.Marshal(rswe)
				} else {
					jsonStr, err = gojson.Marshal(&res[i].MVCCStats)
				}

				if err != nil {
					colexecerror.ExpectedError(err)
				}
				jsonDatum, err := json.ParseJSON(string(jsonStr))
				if err != nil {
					colexecerror.ExpectedError(err)
				}
				jsonOutput.Set(outputIdx, jsonDatum)
			}
		},
	)
	return batch
}

type rangeStatsWithErrors struct {
	RangeStats *enginepb.MVCCStats
	Error      string
}
