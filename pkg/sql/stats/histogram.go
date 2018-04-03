// Copyright 2017 The Cockroach Authors.
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
//
// Data structures and basic infrastructure for distributed SQL APIs. See
// docs/RFCS/distributed_sql.md.
// All the concepts here are "physical plan" concepts.

package stats

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

// EquiDepthHistogram creates a histogram where each bucket contains roughly the
// same number of samples (though it can vary when a boundary value has high
// frequency).
//
// numRows is the total number of rows from which values were sampled.
func EquiDepthHistogram(
	evalCtx *tree.EvalContext, samples tree.Datums, numRows int64, maxBuckets int,
) (HistogramData, error) {
	numSamples := len(samples)
	if numSamples == 0 {
		return HistogramData{}, errors.Errorf("no samples")
	}
	if numRows < int64(numSamples) {
		return HistogramData{}, errors.Errorf("more samples than rows")
	}
	for _, d := range samples {
		if d == tree.DNull {
			return HistogramData{}, errors.Errorf("NULL values not allowed in histogram")
		}
	}
	sort.Slice(samples, func(i, j int) bool {
		return samples[i].Compare(evalCtx, samples[j]) < 0
	})
	numBuckets := maxBuckets
	if maxBuckets > numSamples {
		numBuckets = numSamples
	}
	h := HistogramData{
		Buckets: make([]HistogramData_Bucket, 0, numBuckets),
	}
	var err error
	h.ColumnType, err = sqlbase.DatumTypeToColumnType(samples[0].ResolvedType())
	if err != nil {
		return HistogramData{}, err
	}
	// i keeps track of the current sample and advances as we form buckets.
	for i, b := 0, 0; b < numBuckets && i < numSamples; b++ {
		// num is the number of samples in this bucket.
		num := (numSamples - i) / (numBuckets - b)
		if num < 1 {
			num = 1
		}
		upper := samples[i+num-1]
		// numLess is the number of samples less than upper (in this bucket).
		numLess := 0
		for ; numLess < num-1; numLess++ {
			if c := samples[i+numLess].Compare(evalCtx, upper); c == 0 {
				break
			} else if c > 0 {
				panic("samples not sorted")
			}
		}
		// Advance the boundary of the bucket to cover all samples equal to upper.
		for ; i+num < numSamples; num++ {
			if samples[i+num].Compare(evalCtx, upper) != 0 {
				break
			}
		}
		encoded, err := sqlbase.EncodeTableKey(nil, upper, encoding.Ascending)
		if err != nil {
			return HistogramData{}, err
		}
		i += num
		h.Buckets = append(h.Buckets, HistogramData_Bucket{
			NumEq:      int64(num-numLess) * numRows / int64(numSamples),
			NumRange:   int64(numLess) * numRows / int64(numSamples),
			UpperBound: encoded,
		})
	}
	return h, nil
}
