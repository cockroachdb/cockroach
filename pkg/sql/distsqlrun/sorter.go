// Copyright 2016 The Cockroach Authors.
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
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsqlrun

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// sorter sorts the input rows according to the column ordering specified by ordering. Note
// that this is a no-grouping aggregator and therefore it does not produce a global ordering but
// simply guarantees an intra-stream ordering on the physical output stream.
type sorter struct {
	// input is a row source without metadata; the metadata is directed straight
	// to out.output.
	input NoMetadataRowSource
	// rawInput is the true input, not wrapped in a NoMetadataRowSource.
	rawInput RowSource
	out      procOutputHelper
	ordering sqlbase.ColumnOrdering
	matchLen uint32
	limit    int64
}

var _ processor = &sorter{}

func newSorter(
	flowCtx *FlowCtx, spec *SorterSpec, input RowSource, post *PostProcessSpec, output RowReceiver,
) (*sorter, error) {
	s := &sorter{
		input:    MakeNoMetadataRowSource(input, output),
		rawInput: input,
		ordering: convertToColumnOrdering(spec.OutputOrdering),
		matchLen: spec.OrderingMatchLen,
		limit:    spec.Limit,
	}
	if err := s.out.init(post, input.Types(), &flowCtx.evalCtx, output); err != nil {
		return nil, err
	}
	return s, nil
}

// Run is part of the processor interface.
func (s *sorter) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTag(ctx, "Sorter", nil)
	ctx, span := tracing.ChildSpan(ctx, "sorter")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting sorter run")
		defer log.Infof(ctx, "exiting sorter run")
	}

	// Construct the optimal sorterStrategy.
	var ss sorterStrategy
	switch {
	case s.matchLen == 0 && s.limit == 0:
		// No specified ordering match length and unspecified limit, no optimizations possible so we
		// simply load all rows into memory and sort all values in-place. It has a worst-case time
		// complexity of O(n*log(n)) and a worst-case space complexity of O(n).
		ss = newSortAllStrategy(
			&sorterValues{
				ordering: s.ordering,
			})
	case s.matchLen == 0:
		// No specified ordering match length but specified limit, we can optimize our sort procedure by
		// maintaining a max-heap populated with only the smallest k rows seen. It has a worst-case time
		// complexity of O(n*log(k)) and a worst-case space complexity of O(k).
		ss = newSortTopKStrategy(
			&sorterValues{
				ordering: s.ordering,
			}, s.limit)
	case s.matchLen != 0:
		// Ordering match length is specified, but no specified limit. We will be able to use
		// existing ordering in order to avoid loading all the rows into memory. If we're scanning
		// an index with a prefix matching an ordering prefix, we can only accumulate values for
		// equal fields in this prefix, sort the accumulated chunk and then output.
		ss = newSortChunksStrategy(
			&sorterValues{
				ordering: s.ordering,
			})

	default:
		// TODO(irfansharif): Add optimization for case where both ordering match length and limit is
		// specified.
		panic("optimizationt no implemented yet")
	}

	sortErr := ss.Execute(ctx, s)
	if sortErr != nil {
		log.Errorf(ctx, "error sorting rows: %s", sortErr)
	}
	DrainAndClose(ctx, s.out.output, sortErr, s.rawInput)
}
