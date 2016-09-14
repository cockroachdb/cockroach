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

package distsql

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
)

// sorter sorts the input rows according to the column ordering specified by
// 'outputOrdering'. Note that this is a no-grouping aggregator and therefore
// it does not produce a global ordering but simply guarantees an intra-stream
// ordering on the physical output stream.
type sorter struct {
	input          RowSource
	output         RowReceiver
	inputOrdering  sqlbase.ColumnOrdering
	outputOrdering sqlbase.ColumnOrdering
	limit          int64
	unique         bool // true if all rows in input stream are distinct.
	ctx            context.Context
}

var _ processor = &sorter{}

func newSorter(
	flowCtx *FlowCtx,
	spec *SorterSpec,
	input RowSource,
	output RowReceiver,
) *sorter {
	return &sorter{
		input:          input,
		output:         output,
		inputOrdering:  convertToColumnOrdering(spec.InputOrderingInfo.Ordering),
		outputOrdering: convertToColumnOrdering(spec.OutputOrdering),
		limit:          spec.Limit,
		unique:         spec.InputOrderingInfo.Unique,
		ctx:            log.WithLogTag(flowCtx.Context, "Sorter", nil),
	}
}

// Run is part of the processor interface.
func (s *sorter) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	if log.V(2) {
		log.Infof(s.ctx, "starting sorter run")
		defer log.Infof(s.ctx, "exiting sorter run")
	}

	switch {
	case len(s.inputOrdering) == 0 && s.limit == 0:
		// No specified input ordering and unspecified limit, no optimizations
		// possible so we simply load all rows into memory and sort all values
		// in-place. It has a worst-case time complexity of O(n*log(n)) and a
		// worst-case space complexity of O(n).
		ss := newSortAllStrategy(
			&sorterValues{
				ordering: s.outputOrdering,
			})
		err := ss.Execute(s)
		if err != nil {
			log.Errorf(s.ctx, "error sorting rows in memory: %s", err)
		}

		s.output.Close(err)
	case len(s.inputOrdering) == 0:
		// No specified input ordering but specified limit, we can optimize our
		// sort procedure by maintaining a max-heap populated with only the top
		// k rows seen. It has a worst-case time complexity of O(n*log(k)) and
		// a worst-case space complexity of O(k).
		ss := newSortTopKStrategy(
			&sorterValues{
				ordering: s.outputOrdering,
			}, s.limit)
		err := ss.Execute(s)
		if err != nil {
			log.Errorf(s.ctx, "error sorting rows: %s", err)
		}

		s.output.Close(err)
	case len(s.inputOrdering) != 0 && s.limit == 0:
		// Input ordering is specified, we may be able to use existing ordering
		// in order to avoid loading all the rows into memory. If we're
		// scanning an index with a prefix matching an ordering prefix, we can
		// only accumulate values for equal fields in this prefix, sort the
		// accumulated chunk and then output.
		ss := newSortChunksStrategy(
			&sorterValues{
				ordering: s.outputOrdering,
			}, computeOrderingMatch(s.outputOrdering, s.inputOrdering, s.unique),
		)
		err := ss.Execute(s)
		if err != nil {
			log.Errorf(s.ctx, "error sorting rows: %s", err)
		}

		s.output.Close(err)
	default:
		// TODO(irfansharif): Add optimization for case where both input
		// ordering and limit is specified.
		panic("optimization no implemented yet")
	}
}

// Computes how long of a prefix of a desired ColumnOrdering is matched by an
// existing ordering.
//
// Returns a value between 0 and len(desired).
func computeOrderingMatch(
	desired sqlbase.ColumnOrdering,
	existing sqlbase.ColumnOrdering,
	unique bool,
) int {
	pos := 0
	for i, col := range desired {
		if pos < len(existing) {
			ci := existing[pos]

			// Check that the next column matches. Note: "!=" acts as logical XOR.
			if ci.ColIdx == col.ColIdx && (ci.Direction == col.Direction) {
				pos++
				continue
			}
		} else if unique {
			// Everything matched up to the last column and we know there are
			// no duplicate combinations of values for these columns. Any other
			// columns we may want to "refine" the ordering by don't make a
			// difference.
			return len(desired)
		}

		// Everything matched up to this point.
		return i
	}

	// Everything matched!
	return len(desired)
}
