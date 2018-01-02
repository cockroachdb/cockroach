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

package distsqlrun

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// sorter sorts the input rows according to the column ordering specified by ordering. Note
// that this is a no-grouping aggregator and therefore it does not produce a global ordering but
// simply guarantees an intra-stream ordering on the physical output stream.
type sorter struct {
	processorBase

	// input is a row source without metadata; the metadata is directed straight
	// to out.output.
	input NoMetadataRowSource
	// rawInput is the true input, not wrapped in a NoMetadataRowSource.
	rawInput RowSource
	ordering sqlbase.ColumnOrdering
	matchLen uint32
	// count is the maximum number of rows that the sorter will push to the
	// ProcOutputHelper. 0 if the sorter should sort and push all the rows from
	// the input.
	count int64
	// tempStorage is used to store rows when the working set is larger than can
	// be stored in memory.
	tempStorage engine.Engine
}

var _ Processor = &sorter{}

func newSorter(
	flowCtx *FlowCtx, spec *SorterSpec, input RowSource, post *PostProcessSpec, output RowReceiver,
) (*sorter, error) {
	count := int64(0)
	if post.Limit != 0 {
		// The sorter needs to produce Offset + Limit rows. The ProcOutputHelper
		// will discard the first Offset ones.
		count = int64(post.Limit) + int64(post.Offset)
	}
	s := &sorter{
		input:       MakeNoMetadataRowSource(input, output),
		rawInput:    input,
		ordering:    convertToColumnOrdering(spec.OutputOrdering),
		matchLen:    spec.OrderingMatchLen,
		count:       count,
		tempStorage: flowCtx.TempStorage,
	}
	if err := s.init(post, input.OutputTypes(), flowCtx, nil /* evalCtx */, output); err != nil {
		return nil, err
	}
	return s, nil
}

// Run is part of the processor interface.
func (s *sorter) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx := log.WithLogTag(s.flowCtx.Ctx, "Sorter", nil)
	ctx, span := processorSpan(ctx, "sorter")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting sorter run")
		defer log.Infof(ctx, "exiting sorter run")
	}

	var sv memRowContainer
	// Enable fall back to disk if the cluster setting is set or a memory limit
	// has been set through testing.
	st := s.flowCtx.Settings
	useTempStorage := settingUseTempStorageSorts.Get(&st.SV) ||
		s.flowCtx.testingKnobs.MemoryLimitBytes > 0
	rowContainerMon := s.flowCtx.EvalCtx.Mon
	if s.matchLen == 0 && s.count == 0 && useTempStorage {
		// We will use the sortAllStrategy in this case and potentially fall
		// back to disk.
		// Limit the memory use by creating a child monitor with a hard limit.
		// The strategy will overflow to disk if this limit is not enough.
		limit := s.flowCtx.testingKnobs.MemoryLimitBytes
		if limit <= 0 {
			limit = settingWorkMemBytes.Get(&st.SV)
		}
		limitedMon := mon.MakeMonitorInheritWithLimit(
			"sortall-limited", limit, s.flowCtx.EvalCtx.Mon,
		)
		limitedMon.Start(ctx, s.flowCtx.EvalCtx.Mon, mon.BoundAccount{})
		defer limitedMon.Stop(ctx)

		rowContainerMon = &limitedMon
	}
	sv.initWithMon(s.ordering, s.rawInput.OutputTypes(), s.flowCtx.NewEvalCtx(), rowContainerMon)
	// Construct the optimal sorterStrategy.
	var ss sorterStrategy
	if s.matchLen == 0 {
		if s.count == 0 {
			// No specified ordering match length and unspecified limit; no
			// optimizations are possible so we simply load all rows into memory and
			// sort all values in-place. It has a worst-case time complexity of
			// O(n*log(n)) and a worst-case space complexity of O(n).
			ss = newSortAllStrategy(&sv, useTempStorage)
		} else {
			// No specified ordering match length but specified limit; we can optimize
			// our sort procedure by maintaining a max-heap populated with only the
			// smallest k rows seen. It has a worst-case time complexity of
			// O(n*log(k)) and a worst-case space complexity of O(k).
			ss = newSortTopKStrategy(&sv, s.count)
		}
	} else {
		// Ordering match length is specified. We will be able to use existing
		// ordering in order to avoid loading all the rows into memory. If we're
		// scanning an index with a prefix matching an ordering prefix, we can only
		// accumulate values for equal fields in this prefix, sort the accumulated
		// chunk and then output.
		// TODO(irfansharif): Add optimization for case where both ordering match
		// length and limit is specified.
		ss = newSortChunksStrategy(&sv)
	}

	sortErr := ss.Execute(ctx, s)
	if sortErr != nil {
		log.Errorf(ctx, "error sorting rows: %s", sortErr)
	}
	DrainAndClose(ctx, s.out.output, sortErr, s.rawInput)
}
