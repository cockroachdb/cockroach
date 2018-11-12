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

	"github.com/cockroachdb/cockroach/pkg/util/log"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/exec"
)

func checkNumIn(inputs []exec.Operator, numIn int) error {
	if len(inputs) != numIn {
		return errors.Errorf("expected %d input(s), got %d", numIn, len(inputs))
	}
	return nil
}

func newColOperator(
	ctx context.Context, flowCtx *FlowCtx, spec *ProcessorSpec, inputs []exec.Operator,
) (exec.Operator, error) {
	core := &spec.Core
	post := &spec.Post
	var err error
	var op exec.Operator
	switch {
	case core.TableReader != nil:
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		op, err = newColBatchScan(flowCtx, core.TableReader, post)
	case core.Aggregator != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		spec := core.Aggregator
		if len(spec.GroupCols) == 0 &&
			len(spec.Aggregations) == 1 &&
			spec.Aggregations[0].FilterColIdx == nil &&
			spec.Aggregations[0].Func == AggregatorSpec_COUNT_ROWS &&
			!spec.Aggregations[0].Distinct {
			return exec.NewCountOp(inputs[0]), nil
		}
		return nil, errors.Errorf("unsupported aggregator %+v", core.Aggregator)

	case core.Distinct != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}

		var distinctCols, orderedCols util.FastIntSet
		allSorted := true

		for _, col := range core.Distinct.OrderedColumns {
			orderedCols.Add(int(col))
		}
		for _, col := range core.Distinct.DistinctColumns {
			if !orderedCols.Contains(int(col)) {
				allSorted = false
			}
			distinctCols.Add(int(col))
		}
		if !orderedCols.SubsetOf(distinctCols) {
			return nil, pgerror.NewAssertionErrorf("ordered cols must be a subset of distinct cols")
		}
		if !allSorted {
			return nil, errors.New("unsorted distinct not supported")
		}

		typs := types.FromColumnTypes(spec.Input[0].ColumnTypes)
		op, err = exec.NewOrderedDistinct(inputs[0], core.Distinct.OrderedColumns, typs)

	default:
		return nil, errors.Errorf("unsupported processor core %s", core)
	}
	log.VEventf(ctx, 1, "Made op %T\n", op)

	if err != nil {
		return nil, err
	}

	if !post.Filter.Empty() {
		// TODO(solon): plan selection op
		return nil, errors.New("filters unsupported")
	}
	if post.Projection {
		op = exec.NewSimpleProjectOp(op, post.OutputColumns)
	} else if post.RenderExprs != nil {
		// TODO(solon): plan renders
		return nil, errors.New("renders unsupported")
	}
	if post.Offset != 0 {
		return nil, errors.New("offset unsupported")
	}
	if post.Limit != 0 {
		op = exec.NewLimitOp(op, post.Limit)
	}
	return op, nil
}

func (f *Flow) setupVectorized(ctx context.Context) error {
	f.processors = make([]Processor, 1)

	streamIDToInputOp := make(map[StreamID]exec.Operator)
	streamIDToSpecIdx := make(map[StreamID]int)
	// queue is a queue of indices into f.spec.Processors, for topologically
	// ordered processing.
	queue := make([]int, 0, len(f.spec.Processors))
	for i := range f.spec.Processors {
		if len(f.spec.Processors[i].Input) == 0 {
			// Queue all procs with no inputs.
			queue = append(queue, i)
		}
		for j := range f.spec.Processors[i].Input {
			input := &f.spec.Processors[i].Input[j]
			for k := range input.Streams {
				if input.Streams[k].Type == StreamEndpointSpec_LOCAL {
					id := input.Streams[k].StreamID
					streamIDToSpecIdx[id] = i
				} else {
					return errors.Errorf("unsupported input stream type %s", input.Streams[k].Type)
				}
			}
		}
	}

	inputs := make([]exec.Operator, 0, 2)
	for len(queue) > 0 {
		pspec := &f.spec.Processors[queue[0]]
		queue = queue[1:]
		if len(pspec.Output) > 1 {
			return errors.Errorf("unsupported multi-output proc (%d outputs)", len(pspec.Output))
		}
		output := pspec.Output[0]
		if output.Type != OutputRouterSpec_PASS_THROUGH {
			return errors.Errorf("unsupported routed proc %s", output.Type)
		}
		if len(output.Streams) != 1 {
			return errors.Errorf("unsupported multi outputstream proc (%d streams)", len(output.Streams))
		}
		inputs = inputs[:0]
		for i := range pspec.Input {
			input := &pspec.Input[i]
			if len(input.Streams) > 1 {
				return errors.Errorf("unsupported multi inputstream proc (%d streams)", len(input.Streams))
			}
			inputStream := &input.Streams[0]
			if inputStream.Type != StreamEndpointSpec_LOCAL {
				return errors.Errorf("unsupported input stream type %s", inputStream.Type)
			}
			inputs = append(inputs, streamIDToInputOp[inputStream.StreamID])
		}

		op, err := newColOperator(ctx, &f.FlowCtx, pspec, inputs)
		if err != nil {
			return err
		}

		outputStream := output.Streams[0]
		switch outputStream.Type {
		case StreamEndpointSpec_LOCAL:
		case StreamEndpointSpec_SYNC_RESPONSE:
			// Make the materializer, which will write to the given receiver.
			columnTypes := f.syncFlowConsumer.Types()
			outputToInputColIdx := make([]int, len(columnTypes))
			for i := range outputToInputColIdx {
				outputToInputColIdx[i] = i
			}
			proc, err := newMaterializer(&f.FlowCtx, pspec.ProcessorID, op, columnTypes, outputToInputColIdx, &PostProcessSpec{}, f.syncFlowConsumer)
			if err != nil {
				return err
			}
			f.processors[0] = proc
		default:
			return errors.Errorf("unsupported output stream type %s", outputStream.Type)
		}

		streamIDToInputOp[outputStream.StreamID] = op

		// Now queue all outputs from this op whose inputs are already all
		// populated.
	NEXTOUTPUT:
		for i := range pspec.Output {
			for j := range pspec.Output[i].Streams {
				stream := &pspec.Output[i].Streams[j]
				if stream.Type != StreamEndpointSpec_LOCAL {
					continue
				}
				procIdx, ok := streamIDToSpecIdx[stream.StreamID]
				if !ok {
					return errors.Errorf("Couldn't find stream %d", stream.StreamID)
				}
				outputSpec := &f.spec.Processors[procIdx]
				for k := range outputSpec.Input {
					for l := range outputSpec.Input[k].Streams {
						id := outputSpec.Input[k].Streams[l].StreamID
						if _, ok := streamIDToInputOp[id]; !ok {
							continue NEXTOUTPUT
						}
					}
				}
				// We found an input op for every single stream in this output. Queue
				// it for processing.
				queue = append(queue, procIdx)
			}
		}
	}
	return nil
}
