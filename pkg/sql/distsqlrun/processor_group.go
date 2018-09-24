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

	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// ProcessorGroup is a group of RowSource-implementing processors that are all
// run in the same goroutine.
type ProcessorGroup struct {
	// root is the RowSource that needs to be called to execute this group of
	// processors. Although processors can have multiple inputs, they only have
	// one output. Therefore, there will only ever be one root.
	root RowSource
	// pIdx is the index of the root.
	pIdx int
	// Since each processor group is run in its own goroutine through Run, it
	// needs to push rows from the root into this RowReceiver.
	output     RowReceiver
	processors []RowSource
}

var _ RowSource = ProcessorGroup{}

var ErrProcNotSupported = errors.New("processor not supported")

func (f *Flow) ProcessorSpecsToProcessorGroups(ctx context.Context, pspecs []ProcessorSpec) ([]Processor, error) {
	pgs, err := f.processorSpecsToProcessorGroups(ctx, pspecs)
	if err != nil {
		// Reset the flow to allow a fall back to non-processor group flow setup.
		f.inboundStreams = make(map[StreamID]*inboundStreamInfo)
		f.localStreams = make(map[StreamID]RowReceiver)
		f.startables = f.startables[0:]
		return nil, err
	}
	return pgs, nil
}

// newProcForProcGroup creates a new processor, omitting the creation of the
// output unless it will be external to the procesor group (i.e. running in its
// own goroutine). If an output is created, it is returned.
func (f *Flow) newProcForProcGroup(ctx context.Context, spec ProcessorSpec, inputs []RowSource) (Processor, RowReceiver, error) {
	ospec := spec.Output[0]

	if ospec.Type == OutputRouterSpec_PASS_THROUGH &&
		len(ospec.Streams) == 1 &&
		ospec.Streams[0].Type == StreamEndpointSpec_LOCAL {
		proc, err := newProcessor(ctx, &f.FlowCtx, spec.ProcessorID, &spec.Core, &spec.Post, inputs, []RowReceiver{nil}, f.localProcessors)
		return proc, nil, err
	}

	// Quick exit for unsupported routers. In practice, only ccl code uses other
	// types.
	// TODO(asubiotto): Currently non-hash routers sometimes have a single local
	// output stream. These can be elided for a pass-through router. The issue is
	// that we go into a setupRouter case which expects the router's outputs to be
	// set up. Since we only set up inputs for multiple or non-local streams,
	// these have not been set up (since they are indistinguishable from reading
	// from a RowSource proc). The alternative is to create RowChannels for all
	// inputs, regardless of if we fuse or not, which is wasteful.
	if ospec.Type != OutputRouterSpec_BY_HASH && ospec.Type != OutputRouterSpec_PASS_THROUGH {
		return nil, nil, ErrProcNotSupported
	}

	// makeProcessor takes care of setting up outputs. This is the case in
	// which we hit a processor that will be the root of a processor group.
	return f.makeProcessor(ctx, &spec, inputs)
}

// ProcessorSpecsToProcessorGroups is the helper function that converts a slice
// of ProcessorSpecs into one or more ProcessorGroups that should be run in
// their own goroutines.
func (f *Flow) processorSpecsToProcessorGroups(ctx context.Context, pspecs []ProcessorSpec) ([]Processor, error) {
	var (
		// rootProcessorGroup is a standalone variable because it cannot be added
		// to the processorGroups map due to the response stream having an ID of 0,
		// which will clash with the actual stream of ID 0.
		rootProcessorGroup ProcessorGroup
		processorGroups    = make(map[StreamID]ProcessorGroup)
		// edgesMap maps an input stream id to a processor index.
		edgesMap     = make(map[StreamID]int)
		pIdxToInputs = make(map[int][]RowSource, len(pspecs))
	)
	for pIdx, ps := range pspecs {
		if len(ps.Output) != 1 {
			return nil, ErrProcNotSupported
		}

		var inputs []RowSource

		// Add proc as a new processor group if it has no incident edges, otherwise
		// create a mapping from its input stream IDs to the processor index.
		numInputs := len(ps.Input)
		if numInputs > 0 {
			// If we have inputs, we consider them to not have incident edges if one
			// of the inputs has multiple streams or if any of the inbound streams are
			// remote.
			for _, is := range ps.Input {
				if len(is.Streams) > 1 || is.Streams[0].Type != StreamEndpointSpec_LOCAL {
					inputs = make([]RowSource, numInputs)
					break
				}
			}
			if inputs == nil {
				// This proc should not be created yet as it has local incident edges
				// and we want to connect these as RowSources.
				// Add it to the edges map to be set up later by matching the outputs of
				// processorGroups.
				for _, is := range ps.Input {
					for _, s := range is.Streams {
						edgesMap[s.StreamID] = pIdx
					}
				}
				// Move on to the next processor.
				continue
			} else {
				// TODO(asubiotto): Once processors do not know about metadata, we will
				// insert a shim that accumulates metadata in the processor group rather
				// than pass it to the processor.
				for i, is := range ps.Input {
					sync, err := f.setupInputSync(ctx, is)
					if err != nil {
						return nil, err
					}
					inputs[i] = sync
				}
			}
		}
		// Nil inputs at this stage signify a zero-input processor.
		pIdxToInputs[pIdx] = inputs
	}

	// We need a second loop to instantiate the processors. Otherwise, we could
	// have cases where we hit a processor that needs to instantiate an output
	// router, but the input streams have not been set up.
	for pIdx, inputs := range pIdxToInputs {
		ps := pspecs[pIdx]
		proc, out, err := f.newProcForProcGroup(ctx, ps, inputs)
		if err != nil {
			return nil, err
		}

		rs, ok := proc.(RowSource)
		if !ok {
			return nil, ErrProcNotSupported
		}
		// If we have multiple streams, the output ID will not match any of the
		// inputs, but we still want to add a processor group, so just use the first
		// stream output ID.
		ospec := ps.Output[0]
		outputID := ospec.Streams[0].StreamID
		// A response output spec will only ever have one stream, so it is
		// sufficient to check only the first stream.
		pg := ProcessorGroup{
			root:       rs,
			pIdx:       pIdx,
			output:     out,
			processors: []RowSource{rs},
		}
		if ospec.Streams[0].Type == StreamEndpointSpec_SYNC_RESPONSE {
			rootProcessorGroup = pg
		} else {
			processorGroups[outputID] = pg
		}
	}

	// Add 1 to previousNumGroups for the illusion of progress for the first
	// iteration.
	previousUnconnectedEdges := len(edgesMap) + 1
	for len(edgesMap) < previousUnconnectedEdges {
		previousUnconnectedEdges = len(edgesMap)
		for outputID := range processorGroups {
			pIdx, ok := edgesMap[outputID]
			if !ok {
				// This processor group has been completed.
				continue
			}
			pspec := pspecs[pIdx]
			inputs := make([]RowSource, len(pspec.Input))
			// mergeGroups are the processor groups that we need to merge into one.
			// These are populated when a processor spec has multiple local inputs
			// and one output.
			mergeGroups := make([]ProcessorGroup, len(pspec.Input))
			for i, is := range pspec.Input {
				// Existence in the edges map implies only one stream per input.
				if len(is.Streams) != 1 {
					return nil, errors.New("invalid number of streams for local input")
				}

				inputID := is.Streams[0].StreamID
				var ok bool
				mergeGroups[i], ok = processorGroups[inputID]
				if !ok {
					// TODO(asubiotto): This means that the processor group we want to
					// merge hasn't been added yet.
					inputs = nil
					break
				}
				inputs[i] = processorGroups[inputID].root
				delete(processorGroups, inputID)
				// Delete from edgesMap as this stream has been connected.
				delete(edgesMap, inputID)
			}

			if inputs == nil {
				// The processor group we want to merge hasn't been added yet. Have to
				// be stronger about topo sorting. we want a map though. Do we want a map?
				continue
			}

			proc, out, err := f.newProcForProcGroup(ctx, pspec, inputs)
			if err != nil {
				return nil, err
			}

			rs, ok := proc.(RowSource)
			if !ok {
				return nil, ErrProcNotSupported
			}
			// TODO(asubiotto): Is this right? What about a local flow that then goes
			// into a hashRouter?
			if len(pspec.Output[0].Streams) != 1 {
				return nil, errors.New("unexpected number of output streams")
			}
			// Merge multiple processor groups into one.
			mg := mergeGroups[0]
			for _, mergeGroup := range mergeGroups[1:] {
				mg.processors = append(mg.processors, mergeGroup.processors...)
			}
			mg.root = rs
			mg.pIdx = pIdx
			mg.output = out
			ospec := pspec.Output[0]
			if ospec.Streams[0].Type == StreamEndpointSpec_SYNC_RESPONSE {
				rootProcessorGroup = mg
			} else {
				processorGroups[ospec.Streams[0].StreamID] = mg
			}
		}
	}
	// TODO(asubiotto): This happens sometimes which is weird because I expect
	// forward progress to be made on each iteration. Maybe this is a symptom of
	// a larger problem.
	if len(edgesMap) > 0 {
		// TODO(asubiotto): More information in error msg.
		return nil, errors.New("failed to connect inputs")
	}

	result := make([]Processor, 0, len(processorGroups)+1)
	if rootProcessorGroup.root != nil {
		if rootProcessorGroup.output == nil {
			// Only pass through routers are not set up at this points.
			// TODO(asubiotto): Explain this better.
			output, err := f.setupOutboundStream(
				pspecs[rootProcessorGroup.pIdx].Output[0].Streams[0],
			)
			if err != nil {
				return nil, err
			}
			rootProcessorGroup.output = &copyingRowReceiver{RowReceiver: output}
		}
		// This is the case when we do not have a processor with a SYNC_RESPONSE
		// stream output type i.e. this flow is being planned on a remote node.
		result = append(result, rootProcessorGroup)
	}
	for _, pg := range processorGroups {
		if pg.output == nil {
			// TODO(asubiotto): Getting some ordering issues in TestClusterFlow. Why?
			output, err := f.setupOutboundStream(
				pspecs[pg.pIdx].Output[0].Streams[0],
			)
			if err != nil {
				return nil, err
			}
			pg.output = &copyingRowReceiver{RowReceiver: output}
		}
		result = append(result, pg)
	}

	// TODO(asubiotto): Idea, sanity check localStreams, those should have all
	// been connected.
	return result, nil
}

func (pg ProcessorGroup) OutputTypes() []sqlbase.ColumnType {
	return pg.root.OutputTypes()
}

func (pg ProcessorGroup) Start(ctx context.Context) context.Context {
	return pg.root.Start(ctx)
}

func (pg ProcessorGroup) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	return pg.root.Next()
}

// ConsumerDone is called when no more rows are needed, but metadata is.
func (pg ProcessorGroup) ConsumerDone() {
	pg.root.ConsumerDone()
}

// ConsumerClosed is called when no more rows or metadata is needed.
func (pg ProcessorGroup) ConsumerClosed() {
	pg.root.ConsumerClosed()
}

func (pg ProcessorGroup) Run(ctx context.Context, wg *sync.WaitGroup) {
	ctx = pg.root.Start(ctx)
	Run(ctx, pg, pg.output)
	if wg != nil {
		wg.Done()
	}
}
