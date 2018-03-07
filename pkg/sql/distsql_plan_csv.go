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

package sql

import (
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// DistLoader uses DistSQL to convert external data formats (csv, etc) into
// sstables of our mvcc-format key values.
type DistLoader struct {
	distSQLPlanner *DistSQLPlanner
}

// RowResultWriter is a thin wrapper around a RowContainer.
type RowResultWriter struct {
	rowContainer *sqlbase.RowContainer
	rowsAffected int
	err          error
}

var _ rowResultWriter = &RowResultWriter{}

// NewRowResultWriter creates a new RowResultWriter.
func NewRowResultWriter(rowContainer *sqlbase.RowContainer) *RowResultWriter {
	return &RowResultWriter{rowContainer: rowContainer}
}

// IncrementRowsAffected implements the rowResultWriter interface.
func (b *RowResultWriter) IncrementRowsAffected(n int) {
	b.rowsAffected += n
}

// AddRow implements the rowResultWriter interface.
func (b *RowResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	_, err := b.rowContainer.AddRow(ctx, row)
	return err
}

// SetError is part of the rowResultWriter interface.
func (b *RowResultWriter) SetError(err error) {
	b.err = err
}

// Err is part of the rowResultWriter interface.
func (b *RowResultWriter) Err() error {
	return b.err
}

// callbackResultWriter is a rowResultWriter that runs a callback function
// on AddRow.
type callbackResultWriter struct {
	fn  func(ctx context.Context, row tree.Datums) error
	err error
}

var _ rowResultWriter = &callbackResultWriter{}

// makeCallbackResultWriter creates a new callbackResultWriter.
func newCallbackResultWriter(
	fn func(ctx context.Context, row tree.Datums) error,
) callbackResultWriter {
	return callbackResultWriter{fn: fn}
}

func (*callbackResultWriter) IncrementRowsAffected(n int) {
	panic("IncrementRowsAffected not supported by callbackResultWriter")
}

func (c *callbackResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	return c.fn(ctx, row)
}

func (c *callbackResultWriter) SetError(err error) {
	c.err = err
}

func (c *callbackResultWriter) Err() error {
	return c.err
}

var colTypeBytes = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}

// LoadCSV performs a distributed transformation of the CSV files at from
// and stores them in enterprise backup format at to.
func (l *DistLoader) LoadCSV(
	ctx context.Context,
	job *jobs.Job,
	db *client.DB,
	evalCtx *extendedEvalContext,
	thisNode roachpb.NodeID,
	nodes []roachpb.NodeDescriptor,
	resultRows *RowResultWriter,
	tableDesc *sqlbase.TableDescriptor,
	from []string,
	to string,
	comma, comment rune,
	skip uint32,
	nullif *string,
	walltime int64,
	splitSize int64,
) error {
	ctx = log.WithLogTag(ctx, "import-distsql", nil)

	// Setup common to both stages.

	// For each input file, assign it to a node.
	csvSpecs := make([]*distsqlrun.ReadCSVSpec, 0, len(nodes))
	for i, input := range from {
		// Round robin assign CSV files to nodes. Files 0 through len(nodes)-1
		// creates the spec. Future files just add themselves to the Uris.
		if i < len(nodes) {
			csvSpecs = append(csvSpecs, &distsqlrun.ReadCSVSpec{
				TableDesc: *tableDesc,
				Options: roachpb.CSVOptions{
					Comma:   comma,
					Comment: comment,
					Nullif:  nullif,
					Skip:    skip,
				},
				Progress: distsqlrun.JobProgress{
					JobID: *job.ID(),
					Slot:  int32(i),
				},
				Uri: make(map[int32]string),
			})
		}
		n := i % len(nodes)
		csvSpecs[n].Uri[int32(i)] = input
	}

	for _, rcs := range csvSpecs {
		// TODO(mjibson): using the actual file sizes here would improve progress
		// accuracy.
		rcs.Progress.Contribution = float32(len(rcs.Uri)) / float32(len(from))
	}

	sstSpecs := make([]distsqlrun.SSTWriterSpec, len(nodes))
	for i := range nodes {
		sstSpecs[i] = distsqlrun.SSTWriterSpec{
			Destination:   to,
			WalltimeNanos: walltime,
		}
	}

	planCtx := l.distSQLPlanner.newPlanningCtx(ctx, evalCtx, nil /* txn */)
	// Because we're not going through the normal pathways, we have to set up
	// the nodeID -> nodeAddress map ourselves.
	for _, node := range nodes {
		planCtx.nodeAddresses[node.NodeID] = node.Address.String()
	}

	// Determine if we need to run the sampling plan or not.

	details := job.Record.Details.(jobs.ImportDetails)
	samples := details.Tables[0].Samples
	if samples == nil {
		var err error
		samples, err = l.loadCSVSamplingPlan(ctx, job, db, evalCtx, thisNode, nodes, from, splitSize, &planCtx, csvSpecs, sstSpecs)
		if err != nil {
			return err
		}
	}

	// Add the table keys to the spans.
	tableSpan := tableDesc.TableSpan()
	splits := make([][]byte, 0, 2+len(samples))
	splits = append(splits, tableSpan.Key)
	splits = append(splits, samples...)
	splits = append(splits, tableSpan.EndKey)

	// jobSpans is a slite of split points, including table start and end keys
	// for the table. We create router range spans then from taking each pair
	// of adjacent keys.
	spans := make([]distsqlrun.OutputRouterSpec_RangeRouterSpec_Span, len(splits)-1)
	encFn := func(b []byte) []byte {
		return encoding.EncodeBytesAscending(nil, b)
	}
	for i := 1; i < len(splits); i++ {
		start := splits[i-1]
		end := splits[i]
		stream := int32((i - 1) % len(nodes))
		spans[i-1] = distsqlrun.OutputRouterSpec_RangeRouterSpec_Span{
			Start:  encFn(start),
			End:    encFn(end),
			Stream: stream,
		}
		sstSpecs[stream].Spans = append(sstSpecs[stream].Spans, distsqlrun.SSTWriterSpec_SpanName{
			Name: fmt.Sprintf("%d.sst", i),
			End:  end,
		})
	}

	// We have the split ranges. Now re-read the CSV files and route them to SST writers.

	p := physicalPlan{}
	// This is a hardcoded two stage plan. The first stage is the mappers,
	// the second stage is the reducers. We have to keep track of all the mappers
	// we create because the reducers need to hook up a stream for each mapper.
	firstStageRouters := make([]distsqlplan.ProcessorIdx, len(csvSpecs))
	firstStageTypes := []sqlbase.ColumnType{colTypeBytes, colTypeBytes}

	routerSpec := distsqlrun.OutputRouterSpec_RangeRouterSpec{
		Spans: spans,
		Encodings: []distsqlrun.OutputRouterSpec_RangeRouterSpec_ColumnEncoding{
			{
				Column:   0,
				Encoding: sqlbase.DatumEncoding_ASCENDING_KEY,
			},
		},
	}

	stageID := p.NewStageID()
	// We can reuse the phase 1 ReadCSV specs, just have to clear sampling.
	for i, rcs := range csvSpecs {
		rcs.SampleSize = 0
		node := nodes[i]
		proc := distsqlplan.Processor{
			Node: node.NodeID,
			Spec: distsqlrun.ProcessorSpec{
				Core: distsqlrun.ProcessorCoreUnion{ReadCSV: rcs},
				Output: []distsqlrun.OutputRouterSpec{{
					Type:             distsqlrun.OutputRouterSpec_BY_RANGE,
					RangeRouterSpec:  routerSpec,
					DisableBuffering: true,
				}},
				StageID: stageID,
			},
		}
		pIdx := p.AddProcessor(proc)
		firstStageRouters[i] = pIdx
	}

	// The SST Writer returns 5 columns: name of the file, size of the file,
	// checksum, start key, end key.
	p.planToStreamColMap = []int{0, 1, 2, 3, 4}
	p.ResultTypes = []sqlbase.ColumnType{
		{SemanticType: sqlbase.ColumnType_STRING},
		{SemanticType: sqlbase.ColumnType_INT},
		colTypeBytes,
		colTypeBytes,
		colTypeBytes,
	}

	stageID = p.NewStageID()
	p.ResultRouters = make([]distsqlplan.ProcessorIdx, 0, len(nodes))
	for i, node := range nodes {
		swSpec := sstSpecs[i]
		if len(swSpec.Spans) == 0 {
			continue
		}
		swSpec.Progress = distsqlrun.JobProgress{
			JobID:        *job.ID(),
			Slot:         int32(len(p.ResultRouters)),
			Contribution: float32(len(swSpec.Spans)) / float32(len(spans)),
		}
		proc := distsqlplan.Processor{
			Node: node.NodeID,
			Spec: distsqlrun.ProcessorSpec{
				Input: []distsqlrun.InputSyncSpec{{
					ColumnTypes: firstStageTypes,
				}},
				Core:    distsqlrun.ProcessorCoreUnion{SSTWriter: &swSpec},
				Output:  []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}

		pIdx := p.AddProcessor(proc)
		for _, router := range firstStageRouters {
			p.Streams = append(p.Streams, distsqlplan.Stream{
				SourceProcessor:  router,
				SourceRouterSlot: i,
				DestProcessor:    pIdx,
				DestInput:        0,
			})
		}
		p.ResultRouters = append(p.ResultRouters, pIdx)
	}

	// Clear SamplingProgress and prep second stage job details for progress
	// tracking.
	if err := job.Progressed(ctx, func(ctx context.Context, details jobs.Details) float32 {
		d := details.(*jobs.Payload_Import).Import
		d.Tables[0].SamplingProgress = nil
		d.Tables[0].ReadProgress = make([]float32, len(csvSpecs))
		d.Tables[0].WriteProgress = make([]float32, len(p.ResultRouters))
		d.Tables[0].Samples = samples
		return d.Tables[0].Completed()
	}); err != nil {
		return err
	}

	l.distSQLPlanner.FinalizePlan(&planCtx, &p)

	recv := makeDistSQLReceiver(
		ctx,
		resultRows,
		tree.Rows,
		nil, /* rangeCache */
		nil, /* leaseCache */
		nil, /* txn - the flow does not read or write the database */
		func(ts hlc.Timestamp) {},
	)

	defer log.VEventf(ctx, 1, "finished job %s", job.Record.Description)
	// TODO(dan): We really don't need the txn for this flow, so remove it once
	// Run works without one.
	return db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		l.distSQLPlanner.Run(&planCtx, txn, &p, recv, evalCtx)
		return resultRows.Err()
	})
}

func (l *DistLoader) loadCSVSamplingPlan(
	ctx context.Context,
	job *jobs.Job,
	db *client.DB,
	evalCtx *extendedEvalContext,
	thisNode roachpb.NodeID,
	nodes []roachpb.NodeDescriptor,
	from []string,
	splitSize int64,
	planCtx *planningCtx,
	csvSpecs []*distsqlrun.ReadCSVSpec,
	sstSpecs []distsqlrun.SSTWriterSpec,
) ([][]byte, error) {
	// splitSize is the target number of bytes at which to create SST files. We
	// attempt to do this by sampling, which is what the first DistSQL plan of this
	// function does. CSV rows are converted into KVs. The total size of the KV is
	// used to determine if we should sample it or not. For example, if we had a
	// 100 byte KV and a 30MB splitSize, we would sample the KV with probability
	// 100/30000000. Over many KVs, this produces samples at approximately the
	// correct spacing, but obviously also with some error. We use oversample
	// below to decrease the error. We divide the splitSize by oversample to
	// produce the actual sampling rate. So in the example above, oversampling by a
	// factor of 3 would sample the KV with probability 100/10000000 since we are
	// sampling at 3x. Since we're now getting back 3x more samples than needed,
	// we only use every 1/(oversample), or 1/3 here, in our final sampling.
	const oversample = 3
	sampleSize := splitSize / oversample
	if sampleSize > math.MaxInt32 {
		return nil, errors.Errorf("SST size must fit in an int32: %d", splitSize)
	}

	var p physicalPlan
	stageID := p.NewStageID()

	for _, cs := range csvSpecs {
		cs.SampleSize = int32(sampleSize)
	}

	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(csvSpecs))
	for i, rcs := range csvSpecs {
		node := nodes[i]
		proc := distsqlplan.Processor{
			Node: node.NodeID,
			Spec: distsqlrun.ProcessorSpec{
				Core:    distsqlrun.ProcessorCoreUnion{ReadCSV: rcs},
				Output:  []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}
		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	if err := job.Progressed(ctx, func(ctx context.Context, details jobs.Details) float32 {
		d := details.(*jobs.Payload_Import).Import
		d.Tables[0].SamplingProgress = make([]float32, len(csvSpecs))
		return d.Tables[0].Completed()
	}); err != nil {
		return nil, err
	}

	// We only need the key during sorting.
	p.planToStreamColMap = []int{0}
	p.ResultTypes = []sqlbase.ColumnType{colTypeBytes, colTypeBytes}

	kvOrdering := distsqlrun.Ordering{
		Columns: []distsqlrun.Ordering_Column{{
			ColIdx:    0,
			Direction: distsqlrun.Ordering_Column_ASC,
		}},
	}

	sorterSpec := distsqlrun.SorterSpec{
		OutputOrdering: kvOrdering,
	}

	p.AddSingleGroupStage(thisNode,
		distsqlrun.ProcessorCoreUnion{Sorter: &sorterSpec},
		distsqlrun.PostProcessSpec{},
		[]sqlbase.ColumnType{colTypeBytes},
	)

	var samples [][]byte
	sampleCount := 0
	rowResultWriter := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		sampleCount++
		sampleCount = sampleCount % oversample
		if sampleCount == 0 {
			b := row[0].(*tree.DBytes)
			k, err := keys.EnsureSafeSplitKey(roachpb.Key(*b))
			if err != nil {
				return err
			}
			samples = append(samples, k)
		}
		return nil
	})

	// TODO(dan): Consider making FinalizePlan take a map explicitly instead
	// of this PlanCtx. https://reviewable.io/reviews/cockroachdb/cockroach/17279#-KqOrLpy9EZwbRKHLYe6:-KqOp00ntQEyzwEthAsl:bd4nzje
	l.distSQLPlanner.FinalizePlan(planCtx, &p)

	recv := makeDistSQLReceiver(
		ctx,
		&rowResultWriter,
		tree.Rows,
		nil, /* rangeCache */
		nil, /* leaseCache */
		nil, /* txn - the flow does not read or write the database */
		func(ts hlc.Timestamp) {},
	)
	log.VEventf(ctx, 1, "begin sampling phase of job %s", job.Record.Description)
	// TODO(dan): We really don't need the txn for this flow, so remove it once
	// Run works without one.
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// Clear the stage 2 data in case this function is ever restarted (it shouldn't be).
		samples = nil
		l.distSQLPlanner.Run(planCtx, txn, &p, recv, evalCtx)
		return rowResultWriter.Err()
	}); err != nil {
		return nil, err
	}

	log.VEventf(ctx, 1, "generated %d splits; begin routing for job %s", len(samples), job.Record.Description)

	return samples, nil
}
