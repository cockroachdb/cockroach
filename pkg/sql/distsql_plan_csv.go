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
	"fmt"
	"math"

	"golang.org/x/net/context"

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
	statementType tree.StatementType
	rowContainer  *sqlbase.RowContainer
	rowsAffected  int
}

// NewRowResultWriter creates a new RowResultWriter.
func NewRowResultWriter(
	statementType tree.StatementType, rowContainer *sqlbase.RowContainer,
) *RowResultWriter {
	return &RowResultWriter{statementType: statementType, rowContainer: rowContainer}
}

// StatementType implements the rowResultWriter interface.
func (b *RowResultWriter) StatementType() tree.StatementType {
	return b.statementType
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

// LoadCSV performs a distributed transformation of the CSV files at from
// and stores them in enterprise backup format at to.
func (l *DistLoader) LoadCSV(
	ctx context.Context,
	job *jobs.Job,
	db *client.DB,
	evalCtx tree.EvalContext,
	thisNode roachpb.NodeID,
	nodes []roachpb.NodeDescriptor,
	resultRows *RowResultWriter,
	tableDesc *sqlbase.TableDescriptor,
	from []string,
	to string,
	comma, comment rune,
	nullif *string,
	walltime int64,
	splitSize int64,
) error {
	ctx = log.WithLogTag(ctx, "import-distsql", nil)

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
		return errors.Errorf("SST size must fit in an int32: %d", splitSize)
	}

	var p physicalPlan
	colTypeBytes := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}
	stageID := p.NewStageID()

	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(from))
	// Stage 1: for each input file, assign it to a node
	for i, input := range from {
		// TODO(mjibson): attempt to intelligently schedule http files to matching cockroach nodes
		rcs := distsqlrun.ReadCSVSpec{
			SampleSize: int32(sampleSize),
			TableDesc:  *tableDesc,
			Uri:        input,
			Options: roachpb.CSVOptions{
				Comma:   comma,
				Comment: comment,
				Nullif:  nullif,
			},
		}
		node := nodes[i%len(nodes)]
		proc := distsqlplan.Processor{
			Node: node.NodeID,
			Spec: distsqlrun.ProcessorSpec{
				Core:    distsqlrun.ProcessorCoreUnion{ReadCSV: &rcs},
				Output:  []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}
		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
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

	ci := sqlbase.ColTypeInfoFromColTypes([]sqlbase.ColumnType{colTypeBytes})
	rowContainer := sqlbase.NewRowContainer(*evalCtx.ActiveMemAcc, ci, 0)
	rowResultWriter := NewRowResultWriter(tree.Rows, rowContainer)

	planCtx := l.distSQLPlanner.newPlanningCtx(ctx, &evalCtx, nil)
	// Because we're not going through the normal pathways, we have to set up
	// the nodeID -> nodeAddress map ourselves.
	for _, node := range nodes {
		planCtx.nodeAddresses[node.NodeID] = node.Address.String()
	}
	// TODO(dan): Consider making FinalizePlan take a map explicitly instead
	// of this PlanCtx. https://reviewable.io/reviews/cockroachdb/cockroach/17279#-KqOrLpy9EZwbRKHLYe6:-KqOp00ntQEyzwEthAsl:bd4nzje
	l.distSQLPlanner.FinalizePlan(&planCtx, &p)

	recv, err := makeDistSQLReceiver(
		ctx,
		rowResultWriter,
		nil, /* rangeCache */
		nil, /* leaseCache */
		nil, /* txn - the flow does not read or write the database */
		func(ts hlc.Timestamp) {},
	)
	if err != nil {
		return err
	}
	log.VEventf(ctx, 1, "begin sampling phase of job %s", job.Record.Description)
	// TODO(dan): We really don't need the txn for this flow, so remove it once
	// Run works without one.
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		rowContainer.Clear(ctx)
		return l.distSQLPlanner.Run(&planCtx, txn, &p, &recv, evalCtx)
	}); err != nil {
		return err
	}
	if recv.err != nil {
		return recv.err
	}

	n := rowContainer.Len()
	tableSpan := tableDesc.TableSpan()
	prevKey := tableSpan.Key
	var spans []distsqlrun.OutputRouterSpec_RangeRouterSpec_Span
	encFn := func(b []byte) []byte {
		return encoding.EncodeBytesAscending(nil, b)
	}
	sstSpecs := make([]distsqlrun.SSTWriterSpec, len(nodes))
	for i := range nodes {
		sstSpecs[i] = distsqlrun.SSTWriterSpec{
			Destination:   to,
			WalltimeNanos: walltime,
		}
	}
	addSpan := func(start, end []byte) {
		stream := int32(len(spans) % len(nodes))
		spans = append(spans, distsqlrun.OutputRouterSpec_RangeRouterSpec_Span{
			Start:  encFn(start),
			End:    encFn(end),
			Stream: stream,
		})
		sstSpecs[stream].Spans = append(sstSpecs[stream].Spans, distsqlrun.SSTWriterSpec_SpanWriter{
			Name: fmt.Sprintf("%d.sst", len(spans)),
			End:  end,
		})
	}
	// Assign each span to a node.
	for i := oversample - 1; i < n; i += oversample {
		row := rowContainer.At(i)
		b := row[0].(*tree.DBytes)
		k, err := keys.EnsureSafeSplitKey(roachpb.Key(*b))
		if err != nil {
			return err
		}
		addSpan(prevKey, k)
		prevKey = k
	}
	rowContainer.Close(ctx)
	addSpan(prevKey, tableSpan.EndKey)

	routerSpec := distsqlrun.OutputRouterSpec_RangeRouterSpec{
		Spans: spans,
		Encodings: []distsqlrun.OutputRouterSpec_RangeRouterSpec_ColumnEncoding{
			{
				Column:   0,
				Encoding: sqlbase.DatumEncoding_ASCENDING_KEY,
			},
		},
	}

	log.VEventf(ctx, 1, "generated %d splits; begin routing for job %s", len(spans), job.Record.Description)
	if err := job.Progressed(ctx, 1.0/10.0, jobs.Noop); err != nil {
		log.Warningf(ctx, "failed to update job progress: %s", err)
	}

	// We have the split ranges. Now re-read the CSV files and route them to SST writers.

	p = physicalPlan{}
	// This is a hardcoded two stage plan. The first stage is the mappers,
	// the second stage is the reducers. We have to keep track of all the mappers
	// we create because the reducers need to hook up a stream for each mapper.
	firstStageRouters := make([]distsqlplan.ProcessorIdx, len(from))
	firstStageTypes := []sqlbase.ColumnType{colTypeBytes, colTypeBytes}

	stageID = p.NewStageID()
	for i, input := range from {
		// TODO(mjibson): attempt to intelligently schedule http files to matching cockroach nodes
		rcs := distsqlrun.ReadCSVSpec{
			Options: roachpb.CSVOptions{
				Comma:   comma,
				Comment: comment,
				Nullif:  nullif,
			},
			SampleSize: 0,
			TableDesc:  *tableDesc,
			Uri:        input,
		}
		node := nodes[i%len(nodes)]
		proc := distsqlplan.Processor{
			Node: node.NodeID,
			Spec: distsqlrun.ProcessorSpec{
				Core: distsqlrun.ProcessorCoreUnion{ReadCSV: &rcs},
				Output: []distsqlrun.OutputRouterSpec{{
					Type:            distsqlrun.OutputRouterSpec_BY_RANGE,
					RangeRouterSpec: routerSpec,
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

	l.distSQLPlanner.FinalizePlan(&planCtx, &p)

	recv, err = makeDistSQLReceiver(
		ctx,
		resultRows,
		nil, /* rangeCache */
		nil, /* leaseCache */
		nil, /* txn - the flow does not read or write the database */
		func(ts hlc.Timestamp) {},
	)
	if err != nil {
		return err
	}

	defer log.VEventf(ctx, 1, "finished job %s", job.Record.Description)
	// TODO(dan): We really don't need the txn for this flow, so remove it once
	// Run works without one.
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		return l.distSQLPlanner.Run(&planCtx, txn, &p, &recv, evalCtx)
	}); err != nil {
		return err
	}
	if recv.err != nil {
		return recv.err
	}

	return nil
}
