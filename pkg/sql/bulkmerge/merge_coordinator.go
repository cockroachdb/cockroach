// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/taskset"
	"github.com/cockroachdb/errors"
)

var (
	_ execinfra.Processor = &mergeCoordinator{}
	_ execinfra.RowSource = &mergeCoordinator{}
)

// Emits a single row on completion which is a protobuf containing the details
// of the merged SSTs.
// TODO(jeffswenson): define the protobuf
var mergeCoordinatorOutputTypes = []*types.T{
	types.Bytes,
}

type mergeCoordinator struct {
	execinfra.ProcessorBase

	input execinfra.RowSource
	spec  execinfrapb.MergeCoordinatorSpec
	tasks taskset.TaskSet

	loopback chan rowenc.EncDatumRow
	cleanup  func()

	done    bool
	results execinfrapb.BulkMergeSpec_Output
}

type mergeCoordinatorInput struct {
	sqlInstanceID string
	taskID        taskset.TaskId
	outputSSTs    []execinfrapb.BulkMergeSpec_SST
}

// parseCoordinatorInput ensures each column has the correct type and unmarshals
// the output SSTs.
func parseCoordinatorInput(row rowenc.EncDatumRow) (mergeCoordinatorInput, error) {
	if len(row) != 3 {
		return mergeCoordinatorInput{}, errors.Newf("expected 3 columns, got %d", len(row))
	}
	sqlInstanceID, ok := row[0].Datum.(*tree.DBytes)
	if !ok {
		return mergeCoordinatorInput{}, errors.Newf("expected bytes column for sqlInstanceID, got %s", row[0].Datum.String())
	}
	taskID, ok := row[1].Datum.(*tree.DInt)
	if !ok {
		return mergeCoordinatorInput{}, errors.Newf("expected int4 column for taskID, got %s", row[1].Datum.String())
	}
	outputBytes, ok := row[2].Datum.(*tree.DBytes)
	if !ok {
		return mergeCoordinatorInput{}, errors.Newf("expected bytes column for outputSSTs, got %s", row[2].Datum.String())
	}
	results := execinfrapb.BulkMergeSpec_Output{}
	if err := protoutil.Unmarshal([]byte(*outputBytes), &results); err != nil {
		return mergeCoordinatorInput{}, err
	}
	return mergeCoordinatorInput{
		sqlInstanceID: string(*sqlInstanceID),
		taskID:        taskset.TaskId(*taskID),
		outputSSTs:    results.Ssts,
	}, nil
}

// Next implements execinfra.RowSource.
func (m *mergeCoordinator) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for m.State == execinfra.StateRunning {
		row, meta := m.input.Next()
		switch {
		case row != nil:
			err := m.handleRow(row)
			if err != nil {
				m.MoveToDraining(err)
				return nil, m.DrainHelper()
			}
			continue
		case meta == nil:
			if m.done {
				m.MoveToDraining(nil /* err */)
				return nil, m.DrainHelper()
			}
			m.done = true
			return m.emitResults()
		case meta.Err != nil:
			m.MoveToDraining(meta.Err)
			return nil, m.DrainHelper()
		default:
			m.MoveToDraining(errors.Newf("unexpected meta: %v", meta))
			return nil, m.DrainHelper()
		}
	}
	return nil, m.DrainHelper()
}

func (m *mergeCoordinator) emitResults() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	marshaled, err := protoutil.Marshal(&m.results)
	if err != nil {
		m.MoveToDraining(errors.Wrap(err, "failed to marshal results"))
		return nil, m.DrainHelper()
	}
	return rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(marshaled))},
	}, nil
}

func (m *mergeCoordinator) publishInitialTasks() {
	for _, sqlInstanceID := range m.spec.WorkerSqlInstanceIds {
		// TODO(jeffswenson): test what happens if there are more sql instances
		// than tasks.
		taskID := m.tasks.ClaimFirst()
		if taskID.IsDone() {
			m.closeLoopback()
			return
		}
		m.loopback <- rowenc.EncDatumRow{
			rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(sqlInstanceID))},
			rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(taskID))},
		}
	}
}

func (m *mergeCoordinator) closeLoopback() {
	if m.cleanup != nil {
		m.cleanup()
		m.cleanup = nil
	}
}

// handleRow accepts a row output by the merge processor, marks its task as
// complete
func (m *mergeCoordinator) handleRow(row rowenc.EncDatumRow) error {
	input, err := parseCoordinatorInput(row)
	if err != nil {
		return err
	}

	m.results.Ssts = append(m.results.Ssts, input.outputSSTs...)

	next := m.tasks.ClaimNext(input.taskID)
	if next.IsDone() {
		m.closeLoopback()
		return nil
	}

	// TODO(jeffswenson): do I need to worry about deadlocks here?
	m.loopback <- rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(input.sqlInstanceID))},
		rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(next))},
	}

	return nil
}

// Start implements execinfra.RowSource.
func (m *mergeCoordinator) Start(ctx context.Context) {
	m.StartInternal(ctx, "mergeCoordinator")
	m.input.Start(ctx)
	m.publishInitialTasks()
}

func init() {
	rowexec.NewMergeCoordinatorProcessor = func(ctx context.Context, flow *execinfra.FlowCtx, flowID int32, spec execinfrapb.MergeCoordinatorSpec, postSpec *execinfrapb.PostProcessSpec, input execinfra.RowSource) (execinfra.Processor, error) {
		channel, cleanup := loopback.create(flow)
		mc := &mergeCoordinator{
			input:    input,
			tasks:    taskset.MakeTaskSet(spec.TaskCount),
			loopback: channel,
			cleanup:  cleanup,
			spec:     spec,
		}
		err := mc.Init(ctx, mc, postSpec, mergeCoordinatorOutputTypes, flow, flowID, nil, execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{input},
		})
		if err != nil {
			return nil, err
		}
		return mc, nil
	}
}
