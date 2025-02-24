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
}

// Next implements execinfra.RowSource.
func (m *mergeCoordinator) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for m.State == execinfra.StateRunning {
		row, meta := m.input.Next()
		switch {
		case row == nil && meta == nil:
			m.MoveToDraining(nil /* err */)
			break
		case meta != nil && meta.Err != nil:
			m.MoveToDraining(meta.Err)
			break
		case meta != nil:
			m.MoveToDraining(errors.Newf("unexpected meta: %v", meta))
			break
		case row != nil:
			base := *row[2].Datum.(*tree.DBytes)
			return rowenc.EncDatumRow{
				rowenc.EncDatum{Datum: tree.NewDBytes(base + "->coordinator")},
			}, nil
		}
	}
	return nil, m.DrainHelper()
}

// Start implements execinfra.RowSource.
func (m *mergeCoordinator) Start(ctx context.Context) {
	m.StartInternal(ctx, "mergeCoordinator")
	m.input.Start(ctx)
}

func init() {
	rowexec.NewMergeCoordinatorProcessor = func(ctx context.Context, flow *execinfra.FlowCtx, flowID int32, spec execinfrapb.MergeCoordinatorSpec, postSpec *execinfrapb.PostProcessSpec, input execinfra.RowSource) (execinfra.Processor, error) {
		mc := &mergeCoordinator{
			input: input,
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
