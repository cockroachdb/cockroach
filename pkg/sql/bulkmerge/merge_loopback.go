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
)

var (
	_ execinfra.Processor = &mergeLoopback{}
	_ execinfra.RowSource = &mergeLoopback{}
)

var mergeLoopbackOutputTypes = []*types.T{
	types.Bytes, // Span key for the range router. It encodes the destination processor's SQL instance ID.
	types.Int4,  // Task ID
}

type mergeLoopback struct {
	execinfra.ProcessorBase
	done bool
}

// Next implements execinfra.RowSource.
func (m *mergeLoopback) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if m.done {
		m.MoveToDraining(nil)
		return nil, m.DrainHelper()
	}
	m.done = true
	return rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDBytes("loopback")},
		rowenc.EncDatum{Datum: tree.NewDInt(1)},
	}, nil
}

// Start implements execinfra.RowSource.
func (m *mergeLoopback) Start(ctx context.Context) {
	m.StartInternal(ctx, "mergeLoopback")
	// TODO(jeffswenson): create the initial set of tasks
}

func init() {
	rowexec.NewMergeLoopbackProcessor = func(ctx context.Context, flow *execinfra.FlowCtx, flowID int32, spec execinfrapb.MergeLoopbackSpec, postSpec *execinfrapb.PostProcessSpec) (execinfra.Processor, error) {
		ml := &mergeLoopback{}
		err := ml.Init(ctx, ml, postSpec, mergeLoopbackOutputTypes, flow, flowID, nil, execinfra.ProcStateOpts{})
		if err != nil {
			return nil, err
		}
		return ml, nil
	}
}
