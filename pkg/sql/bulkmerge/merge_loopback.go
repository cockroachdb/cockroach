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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// loopbackMap allows the mergeLoopback processor to communicate with the merge
// coordinator by mapping flow IDs to channels.
type loopbackMap struct {
	syncutil.Mutex
	loopback map[execinfrapb.FlowID]chan rowenc.EncDatumRow
}

var loopback = &loopbackMap{
	loopback: make(map[execinfrapb.FlowID]chan rowenc.EncDatumRow),
}

// get returns the channel for the given id if it exists.
func (l *loopbackMap) get(flowCtx *execinfra.FlowCtx) (chan rowenc.EncDatumRow, bool) {
	l.Lock()
	defer l.Unlock()
	id := flowCtx.ID
	channel, ok := l.loopback[id]
	return channel, ok
}

// create returns a channel for the given id and a function to close it.
func (l *loopbackMap) create(flowCtx *execinfra.FlowCtx) (chan rowenc.EncDatumRow, func()) {
	l.Lock()
	defer l.Unlock()
	id := flowCtx.ID
	ch := make(chan rowenc.EncDatumRow)
	l.loopback[id] = ch
	return ch, func() {
		l.Lock()
		defer l.Unlock()
		delete(l.loopback, id)
		close(ch)
	}
}

var (
	_ execinfra.Processor = &mergeLoopback{}
	_ execinfra.RowSource = &mergeLoopback{}
)

var mergeLoopbackOutputTypes = []*types.T{
	// Span key for the range router. It encodes the destination
	// processor's SQL instance ID.
	types.Bytes,
	// Task ID
	types.Int4,
}

type mergeLoopback struct {
	execinfra.ProcessorBase
	loopback chan rowenc.EncDatumRow
}

// Next implements execinfra.RowSource.
func (m *mergeLoopback) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// Read from the loopback channel until it's closed
	if m.State == execinfra.StateRunning {
		row, ok := <-m.loopback
		if !ok {
			m.MoveToDraining(nil)
			return nil, m.DrainHelper()
		}
		return row, nil
	}
	return nil, m.DrainHelper()
}

// Start implements execinfra.RowSource.
func (m *mergeLoopback) Start(ctx context.Context) {
	m.StartInternal(ctx, "mergeLoopback")
	var ok bool
	m.loopback, ok = loopback.get(m.FlowCtx)
	if !ok {
		m.MoveToDraining(errors.New("loopback channel not found"))
		return
	}
}

func (m *mergeLoopback) DrainHelper() *execinfrapb.ProducerMetadata {
	// First drain any inputs coming back from the coordinator.
	for again := true; again; {
		select {
		case _, ok := <-m.loopback:
			if ok {
				continue
			}
		default:
		}
		again = false
	}
	return m.ProcessorBase.DrainHelper()
}

func init() {
	rowexec.NewMergeLoopbackProcessor = func(
		ctx context.Context,
		flow *execinfra.FlowCtx,
		flowID int32,
		spec execinfrapb.MergeLoopbackSpec,
		postSpec *execinfrapb.PostProcessSpec,
	) (execinfra.Processor, error) {
		ml := &mergeLoopback{}
		err := ml.Init(
			ctx, ml, postSpec, mergeLoopbackOutputTypes, flow, flowID, nil,
			execinfra.ProcStateOpts{},
		)
		if err != nil {
			return nil, err
		}
		return ml, nil
	}
}
