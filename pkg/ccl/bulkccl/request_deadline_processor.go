// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package bulkccl

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var refreshDeadlineFrequency = 1 * time.Second

const requestDeadlineProcName = `requestdeadline`

var requestDeadlineResultTypes = []*types.T{
	types.Bytes, // deadline hlc.Timestamp
}

type requestDeadlineProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.RequestDeadlineSpec
	output  execinfra.RowReceiver

	requestDeadline *bulk.RequestDeadline
	rootProcDone    chan struct{}
}

var _ execinfra.Processor = &requestDeadlineProcessor{}
var _ execinfra.RowSource = &requestDeadlineProcessor{}

func init() {
	rowexec.NewRequestDeadlineProcessor = newRequestDeadlineProcessor
}

func newRequestDeadlineProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.RequestDeadlineSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	srp := &requestDeadlineProcessor{
		flowCtx:         flowCtx,
		spec:            spec,
		output:          output,
		requestDeadline: &bulk.RequestDeadline{},
		rootProcDone:    make(chan struct{}),
	}
	if err := srp.Init(
		srp,
		post,
		requestDeadlineResultTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				srp.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	return srp, nil
}

// Start is part of the RowSource interface.
func (rdp *requestDeadlineProcessor) Start(ctx context.Context) {
	ctx = rdp.StartInternal(ctx, requestDeadlineProcName)
	pickDeadline := func() {
		deadline := timeutil.Now().Add(bulk.AddSSTableRequestDeadlineWindow)
		rdp.requestDeadline.SetDeadline(hlc.Timestamp{WallTime: deadline.UnixNano()})
	}

	// Pick the first deadline so that Next() is guaranteed to have a time set.
	pickDeadline()

	// Start goroutine that picks a new deadline.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-rdp.rootProcDone:
				return
			default:
			}
			time.Sleep(refreshDeadlineFrequency)
			pickDeadline()
		}
	}()
}

// Next is part of the RowSource interface.
func (rdp *requestDeadlineProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if rdp.State != execinfra.StateRunning {
		return nil, rdp.DrainHelper()
	}

	deadline := rdp.requestDeadline.GetDeadline()
	if deadline.IsEmpty() {
		rdp.MoveToDraining(errors.AssertionFailedf("deadline not set on %s", requestDeadlineProcName))
		return nil, rdp.DrainHelper()
	}
	deadlineBytes, err := protoutil.Marshal(&deadline)
	if err != nil {
		rdp.MoveToDraining(err)
		return nil, rdp.DrainHelper()
	}
	row := rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(deadlineBytes))),
	}

	if outRow := rdp.ProcessRowHelper(row); outRow != nil {
		return outRow, nil
	}

	rdp.MoveToDraining(nil /* error */)
	return nil, rdp.DrainHelper()
}

func (rdp *requestDeadlineProcessor) close() {
	if rdp.InternalClose() {
		close(rdp.rootProcDone)
	}
}

// ConsumerClosed is part of the RowSource interface.
func (rdp *requestDeadlineProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	rdp.close()
}
