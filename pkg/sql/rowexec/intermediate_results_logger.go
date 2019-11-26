// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TODO(yuzefovich): this is an almost exact copy of noop processor. Should we
// modify NoopCoreSpec to handle the debugging mode?

// intermediateResultsLogger is a processor that passes rows through and logs
// them. This is useful for debugging purposes.
type intermediateResultsLogger struct {
	execinfra.ProcessorBase

	input       execinfra.RowSource
	componentID string
}

var _ execinfra.Processor = &intermediateResultsLogger{}
var _ execinfra.RowSource = &intermediateResultsLogger{}

const intermediateResultsLoggerName = "logger"

func newIntermediateResultsLogger(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	componentID string,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*intermediateResultsLogger, error) {
	n := &intermediateResultsLogger{
		input:       input,
		componentID: componentID,
	}
	if err := n.Init(
		n,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{n.input}},
	); err != nil {
		return nil, err
	}
	return n, nil
}

// Start is part of the RowSource interface.
func (n *intermediateResultsLogger) Start(ctx context.Context) context.Context {
	n.input.Start(ctx)
	return n.StartInternal(ctx, intermediateResultsLoggerName)
}

// Next is part of the RowSource interface.
func (n *intermediateResultsLogger) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for n.State == execinfra.StateRunning {
		row, meta := n.input.Next()

		if meta != nil {
			if meta.Err != nil {
				n.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			n.MoveToDraining(nil /* err */)
			break
		}

		if outRow := n.ProcessRowHelper(row); outRow != nil {
			// TODO(yuzefovich): I can't seem to figure out which types to pass into
			// outRow.String(). I tried n.input.OutputTypes() and n.OutputTypes(),
			// but both fail.
			log.Infof(n.Ctx, "%s: %v", n.componentID, outRow)
			return outRow, nil
		}
	}
	return nil, n.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (n *intermediateResultsLogger) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	n.InternalClose()
}
