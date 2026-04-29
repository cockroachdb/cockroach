// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnapply"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

var (
	_ execinfra.Processor = &ldrDepResolverProcessor{}
	_ execinfra.RowSource = &ldrDepResolverProcessor{}
)

// ldrDepResolverProcessor wraps a TrackerServer and processes dependency
// resolution requests from applier processors. It receives
// Wait/WaitHorizon/Ready requests via its DistSQL input and sends
// DependencyUpdates back to the co-located applier via a loopback backchannel.
//
// It also handles forwarded DependencyUpdates (from remote dep resolvers
// routed through a remote applier) by passing them directly to the
// co-located applier's loopback channel.
type ldrDepResolverProcessor struct {
	execinfra.ProcessorBase

	spec    execinfrapb.TxnLDRDepResolverSpec
	input   execinfra.RowSource
	tracker *txnapply.TrackerServer

	// loopbackChs is the backchannel to the co-located applier.
	loopbackChs *ldrLoopbackChannels
	cleanup     func()
}

// Start implements execinfra.RowSource.
func (p *ldrDepResolverProcessor) Start(ctx context.Context) {
	p.StartInternal(ctx, "ldrDepResolver")
	p.input.Start(ctx)

	p.tracker = txnapply.NewTrackerServer()

	chs, cleanup := ldrLoopback.create(p.FlowCtx, p.spec.ApplierID)
	p.loopbackChs = chs
	p.cleanup = cleanup
}

// Next implements execinfra.RowSource. It reads requests from the input
// (applier processors), processes them via the TrackerServer, and sends
// resulting DependencyUpdates to the co-located applier via loopback. This
// processor is terminal — it produces no DistSQL output rows.
func (p *ldrDepResolverProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for p.State == execinfra.StateRunning {
		row, meta := p.input.Next()
		if meta != nil {
			if meta.Err != nil {
				p.MoveToDraining(meta.Err)
				break
			}
			return nil, meta
		}
		if row == nil {
			p.MoveToDraining(nil)
			break
		}

		if err := p.handleRequest(row); err != nil {
			p.MoveToDraining(err)
			break
		}
		// This processor produces no output rows; it sends updates via
		// the loopback backchannel only.
	}
	if p.cleanup != nil {
		p.cleanup()
		p.cleanup = nil
	}
	return nil, p.DrainHelper()
}

// handleRequest decodes a dep resolver event row from an applier processor
// and dispatches it to the TrackerServer or loopback backchannel.
func (p *ldrDepResolverProcessor) handleRequest(row rowenc.EncDatumRow) error {
	ev, err := decodeDepResolverEvent(row)
	if err != nil {
		return errors.Wrap(err, "decoding dep resolver event")
	}
	switch ev.Type {
	case streampb.DEP_RESOLVER_EVENT_WAIT:
		for _, txn := range ev.Wait.WaitTxns {
			resolved, resolvedTime := p.tracker.MaybeAddWaiter(txn, ev.Wait.WaitingID)
			if resolved {
				p.sendUpdate(ev.Wait.WaitingID, txnapply.DependencyUpdate{
					TxnID:        txn,
					ResolvedTime: resolvedTime,
				})
			}
		}
	case streampb.DEP_RESOLVER_EVENT_WAIT_HORIZON:
		resolved, resolvedTime := p.tracker.WaitHorizon(
			ev.WaitHorizon.WaitingID, ev.WaitHorizon.TxnHorizon,
		)
		if resolved {
			p.sendUpdate(ev.WaitHorizon.WaitingID, txnapply.DependencyUpdate{
				TxnID:        ldrdecoder.TxnID{ApplierID: ev.WaitHorizon.DependID},
				ResolvedTime: resolvedTime,
			})
		}
	case streampb.DEP_RESOLVER_EVENT_READY:
		updates := p.tracker.Ready(ev.Ready.ReadyTxn, ev.Ready.ResolvedTime)
		for applierID, update := range updates {
			p.sendUpdate(applierID, update)
		}
	case streampb.DEP_RESOLVER_EVENT_FORWARD_UPDATE:
		p.sendUpdate(p.spec.ApplierID, txnapply.DependencyUpdate{
			TxnID:        ev.ForwardUpdate.TxnID,
			ResolvedTime: ev.ForwardUpdate.ResolvedTime,
		})
	default:
		return errors.Newf("unknown dep resolver event type: %s", ev.Type)
	}
	return nil
}

// decodeDepResolverEvent deserializes a DistSQL row into an
// LDRDepResolverEvent. The row format is [routing_key (Bytes), payload (Bytes)].
func decodeDepResolverEvent(row rowenc.EncDatumRow) (streampb.LDRDepResolverEvent, error) {
	if len(row) != 2 {
		return streampb.LDRDepResolverEvent{}, errors.Newf(
			"expected 2 columns, got %d", len(row))
	}
	if err := row[1].EnsureDecoded(types.Bytes, nil); err != nil {
		return streampb.LDRDepResolverEvent{}, err
	}
	payloadBytes := []byte(*row[1].Datum.(*tree.DBytes))

	var ev streampb.LDRDepResolverEvent
	if err := protoutil.Unmarshal(payloadBytes, &ev); err != nil {
		return streampb.LDRDepResolverEvent{}, errors.Wrap(
			err, "unmarshaling dep resolver event")
	}
	return ev, nil
}

// sendUpdate delivers a DependencyUpdate to the co-located applier via
// the loopback backchannel.
func (p *ldrDepResolverProcessor) sendUpdate(
	targetApplierID ldrdecoder.ApplierID, update txnapply.DependencyUpdate,
) {
	// All updates go to the co-located applier. The applier's
	// DistDepResolverClient will forward updates that are not for the
	// local applier.
	update.TargetApplierID = targetApplierID
	p.loopbackChs.updateCh <- update
}

func init() {
	rowexec.NewTxnLDRDepResolverProcessor = func(
		ctx context.Context,
		flowCtx *execinfra.FlowCtx,
		processorID int32,
		spec execinfrapb.TxnLDRDepResolverSpec,
		post *execinfrapb.PostProcessSpec,
		input execinfra.RowSource,
	) (execinfra.Processor, error) {
		proc := &ldrDepResolverProcessor{
			spec:  spec,
			input: input,
		}
		err := proc.Init(
			ctx, proc, post, applierOutputTypes, flowCtx, processorID, nil,
			execinfra.ProcStateOpts{
				InputsToDrain: []execinfra.RowSource{input},
			},
		)
		if err != nil {
			return nil, err
		}
		return proc, nil
	}
}
