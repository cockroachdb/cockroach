// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnapply"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnpb"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnwriter"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var (
	_ execinfra.Processor = &ldrApplierProcessor{}
	_ execinfra.RowSource = &ldrApplierProcessor{}
)

// applierOutputTypes defines the row format emitted by the applier processor
// to downstream dep resolver processors.
// Column 0: routing key (encoded SQL instance ID for the dep resolver).
// Column 1: serialized dep resolver request.
var applierOutputTypes = []*types.T{
	types.Bytes,
	types.Bytes,
}

// ldrApplierProcessor wraps the existing Applier and manages the bidirectional
// communication with the coordinator (input) and dep resolver processors
// (output + loopback backchannel).
type ldrApplierProcessor struct {
	execinfra.ProcessorBase

	spec  execinfrapb.TxnLDRApplierSpec
	input execinfra.RowSource

	txnDecoder  *ldrdecoder.TxnDecoder
	applier     *txnapply.Applier
	depResolver *txnapply.DistDepResolverClient
	loopbackChs *ldrLoopbackChannels

	// applierEvents feeds deserialized events from the coordinator input to
	// the Applier.Run() goroutine.
	applierEvents chan txnapply.ApplierEvent

	// grp manages the lifecycle of the input reader, applier, and
	// backchannel forwarder goroutines. cancelGrp cancels the context
	// used by grp, allowing close() to unblock goroutines waiting on
	// channel operations. errCh is where errors from goroutines are
	// sent.
	grp       ctxgroup.Group
	cancelGrp context.CancelFunc
	grpCtx    context.Context
	errCh     chan error
}

// Start implements execinfra.RowSource.
func (p *ldrApplierProcessor) Start(ctx context.Context) {
	ctx = p.StartInternal(ctx, "ldrApplier")
	p.input.Start(ctx)

	if err := p.setup(ctx); err != nil {
		p.MoveToDraining(err)
		return
	}

	grpCtx, cancelGrp := context.WithCancel(ctx)
	p.cancelGrp = cancelGrp
	p.grpCtx = grpCtx
	p.grp = ctxgroup.WithContext(grpCtx)

	// Read coordinator input rows, deserialize, and feed to the Applier.
	p.grp.GoCtx(func(ctx context.Context) error {
		defer close(p.applierEvents)
		p.sendError(p.runInputReader(ctx))
		return nil
	})

	// Run the Applier's internal pipeline.
	p.grp.GoCtx(func(ctx context.Context) error {
		defer p.applier.Close(ctx)
		p.sendError(p.applier.Run(ctx, p.applierEvents))
		return nil
	})

	// Forward loopback updates from the dep resolver to the Receive()
	// channel or to the output.
	p.grp.GoCtx(func(ctx context.Context) error {
		p.sendError(p.depResolver.RunBackchannelForwarder(ctx, p.loopbackChs.updateCh))
		return nil
	})
}

// sendError returns any non-cancellation error from the applier to
// the errCh. We opt against plumbing the error to the trailing metadata
// as it is cleaner to tear down the flow by cancelling the context on error
// than to try and propagate the error to other appliers; trailing metadata
// is not propagated until draining is complete, but we need the error to
// signal draining for other appliers.
func (p *ldrApplierProcessor) sendError(err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	select {
	case p.errCh <- err:
	default:
		log.Dev.VInfof(p.grpCtx, 2, "dropping additional error: %s", err)
	}
}

// setup initializes all components required for the applier pipeline:
// decoder, dep resolver client, transaction writers, and the applier itself.
func (p *ldrApplierProcessor) setup(ctx context.Context) error {
	applierID := p.spec.ApplierID
	tableMappings, err := buildTableMappings(ctx, p.spec.Schema)
	if err != nil {
		return errors.Wrap(err, "building table mappings")
	}
	p.txnDecoder, err = ldrdecoder.NewTxnDecoder(
		ctx, p.FlowCtx.Cfg.DB, p.FlowCtx.Cfg.Settings, tableMappings,
	)
	if err != nil {
		return errors.Wrap(err, "creating txn decoder")
	}

	p.depResolver = txnapply.NewDistDepResolverClient(applierID)

	// Create the loopback backchannel that the dep resolver will write to.
	// The dep resolver processor on this node registers its own channel;
	// the applier looks it up after the dep resolver has started. For now,
	// the dep resolver creates the channel and the applier looks it up.
	// TODO(msbutler): coordinate startup ordering so the dep resolver's
	// loopback is guaranteed to exist before the applier starts reading.

	sv := &p.FlowCtx.Cfg.Settings.SV
	numWriters := int(txnNumWriters.Get(sv))
	writers := make([]txnwriter.TransactionWriter, 0, numWriters)
	for range numWriters {
		writer, err := txnwriter.NewTransactionWriter(
			ctx,
			p.FlowCtx.Cfg.DB,
			p.FlowCtx.Cfg.LeaseManager.(*lease.Manager),
			p.FlowCtx.Codec(),
			p.FlowCtx.Cfg.Settings,
		)
		if err != nil {
			for _, w := range writers {
				w.Close(ctx)
			}
			return errors.Wrap(err, "creating txn writer")
		}
		writers = append(writers, writer)
	}

	p.applier, err = txnapply.NewApplier(
		ctx, applierID, p.FlowCtx.Cfg.Settings, writers, p.depResolver, p.spec.AllApplierIds,
		func() *admission.SQLCPUHandle {
			return p.FlowCtx.Cfg.SQLCPUProvider.GetHandle(admission.WorkInfo{
				TenantID:   p.FlowCtx.Codec().TenantID,
				Priority:   admissionpb.LowPri,
				CreateTime: timeutil.Now().UnixNano(),
			}, false /* atGateway */)
		},
	)
	if err != nil {
		return errors.Wrap(err, "creating applier")
	}

	p.applierEvents = make(chan txnapply.ApplierEvent)
	p.errCh = make(chan error, 1)

	p.loopbackChs = ldrLoopback.lookupOrCreate(p.FlowCtx, p.spec.ApplierID)
	return nil
}

// Next implements execinfra.RowSource. It multiplexes dep resolver output
// rows and applier frontier metadata.
func (p *ldrApplierProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for p.State == execinfra.StateRunning {
		select {
		case <-p.Ctx().Done():
			p.MoveToDraining(nil)

		case err := <-p.errCh:
			p.MoveToDraining(err)

		case <-p.depResolver.OutCh():
			ev, ok := p.depResolver.PopOutEvent()
			if !ok {
				break
			}
			row, err := p.encodeDepResolverEvent(ev)
			if err != nil {
				p.MoveToDraining(errors.Wrap(err, "encoding dep resolver event"))
				break
			}
			return row, nil

		case frontier, ok := <-p.applier.Frontier():
			if !ok {
				break
			}

			meta, err := p.encodeFrontierMeta(frontier)
			if err != nil {
				p.MoveToDraining(err)
				break
			}
			return nil, meta
		}
	}
	return nil, p.DrainHelper()
}

func (p *ldrApplierProcessor) ConsumerClosed() {
	p.close()
}

// close cancels background goroutines, waits for them to exit, and cleans up
// resources.
func (p *ldrApplierProcessor) close() {
	if p.Closed {
		return
	}

	// Cancel the goroutine context to unblock any goroutines waiting on
	// channel operations, then wait for them to exit.
	if p.cancelGrp != nil {
		p.cancelGrp()
	}
	if err := p.grp.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		// We should be propagating errors through the errCh, so seeing
		// one here would be unexpected.
		log.Dev.Warningf(p.grpCtx, "dropping additional error: %s", err)
	}

	p.InternalClose()
}

// runInputReader reads input rows from the coordinator, deserializes them
// into ApplierEvents, and sends them to the applier's event channel.
func (p *ldrApplierProcessor) runInputReader(ctx context.Context) error {
	for {
		row, meta := p.input.Next()
		if meta != nil {
			if meta.Err != nil {
				return meta.Err
			}
			continue
		}
		if row == nil {
			return nil
		}

		event, err := p.decodeApplierEvent(row)
		if err != nil {
			return errors.Wrap(err, "decoding applier event")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p.applierEvents <- event:
		}
	}
}

// decodeApplierEvent deserializes a DistSQL row from the coordinator into an
// ApplierEvent. The payload column contains a marshaled LDRApplierEvent proto.
func (p *ldrApplierProcessor) decodeApplierEvent(
	row rowenc.EncDatumRow,
) (txnapply.ApplierEvent, error) {
	if len(row) < 2 {
		return nil, errors.AssertionFailedf(
			"expected 2 columns, got %d", len(row))
	}

	if err := row[1].EnsureDecoded(types.Bytes, nil); err != nil {
		return nil, err
	}
	payloadBytes := []byte(*row[1].Datum.(*tree.DBytes))

	var evt txnpb.LDRApplierEvent
	if err := protoutil.Unmarshal(payloadBytes, &evt); err != nil {
		return nil, errors.Wrap(err, "unmarshaling applier event")
	}

	switch e := evt.Event.(type) {
	case *txnpb.LDRApplierEvent_ScheduledTxn:
		st := e.ScheduledTxn

		decoded, err := p.txnDecoder.DecodeTxn(p.Ctx(), st.EncodedKVs)
		if err != nil {
			return nil, errors.Wrap(err, "decoding transaction KVs")
		}
		decoded.TxnID = st.TxnID

		return txnapply.ScheduledTransaction{
			Transaction:  decoded,
			Dependencies: st.Dependencies,
			EventHorizon: st.EventHorizon,
		}, nil

	case *txnpb.LDRApplierEvent_Checkpoint:
		return txnapply.Checkpoint{
			Timestamp: e.Checkpoint.Timestamp,
		}, nil

	default:
		return nil, errors.AssertionFailedf(
			"unknown applier event type: %T", evt.Event)
	}
}

// encodeDepResolverEvent serializes a dep resolver event into a DistSQL
// row routed to the correct dep resolver processor.
func (p *ldrApplierProcessor) encodeDepResolverEvent(
	ev txnapply.DepResolverEvent,
) (rowenc.EncDatumRow, error) {
	routingKey := physicalplan.RoutingKeyForSQLInstance(
		base.SQLInstanceID(ev.TargetApplierID),
	)
	payload, err := protoutil.Marshal(&ev.Event)
	if err != nil {
		return nil, err
	}
	return rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(routingKey))},
		rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(payload))},
	}, nil
}

// encodeFrontierMeta creates a ProducerMetadata carrying the applier's
// frontier timestamp. The gateway metadata handler extracts this and
// persists it as the job's replicated time.
func (p *ldrApplierProcessor) encodeFrontierMeta(
	frontier hlc.Timestamp,
) (*execinfrapb.ProducerMetadata, error) {
	progressBytes, err := protoutil.Marshal(&txnpb.TxnLDRProcProgress{
		ApplierID:  p.spec.ApplierID,
		Checkpoint: frontier,
	})
	if err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err, "marshaling applier frontier progress")
	}
	return &execinfrapb.ProducerMetadata{
		BulkProcessorProgress: &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
			ProgressMessage: progressBytes,
		},
	}, nil
}

func init() {
	rowexec.NewTxnLDRApplierProcessor = func(
		ctx context.Context,
		flowCtx *execinfra.FlowCtx,
		processorID int32,
		spec execinfrapb.TxnLDRApplierSpec,
		post *execinfrapb.PostProcessSpec,
		input execinfra.RowSource,
	) (execinfra.Processor, error) {
		proc := &ldrApplierProcessor{
			spec:  spec,
			input: input,
		}
		err := proc.Init(
			ctx, proc, post, applierOutputTypes, flowCtx, processorID, nil,
			execinfra.ProcStateOpts{
				InputsToDrain: []execinfra.RowSource{input},
				TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
					proc.close()
					return nil
				},
			},
		)
		if err != nil {
			return nil, err
		}
		return proc, nil
	}
}
