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
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
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
	// channel operations.
	grp       ctxgroup.Group
	cancelGrp context.CancelFunc
	grpCtx    context.Context
}

// Start implements execinfra.RowSource.
func (p *ldrApplierProcessor) Start(ctx context.Context) {
	p.StartInternal(ctx, "ldrApplier")
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
		return p.runInputReader(ctx)
	})

	// Run the Applier's internal pipeline.
	p.grp.GoCtx(func(ctx context.Context) error {
		// Closing the applier here ensures the frontier channel closes, allowing
		// Next() to proceed if the applier errrors.
		defer p.applier.Close(ctx)
		return p.applier.Run(ctx, p.applierEvents)
	})

	// Forward loopback updates from the dep resolver to the Receive()
	// channel or to the output.
	p.grp.GoCtx(func(ctx context.Context) error {
		return p.depResolver.RunBackchannelForwarder(ctx, p.loopbackChs.updateCh)
	})
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
		ctx, applierID, writers, p.depResolver, p.spec.AllApplierIds,
	)
	if err != nil {
		return errors.Wrap(err, "creating applier")
	}

	p.applierEvents = make(chan txnapply.ApplierEvent)

	p.loopbackChs = ldrLoopback.lookupOrCreate(p.FlowCtx, p.spec.ApplierID)
	return nil
}

// Next implements execinfra.RowSource. It multiplexes dep resolver output
// rows and applier frontier metadata.
func (p *ldrApplierProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for p.State == execinfra.StateRunning {
		select {
		case ev, ok := <-p.depResolver.OutCh():
			if !ok {
				p.MoveToDraining(nil)
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
				p.MoveToDraining(nil)
				break
			}
			meta := p.encodeFrontierMeta(frontier)
			return nil, meta
		}
	}
	return nil, p.DrainHelper()
}

func (p *ldrApplierProcessor) ConsumerClosed() {
	p.close()
}

// close cancels background goroutines, waits for them to exit, and cleans up
// resources. It returns any non-cancellation error from the ctxgroup as
// trailing metadata so the TrailingMetaCallback can plumb it back to the
// distsql flow consumer.
func (p *ldrApplierProcessor) close() []execinfrapb.ProducerMetadata {
	if p.Closed {
		return nil
	}

	// Cancel the goroutine context to unblock any goroutines waiting on
	// channel operations, then wait for them to exit.
	if p.cancelGrp != nil {
		p.cancelGrp()
	}
	var meta []execinfrapb.ProducerMetadata
	if err := p.grp.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		meta = append(meta, execinfrapb.ProducerMetadata{Err: err})
	}

	p.InternalClose()
	return meta
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
// frontier timestamp. The gateway metadata handler unmarshals this and
// persists it as the job's replicated time.
func (p *ldrApplierProcessor) encodeFrontierMeta(
	frontier hlc.Timestamp,
) *execinfrapb.ProducerMetadata {
	// TODO(msbutler): any protos are the enemy. Refactor this.
	marshalledFrontier, err := pbtypes.MarshalAny(&frontier)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(
			err, "marshaling frontier timestamp"))
	}
	return &execinfrapb.ProducerMetadata{
		BulkProcessorProgress: &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
			ProgressDetails: *marshalledFrontier,
			NodeID:          base.SQLInstanceID(p.spec.ApplierID),
		},
	}
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
					return proc.close()
				},
			},
		)
		if err != nil {
			return nil, err
		}
		return proc, nil
	}
}
