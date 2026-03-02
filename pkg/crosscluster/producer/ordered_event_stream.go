// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// RangefeedHandler is the interface for the rangefeed callbacks. When the
// eventStream is used directly (unordered), it is its own adapter. When
// wrapped by OrderedStreamHandler (ordered), the wrapper implements these
// and buffers KVs, then delivers via OnFrontierAdvance.
type RangefeedHandler interface {
	onValue(ctx context.Context, value *kvpb.RangeFeedValue)
	onValues(ctx context.Context, values []kvpb.RangeFeedValue)
	onFrontier(ctx context.Context, timestamp hlc.Timestamp)
	onCheckpoint(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint)
	onSSTable(ctx context.Context, sst *kvpb.RangeFeedSSTable, registeredSpan roachpb.Span)
	onDeleteRange(ctx context.Context, delRange *kvpb.RangeFeedDeleteRange)
	onMetadata(ctx context.Context, metadata *kvpb.RangeFeedMetadata)
	onInitialScanDone(ctx context.Context)
	// OnFrontierAdvance is called by the ordered adapter after it has fed all
	// events up to and including resolvedTs into the handler's batch; the handler should flush
	// that batch to the consumer.
	OnFrontierAdvance(ctx context.Context, resolvedTs hlc.Timestamp)
	setErr(error) bool
	hasErr() bool
}

// OrderedStreamHandler wraps a RangefeedHandler (the eventStream) and
// buffers KVs, delivering them in (timestamp, key) order on frontier advance.
// SSTable events are handled by reading their KVs and adding each to the buffer.
// It also implements eval.ValueGenerator by delegating to the handler.
type OrderedStreamHandler struct {
	handler    RangefeedHandler
	buffer     *OrderedBuffer
	resolvedTs hlc.Timestamp // Last flushed resolved timestamp for checkpoint generation.
}

var _ eval.ValueGenerator = (*OrderedStreamHandler)(nil)

// NewOrderedEventStream returns an ordered adapter that wraps the given
// handler. The caller must set handler.adapter = returned value so the
// eventStream uses this adapter for rangefeed callbacks.
func NewOrderedEventStream(
	handler RangefeedHandler, config *OrderedBufferConfig, initialScanTs hlc.Timestamp,
) *OrderedStreamHandler {
	return &OrderedStreamHandler{
		handler:    handler,
		buffer:     newOrderedBuffer(*config),
		resolvedTs: initialScanTs,
	}
}

func (h *OrderedStreamHandler) setErr(err error) bool {
	return h.handler.setErr(err)
}

func (h *OrderedStreamHandler) hasErr() bool {
	return h.handler.hasErr()
}

func (h *OrderedStreamHandler) onValue(ctx context.Context, value *kvpb.RangeFeedValue) {
	h.handler.setErr(h.buffer.Add(ctx, value))
}

func (h *OrderedStreamHandler) onValues(ctx context.Context, values []kvpb.RangeFeedValue) {
	for i := range values {
		h.onValue(ctx, &values[i])
	}
}

func (h *OrderedStreamHandler) handleFrontier(ctx context.Context, resolvedTs hlc.Timestamp) error {
	err := h.buffer.FlushToDisk(ctx, resolvedTs)
	if err != nil {
		return err
	}
	var iterExhausted bool
	var events []kvpb.RangeFeedEvent

	// iterate until the iterator is exhausted. This is necessary because GetEventsFromDisk
	// returns early when the batch threshold is reached.
	for !iterExhausted {
		events, iterExhausted, err = h.buffer.GetEventsFromDisk(ctx, resolvedTs)
		if err != nil {
			return err
		}
		for _, e := range events {
			if e.Val != nil {
				h.handler.onValue(ctx, e.Val)
			} else if e.DeleteRange != nil {
				h.handler.onDeleteRange(ctx, e.DeleteRange)
			} else {
				return errors.AssertionFailedf("unexpected RangeFeedEvent variant: %v", e)
			}
		}
	}
	h.handler.OnFrontierAdvance(ctx, resolvedTs)
	h.resolvedTs = resolvedTs
	return nil
}

func (h *OrderedStreamHandler) onFrontier(ctx context.Context, resolvedTs hlc.Timestamp) {
	h.handler.setErr(h.handleFrontier(ctx, resolvedTs))
}

func (h *OrderedStreamHandler) onCheckpoint(
	ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint,
) {
	h.handler.onCheckpoint(ctx, checkpoint)
}

func (h *OrderedStreamHandler) handleSSTable(
	ctx context.Context, sst *kvpb.RangeFeedSSTable, registeredSpan roachpb.Span,
) error {
	// Read each KV from the SST and add to the buffer so they are delivered in
	// (timestamp, key) order with rangefeed KVs.
	scanWithin := sst.Span
	if !registeredSpan.Contains(sst.Span) {
		scanWithin = registeredSpan
	}
	return replicationutils.ScanSST(sst, scanWithin,
		func(kv storage.MVCCKeyValue) error {
			return h.buffer.Add(ctx, &kvpb.RangeFeedValue{
				Key:   kv.Key.Key,
				Value: roachpb.Value{RawBytes: kv.Value, Timestamp: kv.Key.Timestamp},
			})
		},
		func(rk storage.MVCCRangeKeyValue) error {
			return h.buffer.AddDelRange(ctx, &kvpb.RangeFeedDeleteRange{
				Span:      roachpb.Span{Key: rk.RangeKey.StartKey, EndKey: rk.RangeKey.EndKey},
				Timestamp: rk.RangeKey.Timestamp,
			})
		},
	)
}

func (h *OrderedStreamHandler) onSSTable(
	ctx context.Context, sst *kvpb.RangeFeedSSTable, registeredSpan roachpb.Span,
) {
	h.handler.setErr(h.handleSSTable(ctx, sst, registeredSpan))
}

func (h *OrderedStreamHandler) onDeleteRange(
	ctx context.Context, delRange *kvpb.RangeFeedDeleteRange,
) {
	h.handler.setErr(h.buffer.AddDelRange(ctx, delRange))
}

func (h *OrderedStreamHandler) onMetadata(ctx context.Context, metadata *kvpb.RangeFeedMetadata) {
}

func (h *OrderedStreamHandler) onInitialScanDone(ctx context.Context) {
	// Use spec.InitialScanTimestamp instead of hlc.MaxTimestamp to ensure
	// checkpoints reflect the actual scan timestamp, not an artificially high value.
	eventStream, ok := h.handler.(*eventStream)
	if !ok {
		h.handler.setErr(errors.AssertionFailedf("expected handler to be an event stream"))
		return
	}
	scanTs := eventStream.spec.InitialScanTimestamp
	h.handler.setErr(h.handleFrontier(ctx, scanTs))
	h.handler.onInitialScanDone(ctx)
}

// OnFrontierAdvance is required by RangefeedHandler but is only invoked on the
// handler (eventStream), not on the adapter. No-op here.
func (h *OrderedStreamHandler) OnFrontierAdvance(_ context.Context, _ hlc.Timestamp) {}

// ValueGenerator implementation: delegate to handler (which is an eval.ValueGenerator).
func (h *OrderedStreamHandler) ResolvedType() *types.T {
	return h.handler.(eval.ValueGenerator).ResolvedType()
}

func (h *OrderedStreamHandler) Start(ctx context.Context, txn *kv.Txn) error {
	return h.handler.(eval.ValueGenerator).Start(ctx, txn)
}

func (h *OrderedStreamHandler) Next(ctx context.Context) (bool, error) {
	return h.handler.(eval.ValueGenerator).Next(ctx)
}

func (h *OrderedStreamHandler) Values() (tree.Datums, error) {
	return h.handler.(eval.ValueGenerator).Values()
}

func (h *OrderedStreamHandler) Close(ctx context.Context) {
	h.handler.(eval.ValueGenerator).Close(ctx)
	h.handler.setErr(h.buffer.Close(ctx))
}
