// Copyright 2025 The Cockroach Authors.
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
)

// RangefeedHandler is the interface for the rangefeed callbacks. When the
// eventStream is used directly (unordered), it is its own adapter. When
// wrapped by OrderedStreamHandler (ordered), the wrapper implements these
// and buffers KVs, then delivers via OnValue and OnFrontierAdvance.
type RangefeedHandler interface {
	onValue(ctx context.Context, value *kvpb.RangeFeedValue)
	onValues(ctx context.Context, values []kvpb.RangeFeedValue)
	onFrontier(ctx context.Context, timestamp hlc.Timestamp)
	onCheckpoint(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint)
	onSSTable(ctx context.Context, sst *kvpb.RangeFeedSSTable, registeredSpan roachpb.Span)
	onDeleteRange(ctx context.Context, delRange *kvpb.RangeFeedDeleteRange)
	onMetadata(ctx context.Context, metadata *kvpb.RangeFeedMetadata)
	onInitialScanDone(ctx context.Context)

	// OnValue is called by the ordered adapter when delivering a buffered KV.
	OnValue(ctx context.Context, value *kvpb.RangeFeedValue)
	// OnFrontierAdvance is called by the ordered adapter after delivering all
	// KVs up to timestamp; the handler should flush batch and advance debug.
	OnFrontierAdvance(ctx context.Context, timestamp hlc.Timestamp)
	setErr(error) bool
}

// OrderedStreamHandler wraps a RangefeedHandler (the eventStream) and
// buffers KVs, delivering them in (timestamp, key) order on frontier advance.
// SSTable events are handled by reading their KVs and adding each to the buffer.
// It also implements eval.ValueGenerator by delegating to the handler.
type OrderedStreamHandler struct {
	handler RangefeedHandler
	buffer  *OrderedBuffer
}

var _ eval.ValueGenerator = (*OrderedStreamHandler)(nil)

// NewOrderedEventStream returns an ordered adapter that wraps the given
// handler. The caller must set handler.adapter = returned value so the
// eventStream uses this adapter for rangefeed callbacks.
func NewOrderedEventStream(handler RangefeedHandler, config *OrderedBufferConfig) *OrderedStreamHandler {
	return &OrderedStreamHandler{handler: handler, buffer: newOrderedBuffer(*config)}
}

func (h *OrderedStreamHandler) setErr(err error) bool {
	return h.handler.setErr(err)
}

func (h *OrderedStreamHandler) onValue(ctx context.Context, value *kvpb.RangeFeedValue) {
	h.handler.setErr(h.buffer.Add(ctx, value))
}

func (h *OrderedStreamHandler) onValues(ctx context.Context, values []kvpb.RangeFeedValue) {
	for i := range values {
		h.onValue(ctx, &values[i])
	}
}

func (h *OrderedStreamHandler) handleFrontier(ctx context.Context, timestamp hlc.Timestamp) error {
	err := h.buffer.FlushToDisk(ctx, timestamp)
	if err != nil {
		return err
	}
	var iterExhausted bool
	var values []*kvpb.RangeFeedValue
	// iterate until the iterator is exhausted. This is necessary because GetKVsFromDisk
	// returns at most the configured batch size. If the iterator is not exhausted, we need to
	// call GetKVsFromDisk again to get the next batch.
	for !iterExhausted {
		values, iterExhausted, err = h.buffer.GetKVsFromDisk(ctx, timestamp)
		if err != nil {
			return err
		}
		for _, v := range values {
			h.handler.OnValue(ctx, v)
		}
	}
	h.handler.OnFrontierAdvance(ctx, timestamp)
	return nil
}

func (h *OrderedStreamHandler) onFrontier(ctx context.Context, timestamp hlc.Timestamp) {
	h.handler.setErr(h.handleFrontier(ctx, timestamp))
}

func (h *OrderedStreamHandler) onCheckpoint(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
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
		func(storage.MVCCRangeKeyValue) error { return nil },
	)
}

func (h *OrderedStreamHandler) onSSTable(
	ctx context.Context, sst *kvpb.RangeFeedSSTable, registeredSpan roachpb.Span,
) {
	h.handler.setErr(h.handleSSTable(ctx, sst, registeredSpan))
}

func (h *OrderedStreamHandler) handleDeleteRange(
	ctx context.Context, delRange *kvpb.RangeFeedDeleteRange,
) error {
	return h.buffer.Add(ctx, &kvpb.RangeFeedValue{
		Key:   delRange.Span.Key,
		Value: roachpb.Value{RawBytes: nil, Timestamp: delRange.Timestamp},
	})
}

func (h *OrderedStreamHandler) onDeleteRange(ctx context.Context, delRange *kvpb.RangeFeedDeleteRange) {
	h.handler.setErr(h.handleDeleteRange(ctx, delRange))
}

func (h *OrderedStreamHandler) onMetadata(ctx context.Context, metadata *kvpb.RangeFeedMetadata) {
	h.handler.onMetadata(ctx, metadata)
}

func (h *OrderedStreamHandler) onInitialScanDone(ctx context.Context) {
	h.handler.onInitialScanDone(ctx)
}

// OnValue and OnFrontierAdvance are required by RangefeedHandler but are only
// invoked on the handler (eventStream), not on the adapter. No-ops here.
func (h *OrderedStreamHandler) OnValue(_ context.Context, _ *kvpb.RangeFeedValue)    {}
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
	h.handler.setErr(h.buffer.Close(ctx))
	h.handler.(eval.ValueGenerator).Close(ctx)
}
