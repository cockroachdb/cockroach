// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// TxnFeedMessage wraps a TxnFeedEvent with the anchor span it was
// registered for.
type TxnFeedMessage struct {
	*kvpb.TxnFeedEvent
	RegisteredSpan roachpb.Span
}

// TxnFeed divides TxnFeed requests across range boundaries and
// establishes multiplexed TxnFeed streams. Events are delivered on
// eventCh. Blocks until the context is cancelled or a fatal error
// occurs.
func (ds *DistSender) TxnFeed(
	ctx context.Context, spans []SpanTimePair, eventCh chan<- TxnFeedMessage,
) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("expected at least 1 span, got none")
	}
	return muxTxnFeed(ctx, spans, ds, eventCh)
}

// txnFeedErrorInfo describes how a txnfeed error should be handled.
type txnFeedErrorInfo struct {
	evict       bool // Evict routing info before retry.
	resolveSpan bool // Re-divide span on range boundaries.
}

// handleTxnFeedError classifies a txnfeed error and returns a
// recovery strategy. Returns a non-nil error if the txnfeed should
// terminate.
func handleTxnFeedError(ctx context.Context, err error) (txnFeedErrorInfo, error) {
	if err == nil {
		return txnFeedErrorInfo{}, nil
	}
	switch {
	case errors.Is(err, io.EOF):
		return txnFeedErrorInfo{}, nil
	case errors.HasType(err, (*kvpb.StoreNotFoundError)(nil)):
		return txnFeedErrorInfo{evict: true}, nil
	case errors.HasType(err, (*kvpb.NodeUnavailableError)(nil)):
		return txnFeedErrorInfo{}, nil
	case IsSendError(err):
		return txnFeedErrorInfo{evict: true}, nil
	case errors.HasType(err, (*kvpb.RangeNotFoundError)(nil)):
		return txnFeedErrorInfo{evict: true}, nil
	case errors.HasType(err, (*kvpb.RangeKeyMismatchError)(nil)):
		return txnFeedErrorInfo{evict: true, resolveSpan: true}, nil
	case errors.HasType(err, (*kvpb.TxnFeedRetryError)(nil)):
		var t *kvpb.TxnFeedRetryError
		if ok := errors.As(err, &t); !ok {
			return txnFeedErrorInfo{}, errors.AssertionFailedf(
				"wrong error type: %T", err)
		}
		switch t.Reason {
		case kvpb.TxnFeedRetryError_REASON_REPLICA_REMOVED,
			kvpb.TxnFeedRetryError_REASON_RAFT_SNAPSHOT,
			kvpb.TxnFeedRetryError_REASON_TXNFEED_CLOSED:
			return txnFeedErrorInfo{}, nil
		case kvpb.TxnFeedRetryError_REASON_RANGE_SPLIT,
			kvpb.TxnFeedRetryError_REASON_RANGE_MERGED:
			return txnFeedErrorInfo{evict: true, resolveSpan: true}, nil
		case kvpb.TxnFeedRetryError_REASON_MANUAL_RANGE_SPLIT:
			return txnFeedErrorInfo{evict: true, resolveSpan: true}, nil
		default:
			return txnFeedErrorInfo{}, errors.AssertionFailedf(
				"unrecognized TxnFeedRetryError reason: %v", t.Reason)
		}
	default:
		return txnFeedErrorInfo{}, err
	}
}
