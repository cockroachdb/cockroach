// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	RegisterReadWriteCommand(roachpb.DeleteRange, declareKeysDeleteRange, DeleteRange)
}

func declareKeysDeleteRange(
	rs ImmutableRangeState,
	header *roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
	maxOffset time.Duration,
) {
	args := req.(*roachpb.DeleteRangeRequest)
	if args.Inline {
		DefaultDeclareKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	} else {
		DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	}
}

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys.
func DeleteRange(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.DeleteRangeRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.DeleteRangeResponse)

	var timestamp hlc.Timestamp
	if !args.Inline {
		timestamp = h.Timestamp
	}
	// NB: Even if args.ReturnKeys is false, we want to know which intents were
	// written if we're evaluating the DeleteRange for a transaction so that we
	// can update the Result's AcquiredLocks field.
	returnKeys := args.ReturnKeys || h.Txn != nil
	deleted, resumeSpan, num, err := storage.MVCCDeleteRange(
		ctx, readWriter, cArgs.Stats, args.Key, args.EndKey, h.MaxSpanRequestKeys, timestamp, h.Txn, returnKeys,
	)
	if err == nil && args.ReturnKeys {
		reply.Keys = deleted
	}
	reply.NumKeys = num
	if resumeSpan != nil {
		reply.ResumeSpan = resumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}
	// NB: even if MVCC returns an error, it may still have written an intent
	// into the batch. This allows callers to consume errors like WriteTooOld
	// without re-evaluating the batch. This behavior isn't particularly
	// desirable, but while it remains, we need to assume that an intent could
	// have been written even when an error is returned. This is harmless if the
	// error is not consumed by the caller because the result will be discarded.
	return result.FromAcquiredLocks(h.Txn, deleted...), err
}
