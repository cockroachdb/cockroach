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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	RegisterReadWriteCommand(roachpb.ConditionalPut, declareKeysConditionalPut, ConditionalPut)
}

func declareKeysConditionalPut(
	rs ImmutableRangeState,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	args := req.(*roachpb.ConditionalPutRequest)
	if args.Inline {
		DefaultDeclareKeys(rs, header, req, latchSpans, lockSpans)
	} else {
		DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans)
	}
}

// ConditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func ConditionalPut(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ConditionalPutRequest)
	h := cArgs.Header

	var ts hlc.Timestamp
	if !args.Inline {
		ts = h.Timestamp
	}

	var expVal []byte
	if len(args.ExpBytes) != 0 {
		expVal = args.ExpBytes
	} else {
		// Compatibility with 20.1 requests.
		if args.DeprecatedExpValue != nil {
			expVal = args.DeprecatedExpValue.TagAndDataBytes()
		}
	}

	handleMissing := storage.CPutMissingBehavior(args.AllowIfDoesNotExist)
	var err error
	if args.Blind {
		err = storage.MVCCBlindConditionalPut(ctx, readWriter, cArgs.Stats, args.Key, ts, args.Value, expVal, handleMissing, h.Txn)
	} else {
		err = storage.MVCCConditionalPut(ctx, readWriter, cArgs.Stats, args.Key, ts, args.Value, expVal, handleMissing, h.Txn)
	}
	// NB: even if MVCC returns an error, it may still have written an intent
	// into the batch. This allows callers to consume errors like WriteTooOld
	// without re-evaluating the batch. This behavior isn't particularly
	// desirable, but while it remains, we need to assume that an intent could
	// have been written even when an error is returned. This is harmless if the
	// error is not consumed by the caller because the result will be discarded.
	return result.FromAcquiredLocks(h.Txn, args.Key), err
}
