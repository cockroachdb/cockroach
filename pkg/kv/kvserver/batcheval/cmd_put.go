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
	RegisterReadWriteCommand(roachpb.Put, declareKeysPut, Put)
}

func declareKeysPut(
	rs ImmutableRangeState,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	args := req.(*roachpb.PutRequest)
	if args.Inline {
		DefaultDeclareKeys(rs, header, req, latchSpans, lockSpans)
	} else {
		DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans)
	}
}

// Put sets the value for a specified key.
func Put(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.PutRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	var ts hlc.Timestamp
	if !args.Inline {
		ts = h.Timestamp
	}
	var err error
	if args.Blind {
		err = storage.MVCCBlindPut(ctx, readWriter, ms, args.Key, ts, args.Value, h.Txn)
	} else {
		err = storage.MVCCPut(ctx, readWriter, ms, args.Key, ts, args.Value, h.Txn)
	}
	// NB: even if MVCC returns an error, it may still have written an intent
	// into the batch. This allows callers to consume errors like WriteTooOld
	// without re-evaluating the batch. This behavior isn't particularly
	// desirable, but while it remains, we need to assume that an intent could
	// have been written even when an error is returned. This is harmless if the
	// error is not consumed by the caller because the result will be discarded.
	return result.FromAcquiredLocks(h.Txn, args.Key), err
}
