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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	RegisterReadWriteCommand(kvpb.Put, declareKeysPut, Put)
}

func declareKeysPut(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	maxOffset time.Duration,
) {
	args := req.(*kvpb.PutRequest)
	if args.Inline {
		DefaultDeclareKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	} else {
		DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	}
}

// Put sets the value for a specified key.
func Put(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.PutRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	var ts hlc.Timestamp
	if !args.Inline {
		ts = h.Timestamp
	}
	var err error
	if args.Blind {
		err = storage.MVCCBlindPut(ctx, readWriter, ms, args.Key, ts, cArgs.Now, args.Value, h.Txn)
	} else {
		err = storage.MVCCPut(ctx, readWriter, ms, args.Key, ts, cArgs.Now, args.Value, h.Txn)
	}
	if err != nil {
		return result.Result{}, err
	}
	return result.FromAcquiredLocks(h.Txn, args.Key), nil
}
