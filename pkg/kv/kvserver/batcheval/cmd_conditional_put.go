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
	RegisterReadWriteCommand(kvpb.ConditionalPut, declareKeysConditionalPut, ConditionalPut)
}

func declareKeysConditionalPut(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	maxOffset time.Duration,
) {
	args := req.(*kvpb.ConditionalPutRequest)
	if args.Inline {
		DefaultDeclareKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	} else {
		DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	}
}

// ConditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func ConditionalPut(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.ConditionalPutRequest)
	h := cArgs.Header

	var ts hlc.Timestamp
	if !args.Inline {
		ts = h.Timestamp
	}

	handleMissing := storage.CPutMissingBehavior(args.AllowIfDoesNotExist)
	lrsHandling := storage.LogicalReplicationBehavior(args.MaintainLogicalReplication)
	var err error
	if args.Blind {
		err = storage.MVCCBlindConditionalPut(ctx, readWriter, cArgs.Stats, args.Key, ts, cArgs.Now,
			args.Value, args.ExpBytes, handleMissing, lrsHandling, h.Txn)
	} else {
		err = storage.MVCCConditionalPut(ctx, readWriter, cArgs.Stats, args.Key, ts, cArgs.Now,
			args.Value, args.ExpBytes, handleMissing, lrsHandling, h.Txn)
	}
	if err != nil {
		return result.Result{}, err
	}
	return result.FromAcquiredLocks(h.Txn, args.Key), nil
}
